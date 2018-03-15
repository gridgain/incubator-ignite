/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheLockTimeoutException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockCancelledException;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxEnlistResponse;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.DUPLICATE_KEY;

/**
 * Cache lock future.
 */
public class GridDhtTxEnlistFuture extends GridDhtTxEnlistAbstractFuture<GridNearTxEnlistResponse> {
    /** */
    private Collection<IgniteBiTuple> rows;

    /** */
    private GridCacheOperation op;

    /**
     * @param nearNodeId Near node ID.
     * @param nearLockVer Near lock version.
     * @param topVer Topology version.
     * @param mvccSnapshot Mvcc snapshot.
     * @param threadId Thread ID.
     * @param nearFutId Near future id.
     * @param nearMiniId Near mini future id.
     * @param tx Transaction.
     * @param timeout Lock acquisition timeout.
     * @param cctx Cache context.
     * @param rows Collection of rows.
     * @param op Cache operation.
     */
    public GridDhtTxEnlistFuture(UUID nearNodeId,
        GridCacheVersion nearLockVer,
        AffinityTopologyVersion topVer,
        MvccSnapshot mvccSnapshot,
        long threadId,
        IgniteUuid nearFutId,
        int nearMiniId,
        GridDhtTxLocalAdapter tx,
        long timeout,
        GridCacheContext<?, ?> cctx,
        Collection<IgniteBiTuple> rows,
        GridCacheOperation op) {
        super(nearNodeId,
            nearLockVer,
            topVer,
            mvccSnapshot,
            threadId,
            nearFutId,
            nearMiniId,
            tx,
            timeout,
            cctx);

        this.rows = rows;
        this.op = op;
    }

    /** */
    public void init() {
        cancel = new GridQueryCancel();

        cctx.mvcc().addFuture(this);

        if (timeout > 0) {
            timeoutObj = new LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }

        GridDhtCacheAdapter<?, ?> cache = cctx.isNear() ? cctx.near().dht() : cctx.dht();

        try {
            long cnt = 0;

            for (IgniteBiTuple row : rows) {
                while (true) {
                    if (isCancelled())
                        return;

                    GridDhtCacheEntry entry = cache.entryExx(cctx.toCacheKeyObject(row.get1()), topVer);

                    try {
                        addEntry(entry, row.get2());

                        cnt++;

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry when adding lock (will retry): " + entry);
                    }
                    catch (GridDistributedLockCancelledException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to add entry [err=" + e + ", entry=" + entry + ']');

                        onDone(e);

                        return;
                    }
                }
            }

            tx.addActiveCache(cctx, false);

            this.cnt = cnt;
        }
        catch (Throwable e) {
            onDone(e);

            if (e instanceof Error)
                throw (Error)e;

            return;
        }

        readyLocks();
    }

    /**
     *
     * @param entry Entry.
     * @param row Row.
     * @return MVCC candidate.
     * @throws GridCacheEntryRemovedException
     * @throws GridDistributedLockCancelledException
     * @throws IgniteCheckedException
     */
    @Nullable private GridCacheMvccCandidate addEntry(GridDhtCacheEntry entry, Object row)
        throws GridCacheEntryRemovedException, GridDistributedLockCancelledException, IgniteCheckedException {
        // Check if the future is timed out.
        if (isCancelled())
            return null;

        assert !entry.detached();

        IgniteTxEntry txEntry = tx.entry(entry.txKey());

        if (txEntry != null) {
            throw new IgniteSQLException("One row cannot be changed twice in the same transaction. " +
                "Operation is unsupported at the moment.", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        CacheObject val = cctx.toCacheObject(row);

        if (op == CREATE) {
            CacheObject oldVal = entry.innerGet(
                null,
                tx,
                false,
                false,
                false,
                tx.subjectId(),
                null,
                tx.resolveTaskName(),
                null,
                true,
                mvccSnapshot);

            if (oldVal != null)
                throw new IgniteSQLException("Duplicate key during INSERT [key=" + entry.key() + ']', DUPLICATE_KEY);
        }

        txEntry = tx.addEntry(op,
            val,
            null,
            null,
            entry,
            null,
            CU.empty0(),
            false,
            -1L,
            -1L,
            null,
            true,
            true,
            false);

        txEntry.cached(entry);
        txEntry.markValid();
        txEntry.queryEnlisted(true);

        GridCacheMvccCandidate c;

        while (true) { // The entry might get evicted.
            try {
                c = entry.addDhtLocal(
                    nearNodeId,
                    nearLockVer,
                    topVer,
                    threadId,
                    lockVer,
                    null,
                    timeout,
                    false,
                    true,
                    false,
                    false
                );

                break;
            }
            catch (GridCacheEntryRemovedException ex) {
                entry = cctx.dhtCache().entryExx(entry.key(), topVer);

                txEntry.cached(entry);
            }
        }

        if (c == null && timeout < 0) {

            if (log.isDebugEnabled())
                log.debug("Failed to acquire lock with negative timeout: " + entry);

            onDone(new GridCacheLockTimeoutException(lockVer));

            return null;
        }

        synchronized (this) {
            entries.add(c == null || c.reentry() ? null : entry);

            if (c != null && !c.reentry())
                pendingLocks.add(entry.key());
        }

        // Double check if the future has already timed out.
        if (isCancelled()) {
            entry.removeLock(lockVer);

            return null;
        }

        return c;
    }

    /**
     * @param err Error.
     * @return Prepare response.
     */
    @NotNull @Override public GridNearTxEnlistResponse createResponse(@NotNull Throwable err) {
        return new GridNearTxEnlistResponse(cctx.cacheId(), nearFutId, nearMiniId, nearLockVer, 0, err);
    }

    /** */
    @NotNull @Override public GridNearTxEnlistResponse createResponse(long res) {
        return new GridNearTxEnlistResponse(cctx.cacheId(), nearFutId, nearMiniId, nearLockVer, res, null);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridDhtTxEnlistFuture future = (GridDhtTxEnlistFuture)o;

        return Objects.equals(futId, future.futId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return futId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        HashSet<KeyCacheObject> pending;

        synchronized (this) {
            pending = new HashSet<>(pendingLocks);
        }

        return S.toString(GridDhtTxEnlistFuture.class, this,
            "pendingLocks", pending,
            "super", super.toString());
    }
}
