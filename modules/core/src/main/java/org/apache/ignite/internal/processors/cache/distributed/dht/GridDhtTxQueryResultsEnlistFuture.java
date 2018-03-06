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
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockCancelledException;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxQueryResultsEnlistResponse;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class GridDhtTxQueryResultsEnlistFuture
    extends GridDhtTxQueryAbstractFuture<GridNearTxQueryResultsEnlistResponse> {

    /** */
    Collection<IgniteBiTuple> rows;

    /** */
    GridCacheOperation op;


    /**
     *
     * @param nearNodeId
     * @param nearLockVer
     * @param topVer
     * @param mvccSnapshot
     * @param threadId
     * @param nearFutId
     * @param nearMiniId
     * @param tx
     * @param timeout
     * @param cctx
     */
    public GridDhtTxQueryResultsEnlistFuture(UUID nearNodeId,
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
        super(nearNodeId, nearLockVer, topVer, mvccSnapshot, threadId, nearFutId, nearMiniId, tx, timeout, cctx);

        this.rows = rows;
        this.op = op;
    }

    /** */
    public void init() {
        cancel = new GridQueryCancel();

        cctx.mvcc().addFuture(this);

        if (timeout > 0) {
            //timeoutObj = new GridDhtTxQueryEnlistFuture.LockTimeoutObject();

            //cctx.time().addTimeoutObject(timeoutObj);
        }

        GridDhtCacheAdapter<?, ?> cache = cctx.isNear() ? cctx.near().dht() : cctx.dht();

        try {
            long cnt = 0;

            for (IgniteBiTuple row : rows) {
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
     * @param entry
     * @param row
     * @return
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

        GridCacheMvccCandidate c = entry.addDhtLocal(
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
    @NotNull @Override public GridNearTxQueryResultsEnlistResponse createResponse(@NotNull Throwable err) {
        return new GridNearTxQueryResultsEnlistResponse(cctx.cacheId(), nearFutId, nearMiniId, nearLockVer, 0, err);
    }

    /** */
    @NotNull @Override public GridNearTxQueryResultsEnlistResponse createResponse(long res) {
        return new GridNearTxQueryResultsEnlistResponse(cctx.cacheId(), nearFutId, nearMiniId, nearLockVer, res, null);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridDhtTxQueryResultsEnlistFuture future = (GridDhtTxQueryResultsEnlistFuture)o;

        return Objects.equals(futId, future.futId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return futId.hashCode();
    }
}
