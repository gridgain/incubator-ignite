/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.HashSet;
import java.util.Objects;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheInvokeEntry;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheLockTimeoutException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedLockCancelledException;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxQueryEnlistResponse;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 * Cache lock future.
 */
public final class GridDhtTxQueryEnlistFuture extends GridDhtTxQueryEnlistAbstractFuture<GridNearTxQueryEnlistResponse> {

    /** Involved cache ids. */
    private final int[] cacheIds;

    /** Partitions. */
    private final int[] parts;

    /** Schema name. */
    private final String schema;

    /** Query string. */
    private final String qry;

    /** Query parameters. */
    private final Object[] params;

    /** Flags. */
    private final int flags;

    /** Fetch page size. */
    private final int pageSize;

    /**
     * @param nearNodeId Near node ID.
     * @param nearLockVer Near lock version.
     * @param topVer Topology version.
     * @param mvccSnapshot Mvcc snapshot.
     * @param threadId Thread ID.
     * @param nearFutId Near future id.
     * @param nearMiniId Near mini future id.
     * @param tx Transaction.
     * @param cacheIds Involved cache ids.
     * @param parts Partitions.
     * @param schema Schema name.
     * @param qry Query string.
     * @param params Query parameters.
     * @param flags Flags.
     * @param pageSize Fetch page size.
     * @param timeout Lock acquisition timeout.
     * @param cctx Cache context.
     */
    public GridDhtTxQueryEnlistFuture(
        UUID nearNodeId,
        GridCacheVersion nearLockVer,
        AffinityTopologyVersion topVer,
        MvccSnapshot mvccSnapshot,
        long threadId,
        IgniteUuid nearFutId,
        int nearMiniId,
        GridDhtTxLocalAdapter tx,
        int[] cacheIds,
        int[] parts,
        String schema,
        String qry,
        Object[] params,
        int flags,
        int pageSize,
        long timeout,
        GridCacheContext<?, ?> cctx) {
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

        assert timeout >= 0;
        assert nearNodeId != null;
        assert nearLockVer != null;
        assert topVer != null && topVer.topologyVersion() > 0;
        assert threadId == tx.threadId();

        this.cacheIds = cacheIds;
        this.parts = parts;
        this.schema = schema;
        this.qry = qry;
        this.params = params;
        this.flags = flags;
        this.pageSize = pageSize;

        tx.topologyVersion(topVer);
    }

    /**
     *
     */
    public void init() {
        cancel = new GridQueryCancel();

        cctx.mvcc().addFuture(this);

        if (timeout > 0) {
            timeoutObj = new LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }

        GridDhtCacheAdapter<?, ?> cache = cctx.isNear() ? cctx.near().dht() : cctx.dht();

        try {
            checkPartitions(parts);

            long cnt = 0;

            try (GridCloseableIterator<?> it = cctx.kernalContext().query()
                .prepareDistributedUpdate(cctx, cacheIds, parts, schema, qry, params, flags, pageSize, (int)timeout, topVer, mvccSnapshot, cancel)) {
                while (it.hasNext()) {
                    Object row = it.next();

                    KeyCacheObject key = key(row);

                    while (true) {
                        if (isCancelled())
                            return;

                        GridDhtCacheEntry entry = cache.entryExx(key, topVer);

                        try {
                            addEntry(entry, row);

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
            }

            if (cnt == 0) {
                GridNearTxQueryEnlistResponse res = createResponse(0);

                res.removeMapping(tx.empty());

                onDone(res);

                return;
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
     * Adds entry to future.
     *
     * @param entry Entry to add.
     * @param row Source row.
     * @return Lock candidate.
     * @throws GridCacheEntryRemovedException If entry was removed.
     * @throws GridDistributedLockCancelledException If lock is canceled.
     */
    @SuppressWarnings("unchecked")
    @Nullable private GridCacheMvccCandidate addEntry(GridDhtCacheEntry entry, Object row)
        throws GridCacheEntryRemovedException, GridDistributedLockCancelledException, IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Adding entry: " + entry);

        if (entry == null)
            return null;

        // Check if the future is timed out.
        if (isCancelled())
            return null;

        assert !entry.detached();

        IgniteTxEntry txEntry = tx.entry(entry.txKey());

        if (txEntry != null) {
            throw new IgniteSQLException("One row cannot be changed twice in the same transaction. " +
                "Operation is unsupported at the moment.", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        Object[] row0 = row.getClass().isArray() ? (Object[])row : null;
        CacheObject val = row0 != null && (row0.length == 2 || row0.length == 4) ? cctx.toCacheObject(row0[1]) : null;
        EntryProcessor entryProcessor = row0 != null && row0.length == 4 ? (EntryProcessor)row0[2] : null;
        Object[] invokeArgs = entryProcessor != null ? (Object[])row0[3] : null;
        GridCacheOperation op = !row.getClass().isArray() ? DELETE : entryProcessor != null ? TRANSFORM : UPDATE;

        if (op == TRANSFORM) {
            CacheObject oldVal = val;

            if (oldVal == null)
                oldVal = entry.innerGet(
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

            CacheInvokeEntry invokeEntry = new CacheInvokeEntry(entry.key(), oldVal, entry.version(), true, entry);

            entryProcessor.process(invokeEntry, invokeArgs);

            val = cctx.toCacheObject(invokeEntry.value());

            cctx.validateKeyAndValue(entry.key(), val);

            if (oldVal == null && val != null)
                op = CREATE;
            else if (oldVal != null && val == null)
                op = DELETE;
            else if (oldVal != null && val != null && invokeEntry.modified())
                op = UPDATE;
            else
                op = READ;
        }
        else if (op == UPDATE) {
            assert val != null;

            cctx.validateKeyAndValue(entry.key(), val);
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

        GridCacheMvccCandidate c;

        while (true) {
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
            catch (GridCacheEntryRemovedException ignored) {
                entry = cctx.dhtCache().entryExx(entry.key(), topVer);

                txEntry.cached(entry);
            }
        }

        txEntry.markValid();
        txEntry.queryEnlisted(true);

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
     * @param row Query result row.
     * @return Extracted key.
     */
    private KeyCacheObject key(Object row) {
        return cctx.toCacheKeyObject(row.getClass().isArray() ? ((Object[])row)[0] : row);
    }

    /**
     * @param err Error.
     * @return Prepare response.
     */
    @NotNull @Override public GridNearTxQueryEnlistResponse createResponse(@NotNull Throwable err) {
        return new GridNearTxQueryEnlistResponse(cctx.cacheId(), nearFutId, nearMiniId, nearLockVer, 0, err);
    }

    /**
     * @param res {@code True} if at least one entry was enlisted.
     * @return Prepare response.
     */
    @NotNull @Override public GridNearTxQueryEnlistResponse createResponse(long res) {
        return new GridNearTxQueryEnlistResponse(cctx.cacheId(), nearFutId, nearMiniId, nearLockVer, res, null);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridDhtTxQueryEnlistFuture future = (GridDhtTxQueryEnlistFuture)o;

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

        return S.toString(GridDhtTxQueryEnlistFuture.class, this,
            "pendingLocks", pending,
            "super", super.toString());
    }
}
