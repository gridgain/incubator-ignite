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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheFutureAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract future processing transaction enlisting and locking
 * of entries produced with DML queries.
 */
public abstract class GridDhtTxQueryEnlistAbstractFuture<T extends GridCacheIdMessage> extends GridCacheFutureAdapter<T>
    implements GridCacheVersionedFuture<T> {

    /** Future ID. */
    protected IgniteUuid futId;

    /** Cache registry. */
    @GridToStringExclude
    protected GridCacheContext<?, ?> cctx;

    /** Logger. */
    @GridToStringExclude
    protected IgniteLogger log;

    /** Thread. */
    protected long threadId;

    /** Future ID. */
    IgniteUuid nearFutId;

    /** Future ID. */
    int nearMiniId;

    /** Transaction. */
    protected GridDhtTxLocalAdapter tx;

    /** Lock version. */
    protected GridCacheVersion lockVer;

    /** Topology version. */
    protected AffinityTopologyVersion topVer;

    /** */
    protected final MvccSnapshot mvccSnapshot;

    /** Processed entries count. */
    protected long cnt;

    /** Near node ID. */
    protected UUID nearNodeId;

    /** Near lock version. */
    GridCacheVersion nearLockVer;

    /** Timeout object. */
    @GridToStringExclude
    protected LockTimeoutObject timeoutObj;

    /** Lock timeout. */
    protected final long timeout;

    /** Trackable flag. */
    protected boolean trackable = true;

    /** Keys locked so far. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    @GridToStringExclude
    protected List<GridDhtCacheEntry> entries;

    /** Query cancel object. */
    @GridToStringExclude
    protected GridQueryCancel cancel;

    /**
     *
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
     */
    GridDhtTxQueryEnlistAbstractFuture(UUID nearNodeId,
        GridCacheVersion nearLockVer,
        AffinityTopologyVersion topVer,
        MvccSnapshot mvccSnapshot,
        long threadId,
        IgniteUuid nearFutId,
        int nearMiniId,
        GridDhtTxLocalAdapter tx,
        long timeout,
        GridCacheContext<?, ?> cctx) {
        assert tx != null;
        assert timeout >= 0;
        assert nearNodeId != null;
        assert nearLockVer != null;
        assert topVer != null && topVer.topologyVersion() > 0;
        assert threadId == tx.threadId();

        this.threadId = threadId;
        this.cctx = cctx;
        this.nearNodeId = nearNodeId;
        this.nearLockVer = nearLockVer;
        this.nearFutId = nearFutId;
        this.nearMiniId = nearMiniId;
        this.mvccSnapshot = mvccSnapshot;
        this.topVer = topVer;
        this.timeout = timeout;
        this.tx = tx;

        lockVer = tx.xidVersion();

        futId = IgniteUuid.randomUuid();

        entries = new ArrayList<>();

        log = cctx.logger(GridDhtTxQueryEnlistAbstractFuture.class);
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        if (onCancelled())
            cancel.cancel();

        return isCancelled();
    }

    /**
     * Checks whether all the necessary partitions are in {@link GridDhtPartitionState#OWNING} state.
     *
     * @param parts Partitions.
     * @throws ClusterTopologyCheckedException If failed.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    void checkPartitions(@Nullable int[] parts) throws ClusterTopologyCheckedException {
        if(cctx.isLocal() || !cctx.rebalanceEnabled())
            return;

        if (parts == null)
            parts = U.toIntArray(
                cctx.affinity()
                    .primaryPartitions(cctx.localNodeId(), topVer));

        GridDhtPartitionTopology top = cctx.topology();

        try {
            top.readLock();

            for (int i = 0; i < parts.length; i++) {
                GridDhtLocalPartition p = top.localPartition(parts[i]);

                if (p == null || p.state() != GridDhtPartitionState.OWNING)
                    throw new ClusterTopologyCheckedException("Cannot run update query. " +
                        "Node must own all the necessary partitions."); // TODO IGNITE-7185 Send retry instead.
            }
        }
        finally {
            top.readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        trackable = false;
    }

    /**
     * @return Future ID.
     */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return lockVer;
    }

    /**
     * @param nodeId Left node ID
     * @return {@code True} if node was in the list.
     */
    @Override public boolean onNodeLeft(UUID nodeId) {
        return nearNodeId.equals(nodeId) && onDone(
            new ClusterTopologyCheckedException("Requesting node left the grid [nodeId=" + nodeId + ']'));
    }

    /**
     * Callback for whenever entry lock ownership changes.
     *
     * @param entry Entry whose lock ownership changed.
     */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable T res, @Nullable Throwable err) {
        if (err != null)
            res = createResponse(err);

        assert res != null;

        if (super.onDone(res, null)) {
            if (log.isDebugEnabled())
                log.debug("Completing future: " + this);

            // Clean up.
            cctx.mvcc().removeVersionedFuture(this);

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);


            //TODO
            //U.close(it, log);

            return true;
        }

        return false;
    }

    /**
     * @param err Error.
     * @return Prepared response.
     */
    public abstract T createResponse(@NotNull Throwable err);

    /**
     * @param cnt update count.
     * @return Prepared response.
     */
    public abstract T createResponse(long cnt);

    /**
     * Lock request timeout object.
     */
    protected class LockTimeoutObject extends GridTimeoutObjectAdapter {
        /**
         * Default constructor.
         */
        LockTimeoutObject() {
            super(timeout);
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (log.isDebugEnabled())
                log.debug("Timed out waiting for lock response: " + this);

            onDone(new IgniteTxTimeoutCheckedException("Failed to acquire lock within provided timeout for " +
                "transaction [timeout=" + tx.timeout() + ", tx=" + tx + ']'));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LockTimeoutObject.class, this);
        }
    }
}
