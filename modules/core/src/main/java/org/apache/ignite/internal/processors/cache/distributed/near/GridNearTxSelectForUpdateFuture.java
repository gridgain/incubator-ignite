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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.processors.cache.GridCacheCompoundIdentityFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshotResponseListener;
import org.apache.ignite.internal.processors.cache.mvcc.MvccTxInfo;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * A future tracking requests for remote nodes transaction enlisting and locking
 * of entries produced with complex DML queries requiring reduce step.
 */
public class GridNearTxSelectForUpdateFuture extends GridCacheCompoundIdentityFuture<Long>
    implements GridCacheVersionedFuture<Long>, MvccSnapshotResponseListener {
    /** Done field updater. */
    private static final AtomicIntegerFieldUpdater<GridNearTxSelectForUpdateFuture> DONE_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridNearTxSelectForUpdateFuture.class, "done");

    /** */
    @SuppressWarnings("unused")
    @GridToStringExclude
    private volatile int done;

    /** Cache context. */
    @GridToStringExclude
    private final GridCacheContext<?, ?> cctx;

    /** Transaction. */
    private final GridNearTxLocal tx;

    /** Mvcc future id. */
    private final IgniteUuid futId;

    /** Lock version. */
    private final GridCacheVersion lockVer;

    /** */
    private long timeout;

    /** */
    private AffinityTopologyVersion topVer;

    /** Logger. */
    @GridToStringExclude
    private final IgniteLogger log;

    /** Timeout object. */
    @GridToStringExclude
    private LockTimeoutObject timeoutObj;

    /** Topology locked flag. */
    private boolean topLocked;

    /**
     * @param cctx Cache context.
     * @param tx Transaction.
     * @param timeout Timeout.
     */
    public GridNearTxSelectForUpdateFuture(GridCacheContext<?, ?> cctx,
        GridNearTxLocal tx,
        long timeout) {
        super(CU.longReducer());

        this.cctx = cctx;
        this.tx = tx;
        this.timeout = timeout;

        futId = IgniteUuid.randomUuid();
        lockVer = tx.xidVersion();

        log = cctx.logger(GridNearTxSelectForUpdateFuture.class);
    }

    /** */
    public <T> GridFutureAdapter<T> init(IgniteOutClosure<T> mapFut) throws IgniteCheckedException {
        InitFuture<T> initFut = new InitFuture<>(mapFut);

        if (tx.trackTimeout()) {
            if (!tx.removeTimeoutHandler()) {
                tx.finishFuture().listen(new IgniteInClosure<IgniteInternalFuture<IgniteInternalTx>>() {
                    @Override public void apply(IgniteInternalFuture<IgniteInternalTx> fut) {
                        IgniteTxTimeoutCheckedException err = new IgniteTxTimeoutCheckedException("Failed to " +
                            "acquire lock, transaction was rolled back on timeout [timeout=" + tx.timeout() +
                            ", tx=" + tx + ']');

                        onDone(err);
                    }
                });

                return null;
            }
        }

        if (timeout > 0) {
            timeoutObj = new LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }

        boolean added = cctx.mvcc().addFuture(this);

        assert added : this;

        // Obtain the topology version to use.
        long threadId = Thread.currentThread().getId();

        AffinityTopologyVersion topVer = cctx.mvcc().lastExplicitLockTopologyVersion(threadId);

        // If there is another system transaction in progress, use it's topology version to prevent deadlock.
        if (topVer == null && tx.system())
            topVer = cctx.tm().lockedTopologyVersion(threadId, tx);

        if (topVer != null)
            tx.topologyVersion(topVer);

        if (topVer == null)
            topVer = tx.topologyVersionSnapshot();

        if (topVer != null) {
            for (GridDhtTopologyFuture fut : cctx.shared().exchange().exchangeFutures()) {
                if (fut.exchangeDone() && fut.topologyVersion().equals(topVer)) {
                    Throwable err = fut.validateCache(cctx, false, false, null, null);

                    if (err != null) {
                        onDone(err);

                        return null;
                    }

                    break;
                }
            }

            if (this.topVer == null)
                this.topVer = topVer;

            topLocked = true;

            initFut.init();

            return initFut;
        }

        mapOnTopology(initFut);

        return initFut;
    }

    public boolean clientFirst() {
        return cctx.localNode().isClient() && !topLocked && !tx.hasRemoteLocks();
    }

    /**
     * Acquire topology future and wait for its completion.
     * Start forming batches on stable topology.
     * @param initFut
     */
    private void mapOnTopology(InitFuture<?> initFut) throws IgniteCheckedException {
        cctx.topology().readLock();

        try {
            if (cctx.topology().stopping()) {
                onDone(new CacheStoppedException(cctx.name()));

                return;
            }

            GridDhtTopologyFuture fut = cctx.topologyVersionFuture();

            if (fut.isDone()) {
                Throwable err = fut.validateCache(cctx, false, false, null, null);

                if (err != null) {
                    onDone(err);

                    return;
                }

                AffinityTopologyVersion topVer = fut.topologyVersion();

                if (tx != null)
                    tx.topologyVersion(topVer);

                if (this.topVer == null)
                    this.topVer = topVer;

                initFut.init();
            }
            else {
                fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                    @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                        try {
                            fut.get();

                            initFut.init();
                        }
                        catch (IgniteCheckedException e) {
                            onDone(e);
                        }
                        finally {
                            cctx.shared().txContextReset();
                        }
                    }
                });
            }
        }
        finally {
            cctx.topology().readUnlock();
        }
    }

    /**
     * Start iterating the data rows and form batches.
     */
    private void map() {
        for (ClusterNode node : Collections.<ClusterNode>emptyList())
            map(node);

        markInitialized();
    }

    /**
     * @param node Node.
     */
    private void map(ClusterNode node) {
        GridDistributedTxMapping mapping = tx.mappings().get(node.id());

        if (mapping == null)
            tx.mappings().put(mapping = new GridDistributedTxMapping(node));

        mapping.markQueryUpdate();

        add(new MapQueryFuture(node));
    }

    /**
     * @param res Response.
     */
    public void onResult(GridNearTxQueryResultsEnlistResponse res) {
        MapQueryFuture batchFut = batchFuture(res.miniId());

        if (batchFut != null)
            batchFut.onResult(res, null);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Long res, @Nullable Throwable err) {
        if (!DONE_UPD.compareAndSet(this, 0, 1))
            return false;

        cctx.tm().txContext(tx);

        if (err != null)
            tx.setRollbackOnly();

        if (!X.hasCause(err, IgniteTxTimeoutCheckedException.class) && tx.trackTimeout()) {
            // Need restore timeout before onDone is called and next tx operation can proceed.
            boolean add = tx.addTimeoutHandler();

            assert add;
        }

        if (super.onDone(res, err)) {
            // Clean up.
            cctx.mvcc().removeVersionedFuture(this);

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);

            return true;
        }

        return false;
    }

    /**
     * Finds pending batch future by the given ID.
     *
     * @param batchId Batch ID to find.
     * @return Batch future.
     */
    private MapQueryFuture batchFuture(int batchId) {
        synchronized (this) {
            int idx = Math.abs(batchId) - 1;

            assert idx >= 0 && idx < futuresCountNoLock();

            IgniteInternalFuture<Long> fut = future(idx);

            if (!fut.isDone())
                return (MapQueryFuture)fut;
        }

        return null;
    }


    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return lockVer;
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        for (IgniteInternalFuture<?> fut : futures()) {
            MapQueryFuture f = (MapQueryFuture)fut;

            if (f.node.id().equals(nodeId)) {
                if (log.isDebugEnabled())
                    log.debug("Found mini-future for left node [nodeId=" + nodeId + ", mini=" + f + ", fut=" +
                        this + ']');

                ClusterTopologyCheckedException topEx = new ClusterTopologyCheckedException("Failed to enlist keys " +
                    "(primary node left grid, retry transaction if possible) [node=" + nodeId + ']');

                topEx.retryReadyFuture(cctx.shared().nextAffinityReadyFuture(topVer));

                return f.onResult(null, topEx);
            }
        }

        if (log.isDebugEnabled())
            log.debug("Future does not have mapping for left node (ignoring) [nodeId=" + nodeId +
                ", fut=" + this + ']');

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onResponse(UUID crdId, MvccSnapshot res) {
        if (tx != null)
            tx.mvccInfo(new MvccTxInfo(crdId, res));
    }

    /** {@inheritDoc} */
    @Override public void onError(IgniteCheckedException e) {
        onDone(e);
    }

    /** {@inheritDoc} */
    @Override protected void logError(IgniteLogger log, String msg, Throwable e) {
        // no-op
    }

    /** {@inheritDoc} */
    @Override protected void logDebug(IgniteLogger log, String msg) {
        // no-op
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxSelectForUpdateFuture.class, this, super.toString());
    }

    /**
     * A future tracking a single MAP request to be enlisted in transaction and locked on data node.
     */
    private class MapQueryFuture extends GridFutureAdapter<Long> {
        /** */
        private final AtomicBoolean completed = new AtomicBoolean();

        /** Node ID. */
        @GridToStringExclude
        private final ClusterNode node;

        /** Readiness flag. Set when batch is full or no new rows are expected. */
        private boolean ready;

        /**
         * @param node Cluster node.
         */
        private MapQueryFuture(ClusterNode node) {
            this.node = node;
        }

        /**
         * @return Node.
         */
        public ClusterNode node() {
            return node;
        }

        /**
         * @return Readiness flag.
         */
        public boolean ready() {
            return ready;
        }

        /**
         * Sets readiness flag.
         *
         * @param ready Flag value.
         */
        public void ready(boolean ready) {
            this.ready = ready;
        }

        /**
         * @param res Response.
         * @param err Exception.
         * @return {@code True} if future was completed by this call.
         */
        public boolean onResult(GridNearTxQueryResultsEnlistResponse res, Throwable err) {
            assert res != null || err != null : this;

            if (!completed.compareAndSet(false, true))
                return false;

            if (X.hasCause(err, ClusterTopologyCheckedException.class)
                || (res != null && res.removeMapping())) {
                GridDistributedTxMapping m = tx.mappings().get(node.id());

                assert m != null && m.empty();

                tx.removeMapping(node.id());
            }
            else if (res != null && res.result() > 0) {
                if (node.isLocal())
                    tx.colocatedLocallyMapped(true);
                else
                    tx.hasRemoteLocks(true);
            }

            return err != null ? onDone(err) : onDone(res.result(), res.error());
        }
    }

    private final static class InitFuture<T> extends GridFutureAdapter<T> {
        private final IgniteOutClosure<T> clo;

        private InitFuture(IgniteOutClosure<T> clo) {
            this.clo = clo;
        }

        public void init() {
            try {
                onDone(clo.apply());
            }
            catch (Throwable t) {
                onDone(t);
            }
        }
    }

    /**
     * Lock request timeout object.
     */
    private class LockTimeoutObject extends GridTimeoutObjectAdapter {
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
