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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxEnlistFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshotResponseListener;
import org.apache.ignite.internal.processors.cache.mvcc.MvccTxInfo;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Cache lock future.
 */
public class GridNearTxEnlistFuture extends GridCacheCompoundIdentityFuture<Long>
    implements GridCacheVersionedFuture<Long>, MvccSnapshotResponseListener {

    /** Cache context. */
    @GridToStringExclude
    private final GridCacheContext<?, ?> cctx;

    /** Transaction. */
    private final GridNearTxLocal tx;

    /** Initiated thread id. */
    private final long threadId;

    /** Mvcc future id. */
    private final IgniteUuid futId;

    /** Lock version. */
    private final GridCacheVersion lockVer;

    /** */
    private MvccSnapshot mvccSnapshot;

    /** */
    private long timeout;

    /** Update mode. */
    private final int mode;

    /** */
    private GridCacheOperation op;

    /** */
    private final GridIterator<IgniteBiTuple> it;

    /** */
    private int batchSize;

    /** */
    private final Map<UUID, Batch> batches = new ConcurrentHashMap<>();

    /** */
    private AffinityTopologyVersion topVer;

    /** Logger. */
    @GridToStringExclude
    private final IgniteLogger log;

    /** Timeout object. */
    @GridToStringExclude
    private LockTimeoutObject timeoutObj;

    /** Row extracted from iterator but not yet used. */
    private IgniteBiTuple peek;

    /** Topology locked flag. */
    private boolean topLocked;

    /**
     * @param cctx Cache context.
     * @param tx Transaction.
     * @param mvccSnapshot MVCC Snapshot.
     * @param timeout Timeout.
     * @param mode Update mode.
     * @param op Cache operation.
     * @param it Rows iterator.
     * @param batchSize Batch size.
     */
    public GridNearTxEnlistFuture(GridCacheContext<?, ?> cctx,
        GridNearTxLocal tx,
        MvccSnapshot mvccSnapshot,
        long timeout,
        int mode,
        GridCacheOperation op,
        GridIterator<IgniteBiTuple> it,
        int batchSize) {
        super(CU.longReducer());

        this.cctx = cctx;
        this.tx = tx;
        this.mvccSnapshot = mvccSnapshot;
        this.timeout = timeout;
        this.mode = mode;
        this.op = op;
        this.it = it;
        this.batchSize = batchSize;

        threadId = tx.threadId();
        futId = IgniteUuid.randomUuid();
        lockVer = tx.xidVersion();

        log = cctx.logger(GridNearTxEnlistFuture.class);
    }

    /** */
    public void init() {
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

                return;
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

                        return;
                    }

                    break;
                }
            }

            if (this.topVer == null)
                this.topVer = topVer;

            topLocked = true;

            map();

            return;
        }

        mapOnTopology(false);
    }

    /**
     * @param remap Remap flag.
     */
    private void mapOnTopology(final boolean remap) {
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

                map();
            }
            else {
                fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                    @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                        try {
                            fut.get();

                            mapOnTopology(remap);
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

    /** */
    private void map() {
        sendNextBatches(null);

        markInitialized();
    }

    /**
     *
     * @param nodeId
     */
    private void sendNextBatches(UUID nodeId) {
        Collection<Batch> next = mapRows(nodeId);

        if (next == null)
            return;

        boolean first = (nodeId != null);

        for (Batch batch : next) {
            ClusterNode node = batch.node();

            sendBatch(node, batch, first);

            if (!node.isLocal())
                first = false;
        }
    }

    /**
     *
     * @param nodeId
     * @return
     */
    private synchronized Collection<Batch> mapRows(UUID nodeId) {
        if (nodeId != null)
            batches.remove(nodeId);

        ArrayList<Batch> res = null;

        IgniteBiTuple cur = (peek != null) ? peek : (it.hasNext() ? it.next() : null);

        while (cur != null) {
            ClusterNode node = cctx.affinity().primaryByKey(cur.getKey(), topVer);

            Batch batch = batches.get(node.id());

            if (batch == null) {
                batch = new Batch(node);

                batches.put(node.id(), batch);
            }

            if (batch.completed()) {
                // Can't advance further at the moment.
                peek = cur;

                break;
            }

            peek = null;

            batch.add(cur);

            cur = it.hasNext() ? it.next() : null;

            if (batch.size() == batchSize) {
                batch.completed(true);

                if (res == null)
                    res = new ArrayList<>(batchSize);

                res.add(batch);
            }
        }

        if (it.hasNext() || peek != null)
            return res;

        // No data left - flush incomplete batches.
        for (Batch batch : batches.values()) {
            if (!batch.completed()) {
                if (res == null)
                    res = new ArrayList<>(batchSize);

                batch.completed(true);

                res.add(batch);
            }
        }

        return res;
    }

    /**
     *
     * @param node Node.
     * @param batch Batch.
     * @param first First mapping flag.
     */
    private void sendBatch(ClusterNode node, Batch batch, boolean first) {
        GridDistributedTxMapping mapping = tx.mappings().get(node.id());

        if (mapping == null)
            tx.mappings().put(mapping = new GridDistributedTxMapping(node));

        mapping.markQueryUpdate();

        boolean clientFirst = first && cctx.localNode().isClient() && !topLocked && !tx.hasRemoteLocks();

        MiniFuture miniFut = new MiniFuture(node);

        add(miniFut);

        int miniId;

        synchronized (this) {
            miniId = futuresCountNoLock();
        }

        if (node.isLocal())
            enlistLocal(miniId, node.id(), miniFut, batch.rows(), timeout);
        else
            sendBatch(miniId, node.id(), miniFut, batch.rows(), clientFirst, timeout);
    }

    /**
     *
     * @param miniId
     * @param nodeId
     * @param miniFut
     * @param rows
     * @param timeout
     */
    private void enlistLocal(int miniId,
        UUID nodeId,
        MiniFuture miniFut,
        Collection<IgniteBiTuple> rows,
        long timeout) {
        tx.init();

        GridNearTxEnlistRequest req = new GridNearTxEnlistRequest(cctx.cacheId(),
            threadId,
            futId,
            miniId,
            tx.subjectId(),
            topVer,
            lockVer,
            mvccSnapshot,
            false,
            timeout,
            tx.taskNameHash(),
            (byte)mode,
            rows,
            op);

        GridDhtTxEnlistFuture fut = new GridDhtTxEnlistFuture(nodeId,
            lockVer,
            topVer,
            mvccSnapshot,
            threadId,
            futId,
            miniId,
            tx,
            timeout,
            cctx,
            rows,
            op);

        final MiniFuture localMini = miniFut;

        fut.listen(new CI1<IgniteInternalFuture<GridNearTxEnlistResponse>>() {
            @Override public void apply(IgniteInternalFuture<GridNearTxEnlistResponse> fut) {
                assert fut.error() != null || fut.result() != null : fut;

                try {
                    sendNextBatches(nodeId);

                    localMini.onResult(fut.result(), fut.error());
                }
                finally {
                    cctx.io().onMessageProcessed(req);
                }
            }
        });

        fut.init();
    }

    /**
     *
     * @param miniId
     * @param nodeId
     * @param miniFut
     * @param rows
     * @param clientFirst
     * @param timeout
     */
    private void sendBatch(int miniId,
        UUID nodeId,
        MiniFuture miniFut,
        Collection<IgniteBiTuple> rows,
        boolean clientFirst,
        long timeout) {
        assert miniFut != null;

        GridNearTxEnlistRequest req = new GridNearTxEnlistRequest(cctx.cacheId(),
            threadId,
            futId,
            miniId,
            tx.subjectId(),
            topVer,
            lockVer,
            mvccSnapshot,
            clientFirst,
            timeout,
            tx.taskNameHash(),
            (byte)mode,
            rows,
            op);

        try {
            cctx.io().send(nodeId, req, cctx.ioPolicy());
        }
        catch (IgniteCheckedException ex) {
            miniFut.onResult(null, ex);
        }
    }

    /**
     * @param nodeId Sender node id.
     * @param res Response.
     */
    public void onResult(UUID nodeId, GridNearTxEnlistResponse res) {
        GridNearTxEnlistFuture.MiniFuture mini = miniFuture(res.miniId());

        sendNextBatches(nodeId);

        if (mini != null)
            mini.onResult(res, null);
    }

    /**
     * Finds pending mini future by the given mini ID.
     *
     * @param miniId Mini ID to find.
     * @return Mini future.
     */
    private MiniFuture miniFuture(int miniId) {
        synchronized (this) {
            int idx = Math.abs(miniId) - 1;

            assert idx >= 0 && idx < futuresCountNoLock();

            IgniteInternalFuture<Long> fut = future(idx);

            if (!fut.isDone())
                return (MiniFuture)fut;
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
            MiniFuture f = (MiniFuture)fut;

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
        mvccSnapshot = res;

        if (tx != null)
            tx.mvccInfo(new MvccTxInfo(crdId, res));
    }

    /** {@inheritDoc} */
    @Override public void onError(IgniteCheckedException e) {
        onDone(e);
    }

    /**
     * Batch of rows.
     */
    private class Batch {
        /** Node ID. */
        private final ClusterNode node;

        /** Rows. */
        private ArrayList<IgniteBiTuple> rows = new ArrayList<>();

        /** Completion flag. */
        private boolean completed;

        /**
         *
         * @param node
         */
        private Batch(ClusterNode node) {
            this.node = node;
        }

        /**
         * @return Completion flag.
         */
        public boolean completed() {
            return completed;
        }

        /**
         * Sets completion flag.
         *
         * @param completed Flag value.
         */
        public void completed(boolean completed) {
            this.completed = completed;
        }

        /**
         * @return Node.
         */
        public ClusterNode node() {
            return node;
        }

        /**
         * Adds a row.
         *
         * @param row Row.
         */
        public void add(IgniteBiTuple row) {
            rows.add(row);
        }

        /**
         * @return number of rows.
         */
        public int size() {
            return rows.size();
        }

        /**
         * @return Collection of rows.
         */
        public Collection<IgniteBiTuple> rows() {
            return rows;
        }
    }

    /** */
    private class MiniFuture extends GridFutureAdapter<Long> {
        /** */
        private boolean completed;

        /** Node ID. */
        @GridToStringExclude
        private final ClusterNode node;

        /**
         * @param node Cluster node.
         */
        private MiniFuture(ClusterNode node) {
            this.node = node;
        }

        /**
         * @param res Response.
         * @param err Exception.
         * @return {@code True} if future was completed by this call.
         */
        public boolean onResult(GridNearTxEnlistResponse res, Throwable err) {
            assert res != null || err != null : this;

            synchronized (this) {
                if (completed)
                    return false;

                completed = true;
            }

            if (X.hasCause(err, ClusterTopologyCheckedException.class)
                || (res != null && res.removeMapping())) {
                assert tx.mappings().get(node.id()).empty();

                tx.removeMapping(node.id());
            }
            else if (res.result() > 0) {
                if (node.isLocal())
                    tx.colocatedLocallyMapped(true);
                else
                    tx.hasRemoteLocks(true);
            }

            return err != null ? onDone(err) : onDone(res.result(), res.error());
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
