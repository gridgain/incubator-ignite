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
import java.util.Iterator;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryResultsEnlistFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshotResponseListener;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;

/**
 *
 */
public class GridNearTxQueryResultsEnlistFuture extends GridCacheCompoundIdentityFuture<Long>
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
    private final MvccSnapshot mvccSnapshot;

    /** */
    private long timeout;

    /** Update mode. */
    private final int mode;

    /** */
    private GridCacheOperation op;

    /** */
    private final Iterator<IgniteBiTuple> it;

    /** */
    private int batchSize;

    /** */
    private final Map<UUID, Batch> batches = new ConcurrentHashMap<>();

    /** */
    private AffinityTopologyVersion topVer;

    /** Logger. */
    @GridToStringExclude
    private final IgniteLogger log;

    /**
     * Default constructor.
     */
    public GridNearTxQueryResultsEnlistFuture(GridCacheContext<?, ?> cctx, GridNearTxLocal tx,
        MvccSnapshot mvccSnapshot, long timeout, int mode, GridCacheOperation op, Iterator<IgniteBiTuple> it,
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

        log = cctx.logger(GridNearTxQueryResultsEnlistFuture.class);
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
            //timeoutObj = new GridNearTxQueryEnlistFuture.LockTimeoutObject();

            //cctx.time().addTimeoutObject(timeoutObj);
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

                //map(remap, false);
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
        mapRows();

        for (UUID nodeId : batches.keySet())
            sendNextBatch(nodeId);

        markInitialized();
    }

    /** */
    private void mapRows() {
        while (it.hasNext()) {
            IgniteBiTuple tuple = it.next();

            ClusterNode node = cctx.affinity().primaryByKey(tuple.getKey(), topVer);

            Batch batch = batches.get(node.id());

            if (batch == null) {
                batch = new Batch(node);

                batches.put(node.id(), batch);
            }

            batch.add(tuple);
        }
    }

    /** */
    private void sendNextBatch(UUID nodeId) {
        assert nodeId != null;

        Batch batch = batches.get(nodeId);

        if (batch != null) {
            Batch next = batch.next(batchSize);

            if (next == null)
                batches.remove(nodeId);
            else
                batches.put(nodeId, next);

            sendBatch(batch.node(), batch);
        }
    }

    /** */
    public void sendBatch(ClusterNode node, Batch batch) {
        GridDistributedTxMapping mapping = tx.mappings().get(node.id());

        if (mapping == null)
            tx.mappings().put(mapping = new GridDistributedTxMapping(node));

        mapping.markQueryUpdate();

        // TODO
        boolean clientFirst = mapping.clientFirst();

        MiniFuture miniFut = new MiniFuture(node);

        add(miniFut);

        int miniId;

        synchronized (this) {
            miniId = futuresCountNoLock();
        }

        if (node.isLocal())
            enlistLocal(miniId, node.id(), batch.rows(batchSize), clientFirst, timeout);
        else
            sendBatch(miniId, node.id(), batch.rows(batchSize), clientFirst, timeout);
    }

    /** */
    public void enlistLocal(int miniId, UUID nodeId, Collection<IgniteBiTuple> rows, boolean clientFirst, long timeout) {
        tx.init();

        GridNearTxQueryResultsEnlistRequest req = new GridNearTxQueryResultsEnlistRequest(cctx.cacheId(),
            threadId, futId, miniId, tx.subjectId(), topVer, lockVer, mvccSnapshot, clientFirst, timeout,
            tx.taskNameHash(), (byte)mode, rows, op);

        GridDhtTxQueryResultsEnlistFuture fut = new GridDhtTxQueryResultsEnlistFuture(nodeId,
            lockVer, topVer, mvccSnapshot, threadId, futId, miniId, tx, timeout, cctx, rows, op);

        final MiniFuture localMini = miniFuture(miniId);

        fut.listen(new CI1<IgniteInternalFuture<GridNearTxQueryResultsEnlistResponse>>() {
            @Override public void apply(IgniteInternalFuture<GridNearTxQueryResultsEnlistResponse> fut) {
                assert fut.error() != null || fut.result() != null : fut;

                try {
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
     * @param rows
     * @param clientFirst
     * @param timeout
     */
    public void sendBatch(int miniId, UUID nodeId, Collection<IgniteBiTuple> rows, boolean clientFirst, long timeout) {
        GridNearTxQueryResultsEnlistRequest req = new GridNearTxQueryResultsEnlistRequest(cctx.cacheId(),
            threadId, futId, miniId, tx.subjectId(), topVer, lockVer, mvccSnapshot, clientFirst, timeout,
            tx.taskNameHash(), (byte)mode, rows, op);

        try {
            cctx.io().send(nodeId, req, cctx.ioPolicy());
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to send message: " + e.getMessage());
        }
    }

    /**
     * @param nodeId Sender node id.
     * @param res Response.
     */
    public void onResult(UUID nodeId, GridNearTxQueryResultsEnlistResponse res) {
        GridNearTxQueryResultsEnlistFuture.MiniFuture mini = miniFuture(res.miniId());

        sendNextBatch(nodeId);

        if (mini != null)
            mini.onResult(res, null);

        if (log.isInfoEnabled()) {
            int futCount;

            synchronized (this) {
                futCount = futuresCountNoLock();
            }

            log.info("!!! onResult1: " + nodeId + " futCount=" + futCount + " batches=" + batches.size() + " hasPending=" + hasPending());
        }
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

    }

    /** {@inheritDoc} */
    @Override public void onError(IgniteCheckedException e) {

    }

    /** */
    private class Batch {
        /** */
        private ArrayList<IgniteBiTuple> rows = new ArrayList<>();

        /** Node ID. */
        private final ClusterNode node;

        /** */
        private Batch(ClusterNode node) {
            this.node = node;
        }

        /** */
        private Batch(ClusterNode node, ArrayList<IgniteBiTuple> rows) {
            this.node = node;
            this.rows = rows;
        }

        /** */
        public ClusterNode node() {
            return node;
        }

        /** */
        public void add(IgniteBiTuple row) {
            rows.add(row);
        }

        /** */
        public Collection<IgniteBiTuple> rows(int size) {
            if (rows.size() <= size)
                return rows;

            ArrayList<IgniteBiTuple> r = new ArrayList<>(rows.size() - size);

            for (int idx = 0; idx < size; ++idx)
                r.add(rows.get(idx));

            return r;
        }

        /**
         *
         * @param size
         * @return
         */
        public Batch next(int size) {
            if (rows.size() <= size)
                return null;

            ArrayList<IgniteBiTuple> r = new ArrayList<>(rows.size() - size);

            for (int idx = 0; idx < r.size(); ++idx)
                r.add(r.get(idx + size));

            return new Batch(node, r);
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
        public boolean onResult(GridNearTxQueryResultsEnlistResponse res, Throwable err) {
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

}
