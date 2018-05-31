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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryResultsEnlistFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxRemote;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshotWithoutTxs;
import org.apache.ignite.internal.processors.cache.mvcc.MvccTxInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_OP_COUNTER_NA;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * A future tracking requests for remote nodes transaction enlisting and locking
 * of entries produced with complex DML queries requiring reduce step.
 */
public class GridNearTxQueryResultsEnlistFuture extends GridNearTxAbstractEnlistFuture {
    /** */
    private static final long serialVersionUID = 4339957209840477447L;

    /** */
    public static final int DFLT_BATCH_SIZE = 1024;

    /** Res field updater. */
    private static final AtomicLongFieldUpdater<GridNearTxQueryResultsEnlistFuture> RES_UPD =
        AtomicLongFieldUpdater.newUpdater(GridNearTxQueryResultsEnlistFuture.class, "res");

    /** SkipCntr field updater. */
    private static final AtomicIntegerFieldUpdater<GridNearTxQueryResultsEnlistFuture> SKIP_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridNearTxQueryResultsEnlistFuture.class, "skipCntr");

    /** */
    private GridCacheOperation op;

    /** */
    private final UpdateSourceIterator<?> it;

    /** */
    private int batchSize;

    /** */
    private AtomicInteger batchCntr = new AtomicInteger();

    /** */
    @SuppressWarnings("unused")
    @GridToStringExclude
    private volatile int skipCntr;

    /** */
    @SuppressWarnings("unused")
    @GridToStringExclude
    private volatile long res;

    /** */
    private final Map<UUID, Batch> batches = new ConcurrentHashMap<>();

    /** Row extracted from iterator but not yet used. */
    private Object peek;

    /** Topology locked flag. */
    private boolean topLocked;

    /**
     * @param cctx Cache context.
     * @param tx Transaction.
     * @param timeout Timeout.
     * @param op Cache operation.
     * @param it Rows iterator.
     * @param batchSize Batch size.
     */
    GridNearTxQueryResultsEnlistFuture(GridCacheContext<?, ?> cctx,
        GridNearTxLocal tx,
        long timeout,
        GridCacheOperation op,
        UpdateSourceIterator<?> it,
        int batchSize) {
        super(cctx, tx, timeout);

        this.op = op;
        this.it = it;
        this.batchSize = batchSize > 0 ? batchSize : DFLT_BATCH_SIZE;
    }

    /** {@inheritDoc} */
    @Override protected void map(boolean topLocked) {
        this.topLocked = topLocked;

        sendNextBatches(null);
    }

    /**
     * Continue iterating the data rows and form new batches.
     *
     * @param nodeId Node that is ready for a new batch.
     */
    private void sendNextBatches(@Nullable UUID nodeId) {
        Collection<Batch> next;

        try {
            next = mapRows(nodeId);
        }
        catch (IgniteCheckedException e) {
            onDone(e);

            return;
        }

        if (next == null)
            return;

        boolean first = (nodeId != null);

        for (Batch batch : next) {
            if (isDone())
                return;

            ClusterNode node = batch.node();

            sendBatch(node, batch, first);

            if (!node.isLocal())
                first = false;
        }
    }

    /**
     * Iterate data rows and form batches.
     *
     * @param nodeId Id of node acknowledged the last batch.
     * @return Collection of newly completed batches.
     * @throws IgniteCheckedException If failed.
     */
    private Collection<Batch> mapRows(@Nullable UUID nodeId) throws IgniteCheckedException {
        if (nodeId != null)
            batches.remove(nodeId);

        ArrayList<Batch> res = null;

        // Accumulate number of batches released since we got here.
        // Let only one thread do the looping.
        if (SKIP_UPD.getAndIncrement(this) != 0)
            return null;

        boolean flush = false;

        while (true) {
            Object cur = (peek != null) ? peek : (it.hasNextX() ? it.nextX() : null);

            while (cur != null) {
                if (isDone())
                    return null; // Cancelled.

                Object key;

                if (op == GridCacheOperation.DELETE || op == GridCacheOperation.READ)
                    key = cctx.toCacheKeyObject(cur);
                else
                    key = cctx.toCacheKeyObject(((IgniteBiTuple)cur).getKey());

                List<ClusterNode> nodes = cctx.affinity().nodesByKey(key, topVer);

                ClusterNode node;

                if (F.isEmpty(nodes) || ((node = nodes.get(0)) == null))
                    throw new ClusterTopologyCheckedException("Failed to get primary node " +
                        "[topVer=" + topVer + ", key=" + key + ']');

                Batch batch = batches.get(node.id());

                if (batch == null) {
                    batch = new Batch(node);

                    batches.put(node.id(), batch);
                }

                if (batch.ready()) {
                    // Can't advance further at the moment.
                    peek = cur;

                    it.beforeDetach();

                    flush = true;

                    break;
                }

                peek = null;

                batch.add(op == GridCacheOperation.DELETE ? key : cur,
                    cctx.affinityNode() && (cctx.isReplicated() || nodes.indexOf(cctx.localNode()) > 0));

                cur = it.hasNextX() ? it.nextX() : null;

                if (batch.size() == batchSize) {
                    batch.ready(true);

                    if (res == null)
                        res = new ArrayList<>();

                    res.add(batch);
                }
            }

            if (SKIP_UPD.decrementAndGet(this) == 0)
                break;

            skipCntr = 1;
        }

        if (flush)
            return res;

        // No data left - flush incomplete batches.
        for (Batch batch : batches.values()) {
            if (!batch.ready()) {
                if (res == null)
                    res = new ArrayList<>();

                batch.ready(true);

                res.add(batch);
            }
        }

        if (batches.isEmpty())
            onDone(this.res);

        return res;
    }

    /**
     *
     * @param primaryId Primary node id.
     * @param rows Rows.
     * @param dhtVer Dht version assigned at primary node.
     * @param dhtFutId Dht future id assigned at primary node.
     */
    private void processBatchLocalBackupKeys(UUID primaryId, Collection<Object> rows, GridCacheVersion dhtVer,
        IgniteUuid dhtFutId) {
        assert dhtVer != null;
        assert dhtFutId != null;

        final ArrayList<KeyCacheObject> keys = new ArrayList<>(rows.size());
        final ArrayList<CacheObject> vals = (op == GridCacheOperation.DELETE || op == GridCacheOperation.READ) ? null :
            new ArrayList<>(rows.size());

        for (Object row : rows) {
            KeyCacheObject key;

            if (op == GridCacheOperation.DELETE || op == GridCacheOperation.READ)
                key = cctx.toCacheKeyObject(row);
            else
                key = cctx.toCacheKeyObject(((IgniteBiTuple)row).getKey());

            keys.add(key);

            if (vals != null)
                vals.add(cctx.toCacheObject(((IgniteBiTuple)row).getValue()));
        }

        IgniteInternalFuture<Object> keyFut = F.isEmpty(keys) ? null :
            cctx.group().preloader().request(cctx, keys, topVer);

        if (keyFut == null || keyFut.isDone()) {
            if (keyFut != null) {
                try {
                    keyFut.get();
                }
                catch (NodeStoppingException ignored) {
                    return;
                }
                catch (IgniteCheckedException e) {
                    onDone(e);

                    return;
                }
            }

            processBatchLocalBackupKeys0(primaryId, keys, vals, dhtVer, dhtFutId);
        }
        else {
            keyFut.listen(new IgniteInClosure<IgniteInternalFuture<Object>>() {
                @Override public void apply(IgniteInternalFuture<Object> fut) {
                    try {
                        fut.get();
                    }
                    catch (NodeStoppingException ignored) {
                        return;
                    }
                    catch (IgniteCheckedException e) {
                        onDone(e);

                        return;
                    }

                    processBatchLocalBackupKeys0(primaryId, keys, vals, dhtVer, dhtFutId);
                }
            });
        }
    }

    /**
     *
     * @param primaryId Primary node id.
     * @param keys Keys.
     * @param vals Values.
     * @param dhtVer Dht version.
     * @param dhtFutId Dht future id.
     */
    private void processBatchLocalBackupKeys0(UUID primaryId, List<KeyCacheObject> keys, List<CacheObject> vals,
        GridCacheVersion dhtVer, IgniteUuid dhtFutId) {
        try {
            GridDhtTxRemote dhtTx = cctx.tm().tx(dhtVer);

            if (dhtTx == null) {
                dhtTx = new GridDhtTxRemote(cctx.shared(),
                    cctx.localNodeId(),
                    dhtFutId,
                    primaryId,
                    lockVer,
                    topVer,
                    dhtVer,
                    null,
                    cctx.systemTx(),
                    cctx.ioPolicy(),
                    PESSIMISTIC,
                    REPEATABLE_READ,
                    false,
                    tx.remainingTime(),
                    -1,
                    this.tx.subjectId(),
                    this.tx.taskNameHash(),
                    false);

                dhtTx.mvccInfo(new MvccTxInfo(cctx.shared().coordinators().currentCoordinatorId(),
                    new MvccSnapshotWithoutTxs(mvccSnapshot.coordinatorVersion(), mvccSnapshot.counter(),
                        MVCC_OP_COUNTER_NA, mvccSnapshot.cleanupVersion())));

                dhtTx = cctx.tm().onCreated(null, dhtTx);

                if (dhtTx == null || !cctx.tm().onStarted(dhtTx)) {
                    throw new IgniteTxRollbackCheckedException("Failed to update backup " +
                        "(transaction has been completed): " + dhtVer);
                }
            }

            dhtTx.mvccEnlistBatch(cctx, op, keys, vals, mvccSnapshot.withoutActiveTransactions());
        }
        catch (IgniteCheckedException e) {
            onDone(e);

            return;
        }

        sendNextBatches(primaryId);
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

        int batchId = batchCntr.incrementAndGet();

        if (node.isLocal())
            enlistLocal(batchId, node.id(), batch);
        else
            sendBatch(batchId, node.id(), batch, clientFirst);
    }

    /**
     * Send batch request to remote data node.
     *
     * @param batchId Id of a batch mini-future.
     * @param nodeId Node id.
     * @param batchFut Mini-future for the batch.
     * @param clientFirst {@code true} if originating node is client and it is a first request to any data node.
     */
    private void sendBatch(int batchId, UUID nodeId, Batch batchFut, boolean clientFirst) {
        assert batchFut != null;

        try {
            GridNearTxQueryResultsEnlistRequest req = new GridNearTxQueryResultsEnlistRequest(cctx.cacheId(),
                threadId,
                futId,
                batchId,
                tx.subjectId(),
                topVer,
                lockVer,
                mvccSnapshot,
                clientFirst,
                remainingTime(),
                tx.remainingTime(),
                tx.taskNameHash(),
                batchFut.rows(),
                op);
            cctx.io().send(nodeId, req, cctx.ioPolicy());
        }
        catch (IgniteCheckedException ex) {
            onDone(ex);
        }
    }

    /**
     * Enlist batch of entries to the transaction on local node.
     *
     * @param batchId Id of a batch mini-future.
     * @param nodeId Node id.
     * @param batch Batch.
     */
    private void enlistLocal(int batchId, UUID nodeId, Batch batch) {
        Collection<Object> rows = batch.rows();

        try {
            GridDhtTxQueryResultsEnlistFuture fut = new GridDhtTxQueryResultsEnlistFuture(nodeId,
                lockVer,
                topVer,
                mvccSnapshot,
                threadId,
                futId,
                batchId,
                tx,
                remainingTime(),
                cctx,
                rows,
                op);

            fut.listen(new CI1<IgniteInternalFuture<GridNearTxQueryResultsEnlistResponse>>() {
                @Override public void apply(IgniteInternalFuture<GridNearTxQueryResultsEnlistResponse> fut) {
                    assert fut.error() != null || fut.result() != null : fut;

                    try {
                        if (checkResponse(nodeId, true, fut.result(), fut.error()))
                            sendNextBatches(nodeId);
                    }
                    finally {
                        CU.unwindEvicts(cctx);
                    }
                }
            });

            fut.init();
        }
        catch (IgniteTxTimeoutCheckedException e) {
            onDone(e);
        }
    }

    /**
     * @param nodeId Sender node id.
     * @param res Response.
     */
    public void onResult(UUID nodeId, GridNearTxQueryResultsEnlistResponse res) {
        if (checkResponse(nodeId, false, res, res.error())) {

            Batch batch = batches.get(nodeId);

            if (batch != null && !F.isEmpty(batch.localBackupRows()))
                processBatchLocalBackupKeys(nodeId, batch.localBackupRows(), res.dhtVersion(), res.dhtFutureId());
            else
                sendNextBatches(nodeId);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        if (batches.keySet().contains(nodeId)) {
            if (log.isDebugEnabled())
                log.debug("Found unacknowledged batch for left node [nodeId=" + nodeId + ", fut=" +
                    this + ']');

            ClusterTopologyCheckedException topEx = new ClusterTopologyCheckedException("Failed to enlist keys " +
                "(primary node left grid, retry transaction if possible) [node=" + nodeId + ']');

            topEx.retryReadyFuture(cctx.shared().nextAffinityReadyFuture(topVer));

            onDone(topEx);
        }

        if (log.isDebugEnabled())
            log.debug("Future does not have mapping for left node (ignoring) [nodeId=" + nodeId +
                ", fut=" + this + ']');

        return false;
    }

    /**
     * @param nodeId Originating node ID.
     * @param local {@code True} if originating node is local.
     * @param res Response.
     * @param err Exception.
     * @return {@code True} if future was completed by this call.
     */
    public boolean checkResponse(UUID nodeId, boolean local, GridNearTxQueryResultsEnlistResponse res, Throwable err) {
        assert res != null || err != null : this;

        if (err == null && res.error() != null)
            err = res.error();

        if (X.hasCause(err, ClusterTopologyCheckedException.class)
            || (res != null && res.removeMapping())) {
            assert tx.mappings().get(nodeId).empty();

            tx.removeMapping(nodeId);
        }
        else if (err == null && res.result() > 0) {
            if (local)
                tx.colocatedLocallyMapped(true);
            else
                tx.hasRemoteLocks(true);
        }

        if (err != null) {
            onDone(err);

            return false;
        }

        RES_UPD.getAndAdd(this, res.result());

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxQueryResultsEnlistFuture.class, this, super.toString());
    }

    /**
     * A batch of rows
     */
    private class Batch {
        /** Node ID. */
        @GridToStringExclude
        private final ClusterNode node;

        /** Rows. */
        private ArrayList<Object> rows = new ArrayList<>();

        /** Local backup rows. */
        private ArrayList<Object> locBkpRows;

        /** Readiness flag. Set when batch is full or no new rows are expected. */
        private boolean ready;

        /**
         * @param node Cluster node.
         */
        private Batch(ClusterNode node) {
            this.node = node;
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
         * @param localBackup {@code true}, when the row key has local backup.
         */
        public void add(Object row, boolean localBackup) {
            rows.add(row);

            if (localBackup) {
                if (locBkpRows == null)
                    locBkpRows = new ArrayList<>();

                locBkpRows.add(row);
            }
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
        public Collection<Object> rows() {
            return rows;
        }

        /**
         * @return Collection of local backup rows.
         */
        public Collection<Object> localBackupRows() {
            return locBkpRows;
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
    }

}
