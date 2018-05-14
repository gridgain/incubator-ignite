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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheFutureAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheUpdateTxResult;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;

/**
 * Abstract future processing transaction enlisting and locking
 * of entries produced with DML and SELECT FOR UPDATE queries.
 */
public abstract class GridDhtTxQueryEnlistAbstractFuture<T> extends GridCacheFutureAdapter<T> {
    /** */
    private static final AtomicInteger batchIdCntr = new AtomicInteger();

    /** Res field updater. */
    private static final AtomicLongFieldUpdater<GridDhtTxQueryEnlistAbstractFuture> RES_UPD =
        AtomicLongFieldUpdater.newUpdater(GridDhtTxQueryEnlistAbstractFuture.class, "res");

    /** SkipCntr field updater. */
    private static final AtomicIntegerFieldUpdater<GridDhtTxQueryEnlistAbstractFuture> SKIP_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridDhtTxQueryEnlistAbstractFuture.class, "skipCntr");

    /** In-flight batches per node limit. */
    private static final int DFLT_IN_FLIGHT_BATCHES_PER_NODE_LIMIT = 5;

    /** */
    private static final int DFLT_BATCH_SIZE = 1024;

    /** Future ID. */
    protected final IgniteUuid futId;

    /** Cache registry. */
    @GridToStringExclude
    protected final GridCacheContext<?, ?> cctx;

    /** Logger. */
    @GridToStringExclude
    protected final IgniteLogger log;

    /** Thread. */
    protected final long threadId;

    /** Future ID. */
    protected final IgniteUuid nearFutId;

    /** Future ID. */
    protected final int nearMiniId;

    /** Partitions. */
    protected final int[] parts;

    /** Transaction. */
    protected final GridDhtTxLocalAdapter tx;

    /** Lock version. */
    protected final GridCacheVersion lockVer;

    /** Topology version. */
    protected final AffinityTopologyVersion topVer;

    /** */
    protected final MvccSnapshot mvccSnapshot;

    /** Near node ID. */
    protected final UUID nearNodeId;

    /** Near lock version. */
    protected final GridCacheVersion nearLockVer;

    /** Row extracted from iterator but not yet used. */
    private volatile Object peek;

    /** Timeout object. */
    @GridToStringExclude
    protected LockTimeoutObject timeoutObj;

    /** Lock timeout. */
    protected final long timeout;

    /** Trackable flag. */
    protected boolean trackable = true;

    /** Processed entries count. */
    protected long cnt;

    /** Query iterator */
    private UpdateSourceIterator<?> it;

    /** */
    @SuppressWarnings("unused")
    @GridToStringExclude
    private volatile int skipCntr;

    /** */
    @SuppressWarnings("unused")
    @GridToStringExclude
    private volatile long res;

    /** */
    private volatile WALPointer walPtr;

    /** */
    private GridCacheOperation op;

    /** Batches for sending to remote nodes. */
    private final ConcurrentMap<UUID, Batch> batches = new ConcurrentHashMap<>();

    /** Batches already sent to remotes, but their acks are not received yet. */
    private final ConcurrentMap<UUID, ConcurrentMap<Integer, Batch>> batchesInFlight = new ConcurrentHashMap<>();

    /** Finish flag.  */
    private final AtomicBoolean finish = new AtomicBoolean();

    /**
     * @param nearNodeId Near node ID.
     * @param nearLockVer Near lock version.
     * @param topVer Topology version.
     * @param mvccSnapshot Mvcc snapshot.
     * @param threadId Thread ID.
     * @param nearFutId Near future id.
     * @param nearMiniId Near mini future id.
     * @param parts Partitions.
     * @param tx Transaction.
     * @param timeout Lock acquisition timeout.
     * @param cctx Cache context.
     */
    protected GridDhtTxQueryEnlistAbstractFuture(UUID nearNodeId,
        GridCacheVersion nearLockVer,
        AffinityTopologyVersion topVer,
        MvccSnapshot mvccSnapshot,
        long threadId,
        IgniteUuid nearFutId,
        int nearMiniId,
        @Nullable int[] parts,
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
        this.parts = parts;

        lockVer = tx.xidVersion();

        futId = IgniteUuid.randomUuid();

        log = cctx.logger(GridDhtTxQueryEnlistAbstractFuture.class);
    }

    /**
     * @return iterator.
     * @throws IgniteCheckedException If failed.
     */
    protected abstract UpdateSourceIterator<?> createIterator() throws IgniteCheckedException;

    /**
     *
     */
    public void init() {
        cctx.mvcc().addFuture(this, futId);

        if (timeout > 0) {
            timeoutObj = new LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }

        try {
            checkPartitions(parts);

            UpdateSourceIterator<?> it = createIterator();

            if (!it.hasNext()) {
                T res = createResponse();

                U.close(it, log);

                onDone(res);

                return;
            }

            tx.addActiveCache(cctx, false);

            this.it = it;
            this.op = it.operation();
        }
        catch (Throwable e) {
            onDone(e);

            if (e instanceof Error)
                throw (Error)e;

            return;
        }

        sendNextBatches(false);
    }

    /**
     * Iterates over iterator, applies changes locally and sends it on backups.
     *
     * @param ignoreCntr {@code True} if need to ignore skip counter.
     */
    private void sendNextBatches(boolean ignoreCntr) {
        if (isDone())
            return;

        if (!ignoreCntr && (SKIP_UPD.getAndIncrement(this) != 0))
            return;

        cctx.shared().database().checkpointReadLock();

        try {
            while (true) {
                if (!it.hasNext() && peek == null) {
                    if (finish.compareAndSet(false, true)) {
                        if (walPtr != null && !cctx.tm().logTxRecords()) {
                            try {
                                cctx.shared().wal().flush(walPtr, true);
                            }
                            catch (IgniteCheckedException e) {
                                onDone(e);
                            }
                        }

                        // No data left - flush incomplete batches.
                        for (Batch batch : batches.values()) {
                            batch.ready(true);

                            sendBatch(batch);
                        }
                    }

                    if (finish.get() && batches.isEmpty() && inFlightIsEmpty())
                        onDone(createResponse());

                    if (SKIP_UPD.decrementAndGet(this) == 0) {
                        it.beforeDetach();

                        break;
                    }
                }

                while (it.hasNext() || peek != null) {
                    Object cur = (peek != null) ? peek : (it.hasNextX() ? it.nextX() : null);

                    peek = null;

                    KeyCacheObject key;
                    CacheObject val = null;

                    if (op == DELETE || op == READ)
                        key = cctx.toCacheKeyObject(cur);
                    else {
                        key = cctx.toCacheKeyObject(((IgniteBiTuple)cur).getKey());
                        val = cctx.toCacheObject(((IgniteBiTuple)cur).getValue());
                    }

                    if (!ensureFreeSlot(key)) {
                        // Can't advance further at the moment.
                        peek = cur;

                        break;
                    }

                    GridDhtCacheEntry entry = cctx.dhtCache().entryExx(key);

                    if (log.isDebugEnabled())
                        log.debug("Adding entry: " + entry);

                    assert !entry.detached();

                    GridCacheUpdateTxResult res;

                    while (true) {
                        try {
                            switch (op) {
                                case DELETE:
                                    res = entry.mvccRemove(
                                        tx,
                                        cctx.localNodeId(),
                                        topVer,
                                        null,
                                        mvccSnapshot);

                                    break;
                                case CREATE:
                                case UPDATE:
                                    res = entry.mvccSet(
                                        tx,
                                        cctx.localNodeId(),
                                        val,
                                        0,
                                        topVer,
                                        null,
                                        mvccSnapshot,
                                        op);

                                    break;
                                case READ:
                                    res = entry.mvccLock(
                                        tx,
                                        mvccSnapshot);

                                    break;
                                default:
                                    throw new IgniteSQLException("Cannot acquire lock for operation [op= " + op + "]" +
                                        "Operation is unsupported at the moment ", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
                            }

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignored) {
                            entry = cctx.dhtCache().entryExx(entry.key(), topVer);
                        }
                    }

                    walPtr = res.loggedPointer();

                    IgniteInternalFuture<GridCacheUpdateTxResult> updateFut = res.updateFuture();

                    if (updateFut != null) {
                        if (updateFut.isDone()) {
                            try {
                                updateFut.get();
                            }
                            catch (Exception e) {
                                onDone(e);

                                return;
                            }
                        }
                        else {
                            CacheObject finalVal = val;
                            GridDhtCacheEntry finalEntry = entry;

                            it.beforeDetach();

                            updateFut.listen(new CI1<IgniteInternalFuture<GridCacheUpdateTxResult>>() {
                                @Override public void apply(IgniteInternalFuture<GridCacheUpdateTxResult> fut) {
                                    try {
                                        GridCacheUpdateTxResult res = fut.get();

                                        assert res.updateFuture() == null;

                                        processEntry(finalEntry, op, finalVal);

                                        sendNextBatches(true);
                                    }
                                    catch (Throwable e) {
                                        onDone(e);
                                    }
                                }
                            });

                            // Can't move further. Exit loop without decrementing the counter.
                            return;
                        }
                    }

                    processEntry(entry, op, val);
                }
            }
        }
        catch (Throwable e) {
            onDone(e);
        }
        finally {
            cctx.shared().database().checkpointReadUnlock();
        }
    }

    /**
     * @return {@code True} if in-flight batches map is empty.
     */
    private boolean inFlightIsEmpty() {
        for (ConcurrentMap<Integer, Batch> e : batchesInFlight.values()) {
            if (!e.isEmpty())
                return false;
        }

        return true;
    }

    /**
     * @param entry Cache entry.
     * @param op Operation.
     * @param val New value.
     */
    protected void processEntry(GridDhtCacheEntry entry, GridCacheOperation op, CacheObject val) {
        IgniteTxEntry txEntry = tx.addEntry(op,
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

        txEntry.queryEnlisted(true);
        txEntry.markValid();

        cnt++;

        addToBatch(entry.key(), val);
    }

    /**
     * Adds row to batch.
     * <b>IMPORTANT:</b> This method should be called from the critical section in {@link this.sendNextBatches()}
     *
     * @param key Key.
     * @param val Value.
     */
    private void addToBatch(KeyCacheObject key, CacheObject val) {
        List<ClusterNode> dhtNodes = backupNodes(key);

        for (int i = 0; i < dhtNodes.size(); i++) {
            ClusterNode node = dhtNodes.get(i);

            assert !node.isLocal();

            Batch batch = batches.get(node.id());

            if (batch == null) {
                batch = new Batch(node);

                batches.put(node.id(), batch);
            }

            assert op != DELETE || val == null;

            batch.add(key, val);

            if (batch.size() == DFLT_BATCH_SIZE) {
                batch.ready(true);

                sendBatch(batch);
            }
        }
    }

    /**
     * Checks if there free space in batches or free slot in in-flight batches is available for the given key.
     *
     * @param key Key.
     * @return {@code True} if there is possible to add this key to batch or send ready batch.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private boolean ensureFreeSlot(KeyCacheObject key) {
        List<ClusterNode> backupNodes = backupNodes(key);

        // Check possibility of adding to batch and sending.
        for (int i = 0; i < backupNodes.size(); i++) {
            ClusterNode node = backupNodes.get(i);

            Batch batch = batches.get(node.id());

            // We can add key if batch is not full.
            if (batch == null || batch.size() < DFLT_BATCH_SIZE)
                continue;

            ConcurrentMap<Integer, Batch> flyingBatches = batchesInFlight.get(node.id());

            assert flyingBatches.size() <= DFLT_IN_FLIGHT_BATCHES_PER_NODE_LIMIT;

            if (flyingBatches != null && flyingBatches.size() == DFLT_IN_FLIGHT_BATCHES_PER_NODE_LIMIT)
                return false;
        }

        return true;
    }

    /**
     * Send batch request to remote data node.
     *
     * <b>IMPORTANT:</b> This method should be called from the critical section in {@link this.sendNextBatches()}
     *
     * @param batch Batch.
     */
    private void sendBatch(Batch batch) {
        assert batch != null && batch.ready();

        ClusterNode node = batch.node();

        assert !node.isLocal();

        int batchId = batchIdCntr.incrementAndGet();

        ConcurrentMap<Integer, Batch> flyingBatches = batchesInFlight.get(node.id());

        if (flyingBatches == null) {
            flyingBatches = new ConcurrentHashMap<>();

            batchesInFlight.put(node.id(), flyingBatches);
        }

        GridDhtTxQueryEnlistRequest req;

        if (tx.remoteTransactionNodes() == null || !tx.remoteTransactionNodes().contains(node)) {
            tx.addLockTransactionNode(node);
            // If this is a first request to this node, send full info.
            req = new GridDhtTxQueryFirstEnlistRequest(cctx.cacheId(),
                futId,
                cctx.localNodeId(),
                topVer,
                lockVer,
                mvccSnapshot,
                timeout,
                tx.taskNameHash(),
                nearNodeId,
                nearLockVer,
                it.operation(),
                batchId,
                batch.keys(),
                batch.values());
        }
        else {
            // Send only keys, values, LockVersion and batchId if this is not a first request to this backup.
            req = new GridDhtTxQueryEnlistRequest(cctx.cacheId(),
                futId,
                lockVer,
                it.operation(),
                batchId,
                mvccSnapshot.operationCounter(),
                batch.keys(),
                batch.values());
        }

        try {
            Batch prev = flyingBatches.put(batchId, batch);

            assert prev == null;

            batches.remove(node.id());

            cctx.io().send(node, req, cctx.ioPolicy());
        }
        catch (IgniteCheckedException ex) {
            onDone(ex);
        }
    }

    /**
     * @param key Key.
     * @return Backup nodes for the given key.
     */
    @NotNull private List<ClusterNode> backupNodes(KeyCacheObject key) {
        List<ClusterNode> dhtNodes = cctx.affinity().nodesByKey(key, tx.topologyVersion());

        assert !dhtNodes.isEmpty() && dhtNodes.get(0).id().equals(cctx.localNodeId()) :
            "localNode = " + cctx.localNodeId() + ", dhtNodes = " + dhtNodes;

        if (dhtNodes.size() <= 1)
            return Collections.emptyList();

        return dhtNodes.subList(1, dhtNodes.size());
    }

    /**
     * Checks whether all the necessary partitions are in {@link GridDhtPartitionState#OWNING} state.
     *
     * @param parts Partitions.
     * @throws ClusterTopologyCheckedException If failed.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private void checkPartitions(@Nullable int[] parts) throws ClusterTopologyCheckedException {
        if (cctx.isLocal() || !cctx.rebalanceEnabled())
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

    /**
     * Callback on backup response.
     *
     * @param nodeId Backup node.
     * @param res Response.
     */
    public void onResult(UUID nodeId, GridDhtTxQueryEnlistResponse res) {
        int batchId = res.batchId();

        ConcurrentMap<Integer, Batch> inFlight = batchesInFlight.get(nodeId);

        Batch rmv = inFlight.remove(batchId);

        if (rmv == null) {
            onDone(new IgniteCheckedException("Unexpected response received from backup node [node=" + nodeId +
                ", res=" + res + ']'));

            return;
        }

        if (res.error() != null) {
            onDone(new IgniteCheckedException("Failed to update backup node=" + nodeId + '.', res.error()));

            return;
        }

        tx.addRemoteTransactionNode(cctx.node(nodeId));

        sendNextBatches(false);
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
    public GridCacheVersion version() {
        return lockVer;
    }

    /**
     * @param nodeId Left node ID
     * @return {@code True} if node was in the list.
     */
    @Override public boolean onNodeLeft(UUID nodeId) {
        boolean backupLeft = false;

        for (ClusterNode node : tx.lockTransactionNodes()) {
            if (node.id().equals(nodeId)) {
                backupLeft = true;

                break;
            }
        }

        return (backupLeft || nearNodeId.equals(nodeId)) && onDone(
            new ClusterTopologyCheckedException(backupLeft ? "Backup" : "Requesting" +
                " node left the grid [nodeId=" + nodeId + ']'));
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable T res, @Nullable Throwable err) {
        assert res != null ^ err != null;

        if (err != null)
            res = createResponse(err);

        assert res != null;

        if (super.onDone(res, null)) {
            if (log.isDebugEnabled())
                log.debug("Completing future: " + this);

            // Clean up.
            cctx.mvcc().removeFuture(futId);

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);

            U.close(it, log);

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
     * @return Prepared response.
     */
    public abstract T createResponse();

    /**
     * A batch of rows
     */
    private static class Batch {
        /** Node ID. */
        @GridToStringExclude
        private final ClusterNode node;

        /** */
        private List<KeyCacheObject> keys;

        /** */
        private List<CacheObject> vals;

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
         * Adds a row to batch.
         *
         * @param key Key.
         * @param val Value.
         */
        public void add(KeyCacheObject key, CacheObject val) {
            if (keys == null)
                keys = new ArrayList<>();

            keys.add(key);

            if (val != null) {
                if (vals == null)
                    vals = new ArrayList<>();

                vals.add(val);
            }
        }

        /**
         * @return number of rows.
         */
        public int size() {
            return keys == null ? 0 : keys.size();
        }

        /**
         * @return Collection of rows.
         */
        public List<KeyCacheObject> keys() {
            return keys;
        }

        /**
         * @return Collection of rows.
         */
        public List<CacheObject> values() {
            return vals;
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
