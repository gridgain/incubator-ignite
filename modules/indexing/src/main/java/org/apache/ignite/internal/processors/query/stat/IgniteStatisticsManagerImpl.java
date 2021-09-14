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

package org.apache.ignite.internal.processors.query.stat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedEnumProperty;
import org.apache.ignite.internal.processors.query.h2.SchemaManager;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

import static org.apache.ignite.internal.processors.query.stat.StatisticsUsageState.NO_UPDATE;
import static org.apache.ignite.internal.processors.query.stat.StatisticsUsageState.OFF;
import static org.apache.ignite.internal.processors.query.stat.StatisticsUsageState.ON;

/**
 * Statistics manager implementation.
 */
public class IgniteStatisticsManagerImpl implements IgniteStatisticsManager {
    /** Size of statistics collection pool. */
    private static final int STATS_POOL_SIZE = 4;

    /** Default statistics usage state. */
    private static final StatisticsUsageState DEFAULT_STATISTICS_USAGE_STATE = ON;

    /** Interval to check statistics obsolescence in seconds. */
    private static final int OBSOLESCENCE_INTERVAL = 60;

    /** Logger. */
    private final IgniteLogger log;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** SchemaManager */
    private final SchemaManager schemaMgr;

    /** Statistics repository. */
    private final IgniteStatisticsRepository statsRepos;

    /** Ignite statistics helper. */
    private final IgniteStatisticsHelper helper;

    /** Statistics collector. */
    private final StatisticsProcessor statProc;

    /** Statistics configuration manager. */
    private final IgniteStatisticsConfigurationManager statCfgMgr;

    /** Management pool. */
    private final IgniteThreadPoolExecutor mgmtPool;

    /** Gathering pool. */
    private final IgniteThreadPoolExecutor gatherPool;

    /** Cluster wide statistics usage state. */
    private final DistributedEnumProperty<StatisticsUsageState> usageState = new DistributedEnumProperty<>(
        "statistics.usage.state", StatisticsUsageState::fromOrdinal, StatisticsUsageState::index, StatisticsUsageState.class);

    /** Last known statistics usage state. */
    private volatile StatisticsUsageState lastUsageState = null;

    /** Started flag to prevent double start on change statistics usage state and activation and vice versa. */
    private boolean started = false;

    /** Exchange listener. */
    private final PartitionsExchangeAware exchAwareLsnr = new PartitionsExchangeAware() {
        @Override public void onDoneAfterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
            stateChanged();
        }
    };

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param schemaMgr Schema manager.
     */
    public IgniteStatisticsManagerImpl(GridKernalContext ctx, SchemaManager schemaMgr) {
        this.ctx = ctx;
        this.schemaMgr = schemaMgr;

        helper = new IgniteStatisticsHelper(ctx.localNodeId(), schemaMgr, ctx::log);

        log = ctx.log(IgniteStatisticsManagerImpl.class);

        IgniteCacheDatabaseSharedManager db = (GridCacheUtils.isPersistenceEnabled(ctx.config())) ?
            ctx.cache().context().database() : null;

        gatherPool = new IgniteThreadPoolExecutor("stat-gather",
            ctx.igniteInstanceName(),
            0,
            STATS_POOL_SIZE,
            IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.UNDEFINED,
            ctx.uncaughtExceptionHandler()
        );

        mgmtPool = new IgniteThreadPoolExecutor("stat-mgmt",
            ctx.igniteInstanceName(),
            0,
            1,
            IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.UNDEFINED,
            ctx.uncaughtExceptionHandler()
        );

        boolean storeData = !(ctx.config().isClientMode() || ctx.isDaemon());

        IgniteStatisticsStore store;
        if (!storeData)
            store = new IgniteStatisticsDummyStoreImpl(ctx::log);
        else if (db == null)
            store = new IgniteStatisticsInMemoryStoreImpl(ctx::log);
        else
            store = new IgniteStatisticsPersistenceStoreImpl(ctx.internalSubscriptionProcessor(), db, ctx::log);

        statsRepos = new IgniteStatisticsRepository(store, ctx.systemView(), helper, ctx::log);

        statProc = new StatisticsProcessor(
            statsRepos,
            gatherPool,
            ctx::log
        );

        statCfgMgr = new IgniteStatisticsConfigurationManager(
            schemaMgr,
            ctx.internalSubscriptionProcessor(),
            ctx.systemView(),
            ctx.state(),
            ctx.cache().context().exchange(),
            statsRepos,
            statProc,
            mgmtPool,
            ctx::log
        );

        ctx.internalSubscriptionProcessor().registerDistributedConfigurationListener(dispatcher -> {
            usageState.addListener((name, oldVal, newVal) -> {
                if (log.isInfoEnabled())
                    log.info(String.format("Statistics usage state was changed from %s to %s", oldVal, newVal));

                lastUsageState = newVal;

                if (oldVal == newVal)
                    return;

                stateChanged();
            });

            dispatcher.registerProperty(usageState);
        });

        stateChanged();

        if (!ctx.clientNode()) {
            // Use mgmt pool to work with statistics repository in busy lock to schedule some tasks.
            ctx.timeout().schedule(() -> {
                mgmtPool.submit(() -> {
                    try {
                        statProc.busyRun(() -> processObsolescence());
                    }
                    catch (Throwable e) {
                        log.warning("Error while processing statistics obsolescence", e);
                    }
                });
            }, OBSOLESCENCE_INTERVAL * 1000, OBSOLESCENCE_INTERVAL * 1000);
        }

        ctx.cache().context().exchange().registerExchangeAwareComponent(exchAwareLsnr);
    }

    private synchronized void stateChanged() {
        StatisticsUsageState statUsageState = usageState();
        if (ClusterState.ACTIVE == ctx.state().clusterState().state()
            && !ctx.isStopping()
            && (statUsageState == ON || statUsageState == NO_UPDATE)) {
            if (!started) {
                if (log.isDebugEnabled())
                    log.debug("Starting statistics subsystem...");

                statsRepos.start();
                statProc.start();
                statCfgMgr.start();

                started = true;
            }
        }
        else {
            if (started) {
                if (log.isDebugEnabled())
                    log.debug("Stopping statistics subsystem");

                statCfgMgr.stop();
                statProc.stop();
                statsRepos.stop();

                started = false;
            }
        }
    }

    /**
     * @return Statistics repository.
     */
    public IgniteStatisticsRepository statisticsRepository() {
        return statsRepos;
    }

    /** {@inheritDoc} */
    @Override public ObjectStatistics getLocalStatistics(StatisticsKey key) {
        StatisticsUsageState currState = usageState();

        return (currState == ON || currState == NO_UPDATE) ? statsRepos.getLocalStatistics(key) : null;
    }

    /** {@inheritDoc} */
    @Override public void collectStatistics(StatisticsObjectConfiguration... targets) throws IgniteCheckedException {
        checkStatisticsState("collect statistics");

        if (usageState() == OFF)
            throw new IgniteException("Can't gather statistics while statistics usage state is OFF.");

        statCfgMgr.updateStatistics(targets);
    }

    /** {@inheritDoc} */
    @Override public void dropStatistics(StatisticsTarget... targets) throws IgniteCheckedException {
        checkStatisticsState("drop statistics");

        if (usageState() == OFF)
            throw new IgniteException("Can't drop statistics while statistics usage state is OFF.");

        statCfgMgr.dropStatistics(Arrays.asList(targets), true);
    }

    /** {@inheritDoc} */
    @Override public void refreshStatistics(StatisticsTarget... targets) throws IgniteCheckedException {
        checkStatisticsState("refresh statistics");

        if (usageState() == OFF)
            throw new IgniteException("Can't refresh statistics while statistics usage state is OFF.");

        statCfgMgr.refreshStatistics(Arrays.asList(targets));
    }

    /** {@inheritDoc} */
    @Override public void dropAll() throws IgniteCheckedException {
        checkStatisticsState("drop all statistics");

        statCfgMgr.dropAll();
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        //disableOperations();
        stateChanged();

        if (gatherPool != null) {
            List<Runnable> unfinishedTasks = gatherPool.shutdownNow();
            if (!unfinishedTasks.isEmpty())
                log.warning(String.format("%d statistics collection cancelled.", unfinishedTasks.size()));
        }

        if (mgmtPool != null) {
            List<Runnable> unfinishedTasks = mgmtPool.shutdownNow();
            if (!unfinishedTasks.isEmpty())
                log.warning(String.format("%d statistics configuration change handler cancelled.", unfinishedTasks.size()));
        }
    }

    /** */
    public IgniteStatisticsConfigurationManager statisticConfiguration() {
        return statCfgMgr;
    }

    /** {@inheritDoc} */
    @Override public void usageState(StatisticsUsageState state) throws IgniteCheckedException {
        checkStatisticsState("change usage state of statistics");

        try {
            usageState.propagate(state);
        }
        catch (IgniteCheckedException e) {
            log.error("Unable to set usage state value due to " + e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public StatisticsUsageState usageState() {
        return (lastUsageState == null) ? DEFAULT_STATISTICS_USAGE_STATE : lastUsageState;
    }

    /** {@inheritDoc} */
    @Override public void onRowUpdated(String schemaName, String objName, int partId, byte[] keyBytes) {
        try {
            if (statCfgMgr.config(new StatisticsKey(schemaName, objName)) != null)
                statsRepos.addRowsModified(new StatisticsKey(schemaName, objName), partId, keyBytes);
        }
        catch (IgniteCheckedException e) {
            if (log.isInfoEnabled())
                log.info(String.format("Error while obsolescence key in %s.%s due to %s", schemaName, objName,
                    e.getMessage()));
        }
    }

    /**
     * Save dirty obsolescence info to local metastore. Check if statistics need to be refreshed and schedule it.
     * 1) Get all dirty partition statistics.
     * 2) Make separate tasks for each key to avoid saving obsolescence info for removed partition.
     * 3) Check if partition should be recollected and add it to list in its tables task.
     * 4) Submit tasks. Actually obsolescence info will be
     */
    public synchronized void processObsolescence() {
        StatisticsUsageState state = usageState();

        if (state != ON || ctx.isStopping())
            return;

        if (log.isTraceEnabled())
            log.trace("Processing statistics obsolescence...");

        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> dirty = statsRepos.getDirtyObsolescenceInfo();

        if (F.isEmpty(dirty)) {
            if (log.isTraceEnabled())
                log.trace("No dirty obsolescence info found.");

            return;
        }
        else {
            if (log.isTraceEnabled())
                log.trace(String.format("Scheduling obsolescence savings for %d targets", dirty.size()));
        }

        Map<StatisticsKey, List<Integer>> tasks = calculateObsolescenceRefreshTasks(dirty);

        for (Map.Entry<StatisticsKey, List<Integer>> objTask : tasks.entrySet()) {
            GridH2Table tbl = schemaMgr.dataTable(objTask.getKey().schema(), objTask.getKey().obj());

            if (tbl == null) {
                if (log.isDebugEnabled())
                    log.debug(String.format("Got obsolescence statistics for unknown table %s", objTask.getKey()));
            }

            StatisticsObjectConfiguration objCfg;

            try {
                objCfg = statCfgMgr.config(objTask.getKey());
            }
            catch (IgniteCheckedException e) {
                log.warning("Unable to load statistics object configuration from global metastore", e);

                continue;
            }

            if (objTask.getValue().isEmpty()) {
                // Just save obsolescence info, no additional operations needed.
                statProc.updateKeyAsync(true, tbl, objCfg, Collections.emptySet(), null);
            }
            else {
                // Schedule full gathering and aggregating.
                if (objCfg == null) {
                    if (log.isDebugEnabled()) {
                        log.debug(String.format("Got obsolescence statistics for unknown configuration %s",
                            objTask.getKey()));
                    }

                    continue;
                }

                GridCacheContext<?, ?> cctx = (tbl == null) ? null : tbl.cacheContext();

                AffinityTopologyVersion topVer = null;
                if (!cctx.gate().enterIfNotStopped())
                    continue;
                try {
                    topVer = cctx.affinity().affinityTopologyVersion();
                }
                finally {
                    cctx.gate().leave();
                }

                statProc.updateKeyAsync(true, tbl, objCfg, new HashSet<>(objTask.getValue()), topVer);

            }

            //statCfgMgr.gatherLocalStatistics(objCfg, tbl, parts, new HashSet<>(objTask.getValue()), null);
        }
    }

    /**
     * Calculate targets to refresh obsolescence statistics by map of dirty partitions.
     *
     * @param dirty Map of statistics key to list of it's dirty obsolescence info.
     * @return Map of statistics key to partition to refresh statistics.
     */
    private Map<StatisticsKey, List<Integer>> calculateObsolescenceRefreshTasks(
        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> dirty
    ) {
        Map<StatisticsKey, List<Integer>> res = new HashMap<>();

        for (Map.Entry<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> objObs : dirty.entrySet()) {
            StatisticsKey key = objObs.getKey();
            List<Integer> taskParts = new ArrayList<>();

            StatisticsObjectConfiguration cfg = null;
            try {
                cfg = statCfgMgr.config(key);
            }
            catch (IgniteCheckedException e) {
                log.warning("Unable to get configuration by key " + key + " due to " + e.getMessage()
                    + ". Some statistics can be outdated.");

                continue;
            }

            StatisticsObjectConfiguration finalCfg = cfg;

            objObs.getValue().forEach((k, v) -> {
                ObjectPartitionStatisticsImpl partStat = statsRepos.getLocalPartitionStatistics(key, k);

                if (partStat == null || partStat.rowCount() == 0 ||
                    (double)v.modified() * 100 / partStat.rowCount() > finalCfg.maxPartitionObsolescencePercent())
                    taskParts.add(k);
            });

            res.put(key, taskParts);
        }

        return res;
    }

    /**
     * Check that cluster statistics usage state is not OFF and cluster is active.
     *
     * @param op Operation name.
     */
    public void checkStatisticsState(String op) {
        if (ctx.state().clusterState().state() != ClusterState.ACTIVE)
            throw new IgniteException(String.format(
                "Unable to perform %s due to cluster state [state=%s]", op, ctx.state().clusterState().state()));
    }
}
