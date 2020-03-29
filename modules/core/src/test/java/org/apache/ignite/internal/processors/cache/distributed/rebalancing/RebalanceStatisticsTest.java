/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.IgniteRebalanceIterator;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CacheGroupRebalanceStatistics;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CacheGroupSupplierRebalanceStatistics;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CacheGroupTotalRebalanceStatistics;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CacheGroupTotalSupplierRebalanceStatistics;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander.RebalanceFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplier;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridTuple4;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T4;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.SystemPropertiesList;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThread;
import org.junit.Test;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.System.setProperty;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_QUIET;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WRITE_REBALANCE_STATISTICS;
import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.RebalanceStatisticsUtils.availablePrintPartitionsDistribution;
import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.RebalanceStatisticsUtils.availablePrintRebalanceStatistics;
import static org.apache.ignite.internal.util.IgniteUtils.currentTimeMillis;
import static org.apache.ignite.testframework.LogListener.matches;

/**
 * For testing of rebalance statistics.
 */
@SystemPropertiesList(value = {
    @WithSystemProperty(key = IGNITE_QUIET, value = "false"),
    @WithSystemProperty(key = IGNITE_WRITE_REBALANCE_STATISTICS, value = "true"),
    @WithSystemProperty(key = IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS, value = "true")
})
public class RebalanceStatisticsTest extends GridCommonAbstractTest {
    /** Logger for listen messages. */
    private final ListeningTestLogger listenLog = new ListeningTestLogger(false, log);

    /** Caches configurations. */
    private CacheConfiguration[] cacheCfgs;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        listenLog.clearListeners();

        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCacheConfiguration(cacheCfgs)
            .setRebalanceThreadPoolSize(5)
            .setGridLogger(listenLog);
    }

    /**
     * Test checks that rebalance statistics are output into log only if
     * {@link IgniteSystemProperties#IGNITE_QUIET} == {@code false} and
     * {@link IgniteSystemProperties#IGNITE_WRITE_REBALANCE_STATISTICS} ==
     * {@code true}, also partition distribution is present in statistics only
     * if {@link IgniteSystemProperties#IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS}
     * == {@code true}.
     *
     * @throws Exception if any error occurs.
     */
    @Test
    @WithSystemProperty(key = IGNITE_QUIET, value = "true")
    @WithSystemProperty(key = IGNITE_WRITE_REBALANCE_STATISTICS, value = "false")
    @WithSystemProperty(key = IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS, value = "false")
    public void testPrintIntoLogRebStatDependSysProps() throws Exception {
        LogListener[] logListeners = {
            matches(new GrpStatPred()).build(),
            matches(new TotalStatPred()).build(),
            matches(compile("Partitions distribution per cache group \\(.* rebalance\\):.*")).build()
        };

        listenLog.registerAllListeners(logListeners);

        int nodeId = 0;
        startGrid(nodeId++);

        assertFalse(availablePrintRebalanceStatistics());
        assertFalse(availablePrintPartitionsDistribution());
        restartNode(nodeId, l -> assertFalse(l.check()), logListeners);

        setProperty(IGNITE_QUIET, FALSE.toString());
        assertFalse(availablePrintRebalanceStatistics());
        assertFalse(availablePrintPartitionsDistribution());
        restartNode(nodeId, l -> assertFalse(l.check()), logListeners);

        setProperty(IGNITE_WRITE_REBALANCE_STATISTICS, TRUE.toString());
        assertTrue(availablePrintRebalanceStatistics());
        assertFalse(availablePrintPartitionsDistribution());
        restartNode(nodeId, l -> assertEquals(l != logListeners[2], l.check()), logListeners);

        setProperty(IGNITE_WRITE_REBALANCE_PARTITION_STATISTICS, TRUE.toString());
        assertTrue(availablePrintRebalanceStatistics());
        assertTrue(availablePrintPartitionsDistribution());
        restartNode(nodeId, l -> assertTrue(l.check()), logListeners);
    }

    /**
     * Test statistics of a full rebalance.
     *
     * @throws Exception if any error occurs.
     */
    @Test
    public void testFullRebalanceStatistics() throws Exception {
        String grpName0 = "grp0";
        String grpName1 = "grp1";

        cacheCfgs = new CacheConfiguration[] {
            cacheConfiguration("ch_0_0", grpName0, 10, 2),
            cacheConfiguration("ch_0_1", grpName0, 10, 2),
            cacheConfiguration("ch_0_2", grpName0, 10, 2),
            cacheConfiguration("ch_1_0", grpName1, 10, 2),
            cacheConfiguration("ch_1_1", grpName1, 10, 2),
        };

        IgniteEx crd = startGrids(3);

        for (CacheConfiguration cacheCfg : cacheCfgs) {
            String cacheName = cacheCfg.getName();
            IgniteCache<Object, Object> cache = crd.cache(cacheName);

            for (int i = 0; i < cacheCfg.getAffinity().partitions(); i++)
                partitionKeys(cache, i, 10, i * 10).forEach(k -> cache.put(k, cacheName + "_val_" + k));
        }

        int restartNodeId = 2;
        Map<String, CacheGroupRebalanceStatistics> expGrpStats = calcGrpStat(restartNodeId);

        Map<String, CacheGroupTotalRebalanceStatistics> expTotalStats = new HashMap<>();
        updateTotalStat(expTotalStats, expGrpStats);

        GrpStatPred grpStatPred = new GrpStatPred();
        TotalStatPred totalStatPred = new TotalStatPred();

        LogListener[] logListeners = {
            matches(grpStatPred).build(),
            matches(totalStatPred).build()
        };

        long beforeRestartNode = currentTimeMillis();

        listenLog.registerAllListeners(logListeners);
        restartNode(restartNodeId, l -> assertTrue(l.check()), logListeners);

        long afterRestartNode = currentTimeMillis();

        //checking that only for nodeId=2 had statistics into log
        Set<String> nodes = grpStatPred.values.stream().map(GridTuple4::get1).map(Ignite::name).collect(toSet());
        totalStatPred.values.stream().map(IgniteBiTuple::get1).map(Ignite::name).forEach(nodes::add);

        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(grid(restartNodeId).name()));

        assertEquals(expGrpStats.size(), grpStatPred.values.size());

        for (T4<IgniteEx, CacheGroupContext, Boolean, CacheGroupRebalanceStatistics> t4 : grpStatPred.values) {
            //check that result was successful
            assertTrue(t4.get3());

            CacheGroupRebalanceStatistics actGrpStat = t4.get4();
            assertEquals(1, actGrpStat.attempt());

            CacheGroupRebalanceStatistics expGrpStat = expGrpStats.get(t4.get2().cacheOrGroupName());
            checkGrpStat(expGrpStat, actGrpStat, beforeRestartNode, afterRestartNode);
        }

        for (T2<IgniteEx, Map<CacheGroupContext, CacheGroupTotalRebalanceStatistics>> t2 : totalStatPred.values) {
            Map<CacheGroupContext, CacheGroupTotalRebalanceStatistics> actTotalStats = t2.get2();

            assertEquals(expTotalStats.size(), actTotalStats.size());
            assertTrue(actTotalStats.size() > 0);

            for (Entry<CacheGroupContext, CacheGroupTotalRebalanceStatistics> actTotalStatE : actTotalStats.entrySet()) {
                checkTotalStat(
                    expTotalStats.get(actTotalStatE.getKey().cacheOrGroupName()),
                    actTotalStatE.getValue(),
                    beforeRestartNode,
                    afterRestartNode
                );
            }
        }
    }

    /**
     * Create cache configuration.
     *
     * @param cacheName Cache name.
     * @param grpName Cache group name.
     * @param parts Count of partitions.
     * @param backups Count backup.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String cacheName, String grpName, int parts, int backups) {
        requireNonNull(cacheName);
        requireNonNull(grpName);

        return new CacheConfiguration<>(cacheName)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, parts))
            .setBackups(backups)
            .setGroupName(grpName);
    }

    /**
     * Restarting a node with log listeners.
     *
     * @param nodeId        Node id.
     * @param checkConsumer Checking listeners.
     * @param logListeners  Log listeners.
     * @throws Exception if any error occurs.
     */
    private void restartNode(
        int nodeId,
        Consumer<LogListener> checkConsumer,
        LogListener... logListeners
    ) throws Exception {
        requireNonNull(checkConsumer);
        requireNonNull(logListeners);

        A.ensure(logListeners.length > 0, "Empty logListeners");

        for (LogListener rebLogListener : logListeners)
            rebLogListener.reset();

        stopGrid(nodeId);
        awaitPartitionMapExchange();

        startGrid(nodeId);
        awaitPartitionMapExchange();

        for (LogListener rebLogListener : logListeners)
            checkConsumer.accept(rebLogListener);
    }

    /**
     * Сalculation of expected statistics of rebalance for cache groups.
     *
     * @param nodeId Node id.
     * @return Rebalance statistics for cache groups.
     * @throws Exception if any error occurs.
     */
    private Map<String, CacheGroupRebalanceStatistics> calcGrpStat(int nodeId) throws Exception {
        Map<String, CacheGroupRebalanceStatistics> grpStats = new HashMap<>();

        for (CacheGroupContext grpCtx : grid(nodeId).context().cache().cacheGroups()) {
            CacheGroupRebalanceStatistics grpStat = new CacheGroupRebalanceStatistics();
            grpStats.put(grpCtx.cacheOrGroupName(), grpStat);

            for (GridDhtLocalPartition locPart : grpCtx.topology().localPartitions())
                locPart.setState(GridDhtPartitionState.MOVING);

            GridDhtPartitionsExchangeFuture exchFut = grpCtx.shared().exchange().lastTopologyFuture();
            GridDhtPartitionExchangeId exchId = exchFut.exchangeId();

            GridDhtPreloaderAssignments assigns = grpCtx.preloader().generateAssignments(exchId, exchFut);
            for (Entry<ClusterNode, GridDhtPartitionDemandMessage> assignEntry : assigns.entrySet()) {
                IgniteEx supplierNode = (IgniteEx)grid(assignEntry.getKey());
                CacheGroupContext supGrpCtx = supplierNode.context().cache().cacheGroup(grpCtx.groupId());
                GridDhtPartitionSupplier supplier = ((GridDhtPreloader)supGrpCtx.preloader()).supplier();

                GridDhtPartitionDemandMessage demandMsg = assignEntry.getValue();

                Set<Integer> remainingParts = new HashSet<>(demandMsg.partitions().fullSet());
                remainingParts.addAll(demandMsg.partitions().historicalSet());

                IgniteRebalanceIterator rebIter = supGrpCtx.offheap().rebalanceIterator(
                    demandMsg.partitions(),
                    demandMsg.topologyVersion()
                );

                while (rebIter.hasNext()) {
                    CacheDataRow row = rebIter.next();

                    int partId = row.partition();
                    int bytes = supplier.extractEntryInfo(row).marshalledSize(supGrpCtx.cacheObjectContext());

                    grpStat.update(supplierNode.localNode(), partId, rebIter.historical(partId), 1, bytes);
                    remainingParts.remove(partId);
                }

                for (Integer remPartId : remainingParts)
                    grpStat.update(supplierNode.localNode(), remPartId, rebIter.historical(remPartId), 0, 0);
            }
        }
        return grpStats;
    }

    /**
     * Update total rebalance statistics.
     *
     * @param totalStats Total rebalance statistics.
     * @param grpStats   Cache group rebalance statistics.
     */
    private void updateTotalStat(
        Map<String, CacheGroupTotalRebalanceStatistics> totalStats,
        Map<String, CacheGroupRebalanceStatistics> grpStats
    ) {
        requireNonNull(totalStats);
        requireNonNull(grpStats);

        for (Entry<String, CacheGroupRebalanceStatistics> grpStatE : grpStats.entrySet()) {
            totalStats.computeIfAbsent(
                grpStatE.getKey(),
                s -> new CacheGroupTotalRebalanceStatistics()
            ).update(grpStatE.getValue());
        }
    }

    /**
     * Checking equality of {@code exp} and {@code act}.
     *
     * @param exp Expected rebalance statistics.
     * @param act Actual rebalance statistics.
     * @param expStart Expected start time.
     * @param expEnd Expected end time.
     */
    private void checkGrpStat(
        CacheGroupRebalanceStatistics exp,
        CacheGroupRebalanceStatistics act,
        long expStart,
        long expEnd
    ) {
        assertNotNull(exp);
        assertNotNull(act);

        checkTime(expStart, expEnd, act.start(), act.end());

        Map<ClusterNode, CacheGroupSupplierRebalanceStatistics> expSupStats = exp.supplierStatistics();
        Map<ClusterNode, CacheGroupSupplierRebalanceStatistics> actSupStats = act.supplierStatistics();

        assertEquals(expSupStats.size(), actSupStats.size());
        assertTrue(actSupStats.size() > 0);

        for (Entry<ClusterNode, CacheGroupSupplierRebalanceStatistics> expSupStatE : expSupStats.entrySet()) {
            CacheGroupSupplierRebalanceStatistics expSupStat = expSupStatE.getValue();
            CacheGroupSupplierRebalanceStatistics actSupStat = actSupStats.get(expSupStatE.getKey());

            assertNotNull(actSupStat);

            checkTime(expStart, expEnd, actSupStat.start(), actSupStat.end());

            assertEquals(expSupStat.partitions(), actSupStat.partitions());

            assertEquals(expSupStat.fullEntries(), actSupStat.fullEntries());
            assertEquals(expSupStat.histEntries(), actSupStat.histEntries());

            assertEquals(expSupStat.fullBytes(), actSupStat.fullBytes());
            assertEquals(expSupStat.histBytes(), actSupStat.histBytes());
        }
    }

    /**
     * Checking equality of {@code exp} and {@code act}.
     *
     * @param exp Expected total rebalance statistics.
     * @param act Actual total rebalance statistics.
     * @param expStart Expected start time.
     * @param expEnd Expected end time.
     */
    private void checkTotalStat(
        CacheGroupTotalRebalanceStatistics exp,
        CacheGroupTotalRebalanceStatistics act,
        long expStart,
        long expEnd
    ) {
        assertNotNull(exp);
        assertNotNull(act);

        checkTime(expStart, expEnd, act.start(), act.end());

        Map<ClusterNode, CacheGroupTotalSupplierRebalanceStatistics> expSupStats = exp.supplierStatistics();
        Map<ClusterNode, CacheGroupTotalSupplierRebalanceStatistics> actSupStats = act.supplierStatistics();

        assertEquals(expSupStats.size(), actSupStats.size());
        assertTrue(actSupStats.size() > 0);

        for (Entry<ClusterNode, CacheGroupTotalSupplierRebalanceStatistics> expSupStatE : expSupStats.entrySet()) {
            CacheGroupTotalSupplierRebalanceStatistics expSupStat = expSupStatE.getValue();
            CacheGroupTotalSupplierRebalanceStatistics actSupStat = actSupStats.get(expSupStatE.getKey());

            assertNotNull(actSupStat);

            checkTime(expStart, expEnd, actSupStat.start(), actSupStat.end());

            assertEquals(expSupStat.fullParts(), actSupStat.fullParts());
            assertEquals(expSupStat.histParts(), actSupStat.histParts());

            assertEquals(expSupStat.fullEntries(), actSupStat.fullEntries());
            assertEquals(expSupStat.histEntries(), actSupStat.histEntries());

            assertEquals(expSupStat.fullBytes(), actSupStat.fullBytes());
            assertEquals(expSupStat.histBytes(), actSupStat.histBytes());
        }
    }

    /**
     * Time check that {@code actStart} and {@code actEnd} are in between
     * {@code expStart} and {@code expEnd}, and the duration is positive.
     *
     * @param expStart Expected start time.
     * @param expEnd Expected end time.
     * @param actStart Actual start time.
     * @param actEnd Actual end time.
     */
    private void checkTime(long expStart, long expEnd, long actStart, long actEnd) {
        assertTrue(actStart >= expStart && actStart <= expEnd);
        assertTrue(actEnd >= expStart && actEnd <= expEnd);
        assertTrue((actEnd - actStart) >= 0);
    }

    /**
     * Predicate for getting rebalance statistics for cache group when listening log.
     */
    class GrpStatPred extends StatPred<T4<IgniteEx, CacheGroupContext, Boolean, CacheGroupRebalanceStatistics>> {
        /**
         * Default constructor.
         */
        public GrpStatPred() {
            super(compile("Information per cache group \\(.* rebalance\\): \\[id=.*, name=(.*?), startTime=.*"));
        }

        /** {@inheritDoc} */
        @Override public T4<IgniteEx, CacheGroupContext, Boolean, CacheGroupRebalanceStatistics> value(
            Matcher m,
            IgniteThread t
        ) {
            IgniteEx node = grid(t.getIgniteInstanceName());
            CacheGroupContext grpCtx = node.context().cache().cacheGroup(CU.cacheId(m.group(1)));
            RebalanceFuture rebFut = (RebalanceFuture)grpCtx.preloader().rebalanceFuture();

            return new T4<>(node, grpCtx, rebFut.result(), new CacheGroupRebalanceStatistics(rebFut.statistics()));
        }
    }

    /**
     * Predicate for getting total rebalance statistics for all cache group when listening log.
     */
    class TotalStatPred extends StatPred<T2<IgniteEx, Map<CacheGroupContext, CacheGroupTotalRebalanceStatistics>>> {
        /**
         * Default constructor.
         */
        public TotalStatPred() {
            super(compile("Total information \\(including successful and not rebalances\\):.*"));
        }

        /** {@inheritDoc} */
        @Override public T2<IgniteEx, Map<CacheGroupContext, CacheGroupTotalRebalanceStatistics>> value(
            Matcher m,
            IgniteThread t
        ) {
            IgniteEx node = grid(t.getIgniteInstanceName());

            Map<CacheGroupContext, CacheGroupTotalRebalanceStatistics> stat = new HashMap<>();
            for (CacheGroupContext grpCtx : node.context().cache().cacheGroups()) {
                GridDhtPartitionDemander demander = ((GridDhtPreloader)grpCtx.preloader()).demander();
                stat.put(grpCtx, new CacheGroupTotalRebalanceStatistics(demander.totalStatistics()));
            }
            return new T2<>(node, stat);
        }
    }

    /**
     * Base predicate for getting rebalance statistics when listening log.
     */
    private abstract class StatPred<T> implements Predicate<String> {
        /** Pattern for finding statistics of rebalance. */
        final Pattern ptrn;

        /** Obtained values. */
        final Collection<T> values = new ConcurrentLinkedQueue<>();

        /**
         * Constructor.
         *
         * @param ptrn Pattern for finding statistics of rebalance.
         */
        public StatPred(Pattern ptrn) {
            requireNonNull(ptrn);

            this.ptrn = ptrn;
        }

        /**
         * Creating a special value for found statistics.
         *
         * @param m Statistics matcher.
         * @param t Thread of found statistics.
         * @return Special value for found statistics.
         */
        public abstract T value(Matcher m, IgniteThread t);

        /** {@inheritDoc} */
        @Override public boolean test(String logStr) {
            Matcher matcher = ptrn.matcher(logStr);
            if (matcher.matches()) {
                values.add(value(matcher, (IgniteThread)Thread.currentThread()));

                return true;
            }
            return false;
        }
    }
}
