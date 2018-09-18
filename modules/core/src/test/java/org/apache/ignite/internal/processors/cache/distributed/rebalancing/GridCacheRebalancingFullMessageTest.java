/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicyFactory;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionFullCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.mxbean.CacheGroupMetricsMXBean;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.MOVING;

/**
 * Test cases for checking cancellation rebalancing process if some events occurs.
 */
public class GridCacheRebalancingFullMessageTest extends GridCommonAbstractTest {

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration dfltCfg = super
            .getConfiguration(igniteInstanceName)
            .setWorkDirectory(U.defaultWorkDirectory() + "/" + igniteInstanceName)
            .setNetworkTimeout(30_000L)
            .setCommunicationSpi(new TestRecordingCommunicationSpi());

        CacheConfiguration[] cCfgs = new CacheConfiguration[20];

        for(int i = 0; i < 20; i++) {

            CacheConfiguration cCfgi = new CacheConfiguration<Integer, Integer>("cache_group_" + i)
                .setCacheMode(CacheMode.PARTITIONED)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(2);


            cCfgs[i] = cCfgi;
        }

        dfltCfg.setCacheConfiguration(cCfgs);

        dfltCfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
            );
//            .setWalSegments(5)
//            .setWalSegmentSize(1000000);

        dfltCfg.setDataStorageConfiguration(dbCfg);

        ((TcpDiscoverySpi)dfltCfg.getDiscoverySpi()).setIpFinder(ipFinder).setJoinTimeout(100000);

        dfltCfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return dfltCfg;
    }

    /**
     * Test rebalance not cancelled when client node join to cluster.
     *
     * @throws Exception Exception.
     */
    public void testClientNodeJoinAtRebalancing() throws Exception {
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "log", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), getTestIgniteInstanceName(0), false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), getTestIgniteInstanceName(1), false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), getTestIgniteInstanceName(2), false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), getTestIgniteInstanceName(3), false));

        final IgniteEx ignite0 = startGrid(0);
        startGrid(1);
        final IgniteEx ignite2 =  startGrid(2);
        startGrid(3);

        log.info(String.format("Coordinator order = %d", ignite0.cluster().node().order()));

//        U.sleep(10000L);

        ignite0.cluster().active(true);


        for(int i = 0; i < 20; i++) {
            IgniteCache<Integer, Integer> cache = ignite0.getOrCreateCache("cache_group_" + i);

            for (int j = 0; j < 512; j++)
                cache.put(j, j);
        }

        stopGrid(2);
        stopGrid(3);

        TestRecordingCommunicationSpi.spi(ignite(0)).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                boolean res = msg instanceof GridDhtPartitionsFullMessage &&
                    ((GridDhtPartitionsFullMessage)msg).exchangeId() != null
                    && ((GridDhtPartitionsFullMessage)msg).topologyVersion().topologyVersion() == 8
                    && ((GridDhtPartitionsFullMessage)msg).topologyVersion().minorTopologyVersion() == 0
                    && clusterNode.order() == 8;

                if(res)
                    log.info("Message should be blocked");

                return res;
            }
        });

        U.sleep(1000L);

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), getTestIgniteInstanceName(2), false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), getTestIgniteInstanceName(3), false));

        for(int i = 0; i < 20; i++) {
            IgniteCache<Integer, Integer> cache = ignite0.getOrCreateCache("cache_group_" + i);

            for (int j = 512; j < 1024; j++)
                cache.put(j, j);
        }

        log.info("Starting node 2");

        IgniteInternalFuture<IgniteEx> fut = GridTestUtils.runAsync(() -> startGrid(2));

        U.sleep(1_000L);

        log.info("Starting node 3");

        IgniteInternalFuture<IgniteEx> fut0 = GridTestUtils.runAsync(() -> startGrid(3));

        ExecutorService unblocker = Executors.newSingleThreadExecutor();

        unblocker.submit(new Runnable() {
            @Override public void run() {
                try {
                    U.sleep(40_000L);

                    TestRecordingCommunicationSpi.spi(ignite(0)).stopBlock();

                    log.info("Stopped block");
                }
                catch (IgniteInterruptedCheckedException e) {
                    e.printStackTrace();
                }

            }
        });



//        U.sleep(10_000L);
//
//        TestRecordingCommunicationSpi.spi(ignite(0)).stopBlock();
//
//        log.info("Stopped block");

//        String igniteClntName = getTestIgniteInstanceName(3);
//        startGrid(getTestIgniteInstanceName(3), optimize(getConfiguration(igniteClntName).setClientMode(true)));
//        stopGrid(3);

        IgniteEx ignite3a = fut0.get();
        fut.get();

        for(ClusterNode node : ignite3a.cluster().forServers().nodes())
            System.out.println("Node order = " + node.order());

        System.out.println(ignite3a.cluster().localNode().order());

        log.info(String.format("loc UUID after restart = %s", ignite3a.context().localNodeId()));


//        ignite0.cluster().setBaselineTopology(ignite0.cluster().topologyVersion());

//        IgniteCache<Integer, Integer> cache2 = ignite2a.getOrCreateCache("cache_group_4_118");


        unblocker.shutdown();

        boolean correct = true;

        for(int i = 0; i < 20; i++) {
            IgniteCache<Integer, Integer> cache = ignite0.getOrCreateCache("cache_group_" + i);

            for(int j = 0; j < 1024; j++){
                if(cache.get(j) == null || cache.get(j) != j) {
                    log.error(String.format("j = %d; val = %s", j, cache.get(j)));

                    correct = false;

                    break;
                }
            }
        }



//        CacheGroupMetricsMXBean mxBean1Grp1 = mxBean(2, "cache_group_4_118");

//        assertTrue(mxBean1Grp1.getLocalNodeMovingPartitionsCount() != 0);
        assertTrue(correct);

//        String igniteClntName = getTestIgniteInstanceName(4);
//        startGrid(getTestIgniteInstanceName(4), optimize(getConfiguration(igniteClntName).setClientMode(true)));
//        stopGrid(4);

        awaitPartitionMapExchange();

//        stopGrid(3);

//        assertEquals(mxBean1Grp1.getLocalNodeMovingPartitionsCount(), 0);
    }

    private boolean checkPartState(IgniteEx igniteEx, GridDhtPartitionsFullMessage msg){
        try {
            for (Map.Entry<Integer, GridDhtPartitionFullMap> entry : msg.partitions().entrySet()) {

                GridDhtPartitionFullMap partMap = entry.getValue();

                for (Map.Entry<UUID, GridDhtPartitionMap> e : partMap.entrySet()) {

                    log.info(String.format("loc UUID = %s", igniteEx.context().localNodeId()));

                    UUID id = e.getKey();

                    if (igniteEx.cluster().node(id).order() != 5) {
                        log.info(String.format("loc UUID = %s", igniteEx.context().localNodeId()));

                        continue;
                    }
                    else
                        log.info(String.format("loc UUID = %s; order = %d", id, igniteEx.cluster().node(id).order()));

//                log.info(String.format("map UUID = %s", id));
//                log.info(String.format("loc UUID = %s", igniteEx.context().localNodeId()));
//
//                if(!e.getKey().equals(igniteEx.context().localNodeId())) {
//                    log.info(String.format("map UUID != loc UUID returning", id));
//
//
//                    continue;
//                }

//                log.info(String.format("map UUID == loc UUID looking", id));

                    for (Map.Entry<Integer, GridDhtPartitionState> e0 : e.getValue().entrySet()) {
                        int p = e0.getKey();

                        if (e0.getValue() == MOVING)
                            return true;

                    }
                }
            }

            log.info("checkPartState returning false");
        }
        catch (Exception e){
            return false;
        }

        return false;
    }

    /**
     * Gets CacheGroupMetricsMXBean for given node and group name.
     *
     * @param nodeIdx Node index.
     * @param cacheOrGrpName Cache group name.
     * @return MBean instance.
     */
    private CacheGroupMetricsMXBean mxBean(int nodeIdx, String cacheOrGrpName) throws MalformedObjectNameException {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(nodeIdx), "Cache groups", cacheOrGrpName);

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, CacheGroupMetricsMXBean.class,
            true);
    }
}
