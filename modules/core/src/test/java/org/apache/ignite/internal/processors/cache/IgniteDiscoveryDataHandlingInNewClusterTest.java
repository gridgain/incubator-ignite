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
package org.apache.ignite.internal.processors.cache;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKey;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKeyImpl;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteDiscoveryDataHandlingInNewClusterTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String NODE_1_CONS_ID = "node01";

    /** */
    private static final String NODE_2_CONS_ID = "node02";

    /** */
    private static final String NODE_3_CONS_ID = "node03";

    /** */
    private static final String STATIC_CACHE_NAME = "staticCache";

    /** */
    private static final String DYNAMIC_CACHE_NAME_1 = "dynamicCache1";

    /** */
    private static final String DYNAMIC_CACHE_NAME_2 = "dynamicCache2";

    /** */
    private static final String DYNAMIC_CACHE_NAME_3 = "dynamicCache3";

    /** Group where static and dynamic caches reside. */
    private static final String GROUP_WITH_STATIC_CACHES = "group1";

    /** Group where only dynamic caches reside. */
    private static final String DYNAMIC_CACHES_GROUP_WITH_FILTER = "group2";

    /** Group where only dynamic caches reside. */
    private static final String DYNAMIC_CACHES_GROUP_WITHOUT_FILTER = "group3";

    /** Node filter to pin dynamic caches to a specific node. */
    private static final IgnitePredicate<ClusterNode> nodeFilter = new IgnitePredicate<ClusterNode>() {
        @Override public boolean apply(ClusterNode node) {
            return node.consistentId().toString().contains(NODE_1_CONS_ID);
        }
    };

    /** Discovery SPI aimed to fail node with it when another server node joins the topology. */
    private TcpDiscoverySpi failingOnNodeJoinSpi = new TcpDiscoverySpi() {
        @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
            if (msg instanceof TcpDiscoveryNodeAddedMessage) {
                super.startMessageProcess(msg);

                throw new RuntimeException("Simulation of failure of node " + NODE_1_CONS_ID);
            }

            super.startMessageProcess(msg);
        }
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

//        if (igniteInstanceName.contains(NODE_1_CONS_ID)) {
//            failingOnNodeJoinSpi.setIpFinder(ipFinder);
//            failingOnNodeJoinSpi.setJoinTimeout(60_000);
//
//            cfg.setDiscoverySpi(failingOnNodeJoinSpi);
//        }

        cfg.setConsistentId(igniteInstanceName);

        if (igniteInstanceName.contains("client"))
            cfg.setClientMode(true);
        else {
            cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setInitialSize(10500000)
                            .setMaxSize(6659883008L)
                            .setPersistenceEnabled(true)
                    )
            );
        }

//        CacheConfiguration staticCacheCfg = new CacheConfiguration(STATIC_CACHE_NAME)
//            .setGroupName(GROUP_WITH_STATIC_CACHES)
//            .setAffinity(new RendezvousAffinityFunction(false, 32))
//            .setNodeFilter(nodeFilter);
//
//        cfg.setCacheConfiguration(staticCacheCfg);

        cfg.setAtomicConfiguration(new AtomicConfiguration()
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction(false, 32)));

        return cfg;
    }

    /**
     * Verifies that new node received discovery data from stopped grid filters it out
     * from GridCacheProcessor and GridDiscoveryManager internal structures.
     *
     * All subsequent servers and clients join topology successfully.
     *
     * See related ticket <a href="https://issues.apache.org/jira/browse/IGNITE-10878">IGNITE-10878</a>.
     */
    public void testNewClusterFiltersDiscoveryDataReceivedFromStoppedCluster() throws Exception {
        Ignite ig0 = startGrid(NODE_1_CONS_ID);

        prepareDynamicCaches(ig0);

        Ignite ig1 = startGrid(NODE_2_CONS_ID);

        verifyCachesAndGroups(ig1);

        Ignite ig2 = startGrid(NODE_3_CONS_ID);

        verifyCachesAndGroups(ig2);

        Ignite client = startGrid("client01");

        verifyCachesAndGroups(client);
    }

    private volatile boolean stop;
    private CountDownLatch end = new CountDownLatch(1);

    public void testA() throws Exception {
        final int nodeCnt = 5;

        startGrids(nodeCnt);

        Ignite client = startGrid("client");

        GridTestUtils.runAsync(() -> {
            int idx = 0;

            try {
                while (!stop) {
                    try {
                        // restart node.
                        grid(idx).close();

                        startGrid(idx);

                        idx = (idx + 1) % nodeCnt;

                    }
                    catch (Exception e) {
                        e.printStackTrace();

                        return;

                    }

                }

            }
            finally {
                System.out.println(">>>>> idx=" + idx);

                end.countDown();

            }

        });

        IgniteAtomicLong atomic = client.atomicLong("TestAtomicLong", 0, true);

        for (int i = 0; i < 50; ++i) {
            long value = atomic.incrementAndGet();

            System.out.println("value=" + value);

            doSleep(500);

        }

        stop = true;

        end.await();

    }

    public void testB() throws Exception {
        final int nodeCnt = 5;

        startGrids(nodeCnt);

        Ignite client = startGrid("client");

        IgniteAtomicLong at = client.atomicLong("TestAtomicLong", 0, true);

        GridTestUtils.runAsync(() -> {
            int idx = 0;

            try {
                while (!stop) {
                    try {
                        // restart node.
                        grid(idx).close();

                        startGrid(idx);

                        idx = (idx + 1) % nodeCnt;

                        if (idx == 0) {
                            System.out.println(">>>>> cluster restarted idx=" + idx);

                        }

                    }
                    catch (Exception e) {
                        e.printStackTrace();

                        return;

                    }
                }

            }
            finally {
                System.out.println(">>>>> idx=" + idx);

                end.countDown();

            }

        });

        for (int j = 0; j < 10; ++j) {
            IgniteAtomicLong atomic = client.atomicLong("TestAtomicLong-" + j, 0, true);

            for (int i = 0; i < 50; ++i) {
                long value = atomic.incrementAndGet();

                System.out.println("value=" + value);

                doSleep(1500);

            }

            doSleep(2_000);

        }

        stop = true;

        end.await();
    }

    public void testC() throws Exception {
        final int nodeCnt = 5;
        final int atomicCnt = 1;
        final int atomicInc = 10;
        int attempts = 1;

        startGrid(0);

        Ignite client = startGrid("client");

        for (int i = 1; i < nodeCnt; ++i)
            startGrid(i);

        grid(0).cluster().active(true);

        for (int j = 0; j < atomicCnt; ++j) {
            System.out.println(">>>>> Creating " + "TestAtomicLong-" + j + "...");
            IgniteAtomicLong atomic = client.atomicLong("TestAtomicLong-" + j, 0, true);
        }

        while (attempts >= 0) {
            System.out.println("Starting a new attempt " + attempts);

            for (int j = 0; j < atomicCnt; ++j) {
                System.out.println("Update " + "TestAtomicLong-" + j);
                IgniteAtomicLong atomic;
                try {
                    atomic = client.atomicLong("TestAtomicLong-" + j, 0, false);
                }
                catch (IgniteClientDisconnectedException e) {
                    e.reconnectFuture().get();

                    atomic = client.atomicLong("TestAtomicLong-" + j, 0, true);
                }

                for (int i = 0; i < atomicInc; ++i) {
                    long value = atomic.incrementAndGet();

                    System.out.println("\tvalue=" + value);
                }
            }

            roundRestart(nodeCnt);

            attempts--;

            doSleep(5_000);

            //awaitPartitionMapExchange(false, false, grid(0).cluster().forServers().nodes());
        }
    }

    public void testD() throws Exception {
        final int nodeCnt = 5;

        startGrids(nodeCnt);

        grid(0).cluster().active(true);

        Ignite client = startGrid("client");

        int attempts = 5;

        while (attempts >= 0) {
            System.out.println("Starting a new attempt " + attempts);

            for (int j = 0; j < 3; ++j) {
                final GridCacheInternalKey key = new GridCacheInternalKeyImpl("TestAtomicLong-" + j, "default-ds-group");

                String cacheName = "ignite-sys-atomic-cache@default-ds-group";

                IgniteAtomicLong atomic = null;

                try {
                    atomic = client.atomicLong("TestAtomicLong-" + j, 0, true);
                }
                catch (IgniteClientDisconnectedException e) {
                    e.reconnectFuture().get();

                    atomic = client.atomicLong("TestAtomicLong-" + j, 0, true);
                }

                int partId = grid(0).cachex(cacheName).affinity().partition(key);

                System.out.println("Update " + "TestAtomicLong-" + j + ", part=" + partId);

                for (int i = 0; i < 3; ++i) {
                    long value = atomic.incrementAndGet();

                    System.out.println("\tvalue=" + value);
                }
            }

            roundRestart(nodeCnt);

            attempts--;

            doSleep(5_000);

            //awaitPartitionMapExchange(false, false, grid(0).cluster().forServers().nodes());
        }

        client.close();
    }

    private void roundRestart(int nodecnt) throws Exception {
        for (int i = 0; i < nodecnt; ++i) {
            final int nodeIdx = i;

            grid(nodeIdx).close();

            GridTestUtils.runAsync(() -> {
                try {
                    startGrid(nodeIdx);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            });

            doSleep(3_000);
        }
    }

    /** */
    private void verifyCachesAndGroups(Ignite ignite) {
        Map<String, DynamicCacheDescriptor> caches = ((IgniteEx)ignite).context().cache().cacheDescriptors();

        assertEquals(2, caches.size());
        caches.keySet().contains(GridCacheUtils.UTILITY_CACHE_NAME);
        caches.keySet().contains(STATIC_CACHE_NAME);

        Map<Integer, CacheGroupDescriptor> groups = ((IgniteEx)ignite).context().cache().cacheGroupDescriptors();

        assertEquals(2, groups.size());

        boolean defaultGroupFound = false;
        boolean staticCachesGroupFound = false;

        for (CacheGroupDescriptor grpDesc : groups.values()) {
            if (grpDesc.cacheOrGroupName().equals(GridCacheUtils.UTILITY_CACHE_NAME))
                defaultGroupFound = true;
            else if (grpDesc.cacheOrGroupName().equals(GROUP_WITH_STATIC_CACHES))
                staticCachesGroupFound = true;
        }

        assertTrue(String.format("Default group found: %b, static group found: %b",
            defaultGroupFound,
            staticCachesGroupFound),
            defaultGroupFound && staticCachesGroupFound);
    }

    /** */
    private void prepareDynamicCaches(Ignite ig) {
        ig.getOrCreateCache(new CacheConfiguration<>(DYNAMIC_CACHE_NAME_1)
            .setGroupName(GROUP_WITH_STATIC_CACHES)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setNodeFilter(nodeFilter)
        );

        ig.getOrCreateCache(new CacheConfiguration<>(DYNAMIC_CACHE_NAME_2)
            .setGroupName(DYNAMIC_CACHES_GROUP_WITH_FILTER)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
            .setNodeFilter((IgnitePredicate<ClusterNode>)node -> node.consistentId().toString().contains(NODE_1_CONS_ID))
        );

        ig.getOrCreateCache(new CacheConfiguration<>(DYNAMIC_CACHE_NAME_3)
            .setGroupName(DYNAMIC_CACHES_GROUP_WITHOUT_FILTER)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
        );
    }

    /** {@inheritDoc} */
    @Override public void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

}
