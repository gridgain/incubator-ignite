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
package org.apache.ignite.spi.discovery.tcp;

import java.util.function.IntFunction;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**

 */
public class TcpDiscoveryMessageDataCompressionTest extends GridCommonAbstractTest {
    /** */
    public static final int CACHES_CNT = 1000;

    /** */
    private boolean applyDiscoveryHook;

    /** */
    private GridTestUtils.DiscoveryHook discoveryHook;

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().
            setPageSize(1024).
            setWalSegmentSize(8 * 1024 * 1024).
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMaxSize(10 * 1024 * 1024).setPersistenceEnabled(true)));

        if (applyDiscoveryHook) {
            final GridTestUtils.DiscoveryHook hook = discoveryHook != null ? discoveryHook : new GridTestUtils.DiscoveryHook();

            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi() {
                @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
                    super.setListener(GridTestUtils.DiscoverySpiListenerWrapper.wrap(lsnr, hook));
                }
            };

            cfg.setDiscoverySpi(discoSpi);

            cfg.setMetricsUpdateFrequency(1000);
        }

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setClientMode(gridName.startsWith("client"));

        if (!cfg.isClientMode()) {
            CacheConfiguration[] cfgs = new CacheConfiguration[CACHES_CNT];

            for (int i = 0; i < CACHES_CNT; i++)
                cfgs[i] = configuration(i);

            cfg.setCacheConfiguration(cfgs);
        }

        return cfg;
    }

    /**
     * @param idx Index.
     */
    private CacheConfiguration<Integer, Integer> configuration(int idx) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>("cache" + idx);

        ccfg.setCacheMode(CacheMode.REPLICATED);

        ccfg.setGroupName("grp");

        return ccfg;
    }

    /**
     *
     */
    public void testLargeCachesNumberInGroupStart() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_START_CACHES_ON_JOIN, "true");

        try {
            IgniteEx g0 = startGrid(0);

            g0.cluster().active(true);

            awaitPartitionMapExchange();

            IgniteEx g1 = startGrid(1);

            awaitPartitionMapExchange();

            IgniteEx client = (IgniteEx)startGrid("client");

            for (int i = 0; i < CACHES_CNT; i++)
                assertNotNull(client.context().cache().jcacheProxy(configuration(i).getName(), false));
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_START_CACHES_ON_JOIN);

            cleanPersistenceDir();
        }
    }
}

