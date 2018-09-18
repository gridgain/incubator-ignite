/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

/**
 */
public class ChangeBaselineAndRestartNodesTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }

    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        return super.getConfiguration(name)
            .setConsistentId(name)
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER))
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setCheckpointFrequency(2L * 60 * 1000)
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)));
    }

    /**
     *
     */
    public void test() throws Exception {
        try {
            final IgniteEx ignite0 = (IgniteEx)startGrids(3);

            ignite0.cluster().active(true);

            IgniteEx ignite2 = grid(2);

            for (int i = 0; i < 3; i++) {
                ignite2.close();

                checkTopology(2);

                ignite0.cluster().setBaselineTopology(ignite0.cluster().topologyVersion());

                ignite2 = startGrid(2);

                ignite0.compute(ignite0.cluster().forNode(ignite2.localNode())).run(new IgniteRunnable() {
                    @IgniteInstanceResource
                    Ignite ignite;

                    @LoggerResource
                    IgniteLogger log;

                    @Override public void run() {
                        assertTrue(ignite.cluster().currentBaselineTopology().size() == 2);
                    }
                });

                checkTopology(3);

                ignite0.cluster().setBaselineTopology(ignite0.cluster().topologyVersion());
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
