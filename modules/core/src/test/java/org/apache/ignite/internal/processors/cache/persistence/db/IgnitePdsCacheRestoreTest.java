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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgnitePdsCacheRestoreTest extends GridCommonAbstractTest {

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Non-persistent data region name. */
    private static final String NO_PERSISTENCE_REGION = "no-persistence-region";

    /** */
    private CacheConfiguration[] ccfgs;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        if (ccfgs != null) {
            cfg.setCacheConfiguration(ccfgs);

            ccfgs = null;
        }

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(10 * 1024 * 1024).setPersistenceEnabled(true))
            .setPageSize(4 * 1024)
            .setWalMode(WALMode.LOG_ONLY);

        memCfg.setDataRegionConfigurations(new DataRegionConfiguration()
            .setMaxSize(10 * 1024 * 1024)
            .setName(NO_PERSISTENCE_REGION)
            .setPersistenceEnabled(false));

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestoreAndNewCache1() throws Exception {
        restoreAndNewCache(false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestoreAndNewCache2() throws Exception {
        restoreAndNewCache(true, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestoreAndNewCache3() throws Exception {
        restoreAndNewCache(false, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestoreAndNewCache4() throws Exception {
        restoreAndNewCache(false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestoreAndNewCache5() throws Exception {
        restoreAndNewCache(true, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestoreAndNewCache6() throws Exception {
        restoreAndNewCache(false, true, true);
    }

    /**
     * @param dynamicStart If {@code true} need cache is added while node is stopped.
     * @throws Exception If failed.
     */
    private void restoreAndNewCache(boolean dynamicStart, boolean fullRestart, boolean changeGroup) throws Exception {
        for (int i = 0; i < 3; i++) {
            ccfgs = configurations1();

            startGrid(i);
        }

        ignite(0).cluster().active(true);

        IgniteCache<Object, Object> cache1 = ignite(2).cache("c1");

        List<Integer> keys = primaryKeys(cache1, 10);

        for (Integer key : keys)
            cache1.put(key, key);

        if (fullRestart)
            stopAllGrids();
        else
            stopGrid(2);

        if (dynamicStart) {
            assert !fullRestart;

            // New cache is added when node is stopped.
            ignite(0).getOrCreateCaches(Arrays.asList(configurations2(changeGroup)));
        }
        else {
            // New cache is added on node restart.
            ccfgs = configurations2(changeGroup);
        }

        try {
            startGrid(2);
        }
        catch (IgniteCheckedException e) {
            if (changeGroup && e.getMessage().contains("Cache group name mismatch"))
                return; // expected error

            throw e;
        }

        if (fullRestart) {
            ccfgs = configurations2(changeGroup); // https://issues.apache.org/jira/browse/IGNITE-7383

            startGrid(0);

            ccfgs = configurations2(changeGroup); // https://issues.apache.org/jira/browse/IGNITE-7383

            startGrid(1).cluster().active(true);
        }

        cache1 = ignite(2).cache("c1");

        IgniteCache<Object, Object> cache2 = ignite(2).cache("c2");

        IgniteCache<Object, Object> cache3 = ignite(2).cache("c3");

        for (Integer key : keys) {
            assertEquals(key, cache1.get(key));

            assertNull(cache2.get(key));

            assertNull(cache3.get(key));

            cache2.put(key, key);

            assertEquals(key, cache2.get(key));

            cache3.put(key, key);

            assertEquals(key, cache3.get(key));
        }

        List<Integer> nearKeys = nearKeys(cache1, 10, 0);

        for (Integer key : nearKeys) {
            assertNull(cache1.get(key));
            assertNull(cache2.get(key));
            assertNull(cache3.get(key));

            cache3.put(key, key);
            assertEquals(key, cache3.get(key));

            cache2.put(key, key);
            assertEquals(key, cache2.get(key));

            cache1.put(key, key);
            assertEquals(key, cache1.get(key));
        }

        startGrid(3);

        awaitPartitionMapExchange();

        for (Integer key : nearKeys) {
            assertEquals(key, cache3.get(key));

            assertEquals(key, cache2.get(key));

            assertEquals(key, cache1.get(key));
        }

        if (!dynamicStart && changeGroup)
            fail("Test should fail on cache group change");
    }

    /**
     * @return Configurations set 1.
     */
    private CacheConfiguration[] configurations1() {
        CacheConfiguration[] ccfgs = new CacheConfiguration[1];

        ccfgs[0] = cacheConfiguration("c1", "g1");

        return ccfgs;
    }

    /**
     * @return Configurations set 1.
     */
    private CacheConfiguration[] configurations2(boolean changeGroup) {
        CacheConfiguration[] ccfgs = new CacheConfiguration[3];

        ccfgs[0] = cacheConfiguration("c1", changeGroup ? "g2" : "g1");
        ccfgs[1] = cacheConfiguration("c2", "g1");

        ccfgs[2] = cacheConfiguration("c3", null);
        ccfgs[2].setDataRegionName(NO_PERSISTENCE_REGION);

        return ccfgs;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name, String group) {
        CacheConfiguration ccfg = new CacheConfiguration(name);

        if (group != null)
            ccfg.setGroupName(group);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }
}
