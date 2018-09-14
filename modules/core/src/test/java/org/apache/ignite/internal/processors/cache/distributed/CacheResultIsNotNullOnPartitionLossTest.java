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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheResultIsNotNullOnPartitionLossTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Number of servers to be started. */
    private static final int SERVERS = 10;

    /** Index of node that is goning to be the only client node. */
    private static final int CLIENT_IDX = SERVERS;

    /** True if {@link #getConfiguration(String)} is expected to configure client node on next invocations. */
    private boolean isClient = false;

    /** Client Ignite instance. */
    private IgniteEx client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discovery = new TcpDiscoverySpi();

        discovery.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discovery);

        cfg.setIncludeEventTypes(EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

        if (isClient)
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        startGrids(SERVERS);

        isClient = true;

        client = startGrid(CLIENT_IDX);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheResultIsNotNull() throws Exception {
        IgniteCache<Integer, Integer> cache = client.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>("cache")
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(0)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE)
        );

        int cacheEntriesCnt = 100;

        for (int i = 0; i < cacheEntriesCnt; i++)
            cache.put(i, i);

        AtomicBoolean nullCacheValueFound = new AtomicBoolean(false);

        // Read cache entries while servers are stopped one by one in separate thread.
        Thread readerThread = new Thread() {
            /** {@inheritDoc} */
            @Override public void run() {
                while (!isInterrupted())
                    for (int i = 0; i < cacheEntriesCnt; i++) {
                        try {
                            if (cache.get(i) == null) {
                                nullCacheValueFound.set(true);

                                break;
                            }
                        }
                        catch (Exception ignored) {
                        }
                    }
            }
        };

        readerThread.start();

        for (int i = 0; i < SERVERS - 1; i++) {
            grid(i).close();

            Thread.sleep(50L);
        }

        // Wait for reader thread to finish its execution.
        readerThread.interrupt();
        readerThread.join();

        assertFalse("Null value was returned by cache.get instead of exception.", nullCacheValueFound.get());
    }
}
