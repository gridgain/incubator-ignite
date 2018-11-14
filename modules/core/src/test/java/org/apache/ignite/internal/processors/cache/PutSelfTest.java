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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

/**
 *
 */
public class PutSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "partitioned";

    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName(CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        cfg.setCacheConfiguration(ccfg);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception If failed.
     */
    public void test() throws Exception {
        Collection<Thread> threads = new ArrayList<>(2);

        for (int i = 0; i < 2; i++) {
            int i0 = i;

            Thread t = new Thread(() -> {
                IgniteEx node = grid(i0);

                IgniteCache<Integer, TestValue> cache = node.cache(CACHE_NAME);

                for (int j = i0 * 1_000_000; j < (i0 + 1) * 1_000_000; j++) {
                    cache.put(j, new TestValue(j));
                    cache.get(j + 1_000_000);

                    if (((j + 1) % 100) == 0)
                        System.out.println(Thread.currentThread().getName() + ": " + (j + 1));
                }

            });

            t.start();

            threads.add(t);
        }

        for (Thread t : threads)
            t.join();
    }

    /**
     *
     */
    private static class TestValue implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        private TestValue(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof TestValue))
                return false;

            TestValue value = (TestValue)o;

            return val == value.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }
    }
}
