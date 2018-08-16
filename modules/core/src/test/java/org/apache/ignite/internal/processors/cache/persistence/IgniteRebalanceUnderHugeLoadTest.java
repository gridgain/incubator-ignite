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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 *
 */
public class IgniteRebalanceUnderHugeLoadTest extends GridCommonAbstractTest {
    /** */
    public static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setConsistentId(name);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                )
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAffinity(
                    new RendezvousAffinityFunction(false, 32))
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
        );

        cfg.setCommunicationSpi(new BlockTcpCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }


    /** */
    private static final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();

    private static volatile boolean rebalanceResumed = false;


    /**
     *
     * @throws Exception If failed.
     */
    public void test() throws Exception {
        Ignite ig0 = startGrid(0);

        ig0.cluster().active(true);

        int entries = 100_000;

        try (IgniteDataStreamer<Integer, Integer> st = ig0.dataStreamer(DEFAULT_CACHE_NAME)) {
            st.allowOverwrite(true);

            for (int i = 0; i < entries; i++)
                st.addData(i, i);
        }

        {
            IgniteCache<Object, Object> cache = ig0.cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < entries; i++) {
                assertEquals(i, cache.get(i));
            }
        }

        IgniteInternalFuture<IgniteEx> igniteExIgniteInternalFuture = runAsync(new Callable<IgniteEx>() {
            @Override
            public IgniteEx call() throws Exception {
                return startGrid(1);
            }
        });


        U.sleep(2000);

        try (IgniteDataStreamer<Integer, Integer> st = ig0.dataStreamer(DEFAULT_CACHE_NAME)) {
            st.allowOverwrite(true);

            for (int i = 0; i < entries; i++)
                st.addData(i, -i);
        }

        rebalanceResumed = true;

        for (Runnable runnable : queue) {
            runnable.run();
        }

        queue.clear();

        IgniteEx ig1 = igniteExIgniteInternalFuture.get();

        IgniteInternalFuture ig0Fut = runAsync(new Runnable() {
            @Override
            public void run() {
                IgniteCache<Object, Object> cache = ig0.cache(DEFAULT_CACHE_NAME);

                for (int i = 0; i < entries; i++) {
                    cache.getAndPut(entries / 2, i);
                }
            }
        });

        IgniteInternalFuture ig1Fut = runAsync(new Runnable() {
            @Override
            public void run() {
                IgniteCache<Object, Object> cache = ig1.cache(DEFAULT_CACHE_NAME);

                for (int i = 0; i < entries; i++) {
                    cache.getAndPut(entries / 2 + 1, i);
                }
            }
        });

        ig0Fut.get();

        ig1Fut.get();


        awaitPartitionMapExchange();

    }

    /** {@inheritDoc} */
    @Override protected long getPartitionMapExchangeTimeout() {
        return super.getPartitionMapExchangeTimeout() * 2;
    }

    /**
     *
     */
    protected static class BlockTcpCommunicationSpi extends TcpCommunicationSpi {
        private volatile IgniteInClosure<GridDhtPartitionsSingleMessage> cls;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {

            if (!rebalanceResumed && ((GridIoMessage)msg).message().getClass().equals(GridDhtPartitionDemandMessage.class)) {
                boolean b = log == null;

                String thisStr = this.toString();

                IgniteLogger log1 = log;

                log = log1;

                if (b)
                    System.err.println("NULLL!!!");
                else
                    System.err.println("!!! NULLL!!!");


                queue.add(new Runnable() {
                    @Override
                    public void run() {
                        System.err.println(b);
                        System.err.println(BlockTcpCommunicationSpi.this.log == null);
                        System.err.println(thisStr);
                        System.err.println(log1);

                        BlockTcpCommunicationSpi.this.sendMessage(node, msg, ackC);
                    }
                });

                log.info("Block message: " + msg);

                return;
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
