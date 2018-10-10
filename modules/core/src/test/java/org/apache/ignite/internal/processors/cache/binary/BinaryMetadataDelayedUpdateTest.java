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

package org.apache.ignite.internal.processors.cache.binary;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 */
public class BinaryMetadataDelayedUpdateTest extends GridCommonAbstractTest {
    public static final int GRIDS = 6;

    public static final int FIELDS = 10;

    private static final TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        BlockTcpDiscoverySpi spi = new BlockTcpDiscoverySpi();
        spi.skipAddrsRandomization = true;

        cfg.setDiscoverySpi(spi.setIpFinder(ipFinder));

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setClientMode(igniteInstanceName.startsWith("client"));

        cfg.setMetricsUpdateFrequency(600_000);
        cfg.setClientFailureDetectionTimeout(600_000);
        cfg.setFailureDetectionTimeout(600_000);

//        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setWalMode(LOG_ONLY).setPageSize(1024).
//            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true).
//                setInitialSize(100 * MB).setMaxSize(100 * MB)));

        QueryEntity qryEntity = new QueryEntity("java.lang.Integer", "Value");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        for (int i = 0; i < FIELDS; i++)
            fields.put("s" + i, "java.lang.String");

        qryEntity.setFields(fields);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setBackups(0).
            setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).
            setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC).
            setCacheMode(CacheMode.PARTITIONED));

        return cfg;
    }

    public void testMetadataDelayedUpdate() throws Exception {
        int clId = 0;

        int clientCnt = 4;

        for (int i = 0; i < GRIDS; i++) {
            IgniteEx ex = startGrid(i);

            BlockTcpDiscoverySpi spi = (BlockTcpDiscoverySpi)ex.configuration().getDiscoverySpi();

            spi.blockOnNextMessage(new IgniteBiPredicate<ClusterNode, DiscoveryCustomMessage>() {
                @Override public boolean apply(ClusterNode snd, DiscoveryCustomMessage msg) {
                    if (msg instanceof MetadataUpdateAcceptedMessage)
                        doSleep(50);

                    return false;
                }
            });

            for (int j = 0; j < clientCnt; j++)
                startGrid("client" + clId++);
        }

        awaitPartitionMapExchange();

        int pool = GRIDS * clientCnt;

        CountDownLatch l = new CountDownLatch(pool);

        Thread[] threads = new Thread[pool];

        for (int i = 0; i < pool; i++) {
            int clientId = i / clientCnt;

            IgniteEx client = grid("client" + clientId);

            final int finalI = i;

            Thread t = new Thread(new Runnable() {
                @Override public void run() {
                    try (Transaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                        l.countDown();
                        l.await();

                        client.cache(DEFAULT_CACHE_NAME).put(finalI, build(client, finalI));

                        tx.commit();
                    }
                    catch (Throwable t) {
                        log.error("err", t);
                    }
                }
            });

            t.setName("WorkerClient" + i);
            threads[i] = t;
        }

        for (Thread thread : threads)
            thread.start();

        for (Thread thread : threads)
            thread.join();
    }

    protected BinaryObject build(Ignite ignite, int... fields) {
        BinaryObjectBuilder builder = ignite.binary().builder("Value");

        for (int i = 0; i < fields.length; i++) {
            int field = fields[i];

            builder.setField("i" + field, field);
            builder.setField("s" + field, "testVal" + field);
        }

        return builder.build();
    }

    /**
     * Discovery SPI which can simulate network split.
     */
    protected class BlockTcpDiscoverySpi extends TcpDiscoverySpi {
        /** Block predicate. */
        private volatile IgniteBiPredicate<ClusterNode, DiscoveryCustomMessage> blockPred;

        /**
         * @param blockPred Block predicate.
         */
        public void blockOnNextMessage(IgniteBiPredicate<ClusterNode, DiscoveryCustomMessage> blockPred) {
            this.blockPred = blockPred;
        }

        /** */
        public synchronized void stopBlock() {
            blockPred = null;

            notifyAll();
        }

        /**
         * @param addr Address.
         * @param msg Message.
         */
        private synchronized void block(ClusterNode addr, TcpDiscoveryAbstractMessage msg) {
            if (!(msg instanceof TcpDiscoveryCustomEventMessage))
                return;

            TcpDiscoveryCustomEventMessage cm = (TcpDiscoveryCustomEventMessage)msg;

            DiscoveryCustomMessage delegate;

            try {
                DiscoverySpiCustomMessage custMsg = cm.message(marshaller(), U.resolveClassLoader(ignite().configuration()));

                delegate = ((CustomMessageWrapper)custMsg).delegate();

            }
            catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }

            while (blockPred != null && blockPred.apply(addr, delegate)) {
                try {
                    wait();
                }
                catch (InterruptedException e) {
                    stopBlock();

                    Thread.currentThread().interrupt();

                    log.info("Discovery thread was interrupted, finish blocking ");

                    return;
                }
            }
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(
            Socket sock,
            TcpDiscoveryAbstractMessage msg,
            byte[] data,
            long timeout
        ) throws IOException {
            if (spiCtx != null)
                block(spiCtx.localNode(), msg);

            super.writeToSocket(sock, msg, data, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock,
            OutputStream out,
            TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (spiCtx != null)
                block(spiCtx.localNode(), msg);

            //doSleep(500);

            super.writeToSocket(sock, out, msg, timeout);
        }

        /** {@inheritDoc} */
//        @Override protected void writeToSocket(TcpDiscoveryAbstractMessage msg, Socket sock, int res,
//            long timeout) throws IOException {
//            if (spiCtx != null)
//                block(spiCtx.localNode(), msg);
//
//            //doSleep(500);
//
//            super.writeToSocket(msg, sock, res, timeout);
//        }
    }
}
