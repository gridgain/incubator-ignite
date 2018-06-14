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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Tests an ability to async rollback near transactions.
 */
public class TxLongRunningRecoveryTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "test";

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 3;

    /** */
    public static final long MB = 1024 * 1024;

    private boolean persistenceEnabled;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        boolean client = igniteInstanceName.startsWith("client");

        cfg.setClientMode(client);

        if (persistenceEnabled)
            cfg.setDataStorageConfiguration(new DataStorageConfiguration().setWalMode(LOG_ONLY).setPageSize(1024).
                setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true).
                    setInitialSize(100 * MB).setMaxSize(100 * MB)));

        if (!client) {
            CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(2);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setOnheapCacheEnabled(false);

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        System.setProperty(IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, "5000");

        System.setProperty(IgniteSystemProperties.IGNITE_ENABLE_RECOVERY_FOR_LONG_RUNNING_TXS, "true");

        final IgniteEx crd = startGrid(0);

        startGridsMultiThreaded(1, GRID_CNT - 1);

        crd.cluster().active(true);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        try {
            stopAllGrids();

            cleanPersistenceDir();
        }
        finally {
            System.clearProperty(IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT);

            System.clearProperty(IgniteSystemProperties.IGNITE_ENABLE_RECOVERY_FOR_LONG_RUNNING_TXS);
        }
    }

    /**
     * @return Started client.
     * @throws Exception If f nodeailed.
     */
    private Ignite startClient(String name) throws Exception {
        Ignite client = startGrid(name);

        assertTrue(client.configuration().isClientMode());

        assertNotNull(client.cache(CACHE_NAME));

        return client;
    }

    /**
     *
     */
    public void testRecovery() throws Exception {
        persistenceEnabled = false;

        doTestRecovery();
    }

    /**
     *
     */
    public void testRecoveryWithPersistence() throws Exception {
        persistenceEnabled = true;

        doTestRecovery();
    }

    /**
     *
     */
    private void doTestRecovery() throws Exception {
        Ignite[] clients = new Ignite[] {
            startClient("client1"),
            startClient("client2"),
            startClient("client3"),
            startClient("client4")
        };

        long seed = System.nanoTime();

        Random r = new Random(seed);

        LongAdder progress = new LongAdder();

        AtomicInteger idx = new AtomicInteger();

        int tc = clients.length;

        CountDownLatch lockLatch = new CountDownLatch(1);
        CountDownLatch commitLatch = new CountDownLatch(1);

        Ignite prim = primaryNode(0L, CACHE_NAME);

        TestRecordingCommunicationSpi primSpi = TestRecordingCommunicationSpi.spi(prim);

        primSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message message) {
                return message instanceof GridDhtTxFinishRequest;
            }
        });

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int id = idx.getAndIncrement();

                Ignite client = clients[id];

                try (Transaction tx = client.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 0, 1)) {
                    IgniteCache<Long, Long> cache = client.cache(CACHE_NAME);

                    if (id != 0)
                        U.awaitQuiet(lockLatch);

                    cache.invoke(0L, new IncrementClosure(), null);

                    if (id == 0) {
                        lockLatch.countDown();

                        U.awaitQuiet(commitLatch);

                        doSleep(500); // Wait until candidates will enqueue.
                    }

                    log.info("Before commit: " + id);

                    tx.commit();
                }
                catch (Exception e) {
                    assertTrue(X.hasCause(e, TransactionTimeoutException.class));
                }

                progress.increment();

            }
        }, tc, "invoke-thread");

        U.awaitQuiet(lockLatch);

        commitLatch.countDown();

        primSpi.waitForBlocked();

        doSleep(3000);

        primSpi.stopBlock(true, new IgnitePredicate<T2<ClusterNode,GridIoMessage>>() {
            @Override public boolean apply(T2<ClusterNode, GridIoMessage> objects) {
                GridIoMessage iom = objects.get2();

                Message m = iom.message();

                if (m instanceof GridDhtTxFinishRequest) {
                    GridDhtTxFinishRequest r = (GridDhtTxFinishRequest)m;

                    if (r.nearNodeId().equals(clients[0].cluster().localNode().id()))
                        return false;
                }

                return true;
            }
        });

        // Wait for all remote txs to finish.
        for (Ignite ignite : G.allGrids()) {
            if (ignite.configuration().isClientMode())
                continue;

            Collection<IgniteInternalTx> txs = ((IgniteEx)ignite).context().cache().context().tm().activeTransactions();

            for (IgniteInternalTx tx : txs)
                if (!tx.local())
                    tx.finishFuture().get();
        }

        // Send remaining message.
        primSpi.stopBlock(true);

        fut.get();

        Long cur = (Long)clients[0].cache(CACHE_NAME).get(0L);

        assertEquals(tc - 1, cur.longValue());

        checkFutures();
    }

    /**
     * Checks if all tx futures are finished.
     */
    private void checkFutures() {
        for (Ignite ignite : G.allGrids()) {
            IgniteEx ig = (IgniteEx)ignite;

            final Collection<GridCacheFuture<?>> futs = ig.context().cache().context().mvcc().activeFutures();

            for (GridCacheFuture<?> fut : futs)
                log.info("Waiting for future: " + fut);

            Collection<IgniteInternalTx> activeTxs = ig.context().cache().context().tm().activeTransactions();

            for (IgniteInternalTx tx : activeTxs)
                log.info("Waiting for transactions: " + tx);

            assertTrue("Expecting no active futures: node=" + ig.localNode().id(), futs.isEmpty());
            assertTrue("Expecting no active transactions: tx=" + ig.localNode().id(), activeTxs.isEmpty());
        }
    }

    /** */
    private static class IncrementClosure implements EntryProcessor<Long, Long, Void>{
        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Long, Long> entry, Object... arguments) throws EntryProcessorException {
            entry.setValue(entry.exists() ? entry.getValue() + 1 : 0);

            return null;
        }
    }
}