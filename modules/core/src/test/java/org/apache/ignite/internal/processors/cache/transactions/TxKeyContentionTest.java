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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 */
public class TxKeyContentionTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 3;

    /** */
    public static final long MB = 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        boolean client = igniteInstanceName.startsWith("client");

        cfg.setClientMode(client);
        cfg.setStripedPoolSize(Runtime.getRuntime().availableProcessors() * 2);

//        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setWalMode(LOG_ONLY).setPageSize(1024).
//            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true).
//                setInitialSize(100 * MB).setMaxSize(100 * MB)));

        if (!client) {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(2);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setOnheapCacheEnabled(false);

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

//    /** {@inheritDoc} */
//    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
//        return new StopNodeFailureHandler();
//    }

    /**
     * @return Started client.
     * @throws Exception If node failed.
     */
    private Ignite startClient(int idx) throws Exception {
        Ignite client = startGrid("client" + idx);

        assertTrue(client.configuration().isClientMode());

        assertNotNull(client.cache(DEFAULT_CACHE_NAME));

        return client;
    }

    /**
     *
     */
    public void testTimeoutOnContention() throws Exception {
        startGridsMultiThreaded(GRID_CNT);

        Ignite client = startClient(0);

        AtomicBoolean stop = new AtomicBoolean();

        final Random r = new Random();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                while (!stop.get()) {
                    try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ,
                        r.nextBoolean() ? 200 : 0, 1)) {
                        Integer val = (Integer)client.cache(DEFAULT_CACHE_NAME).get(0);

                        if (val == null)
                            val = 0;

                        client.cache(DEFAULT_CACHE_NAME).put(0, val + 1);
                    }
                    catch (Exception e) {
                        assertTrue(X.hasCause(e, TransactionTimeoutException.class));
                    }
                }
            }
        }, Runtime.getRuntime().availableProcessors());

        doSleep(120_000);

        stop.set(true);

        fut.get();
    }

    public void testException() throws Exception {
        Ignite crd = startGridsMultiThreaded(GRID_CNT);

        Ignite client = startClient(0);
        Ignite client2 = startClient(1);
        Ignite client3 = startClient(2);

        AtomicInteger inc = new AtomicInteger();

        CountDownLatch lockLatch = new CountDownLatch(1);
        CountDownLatch commitLatch = new CountDownLatch(1);

        AtomicReference<Transaction> ref = new AtomicReference<>();
        AtomicReference<Transaction> ref2 = new AtomicReference<>();

        int key = 0;

        IgniteEx node = (IgniteEx)primaryNode(key, DEFAULT_CACHE_NAME);

        final int cnt = 3;

        client.cache(DEFAULT_CACHE_NAME).put(key, 0);

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                switch (inc.getAndIncrement()) {
                    case 0:
                        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
                            client.cache(DEFAULT_CACHE_NAME).get(key);

                            lockLatch.countDown();

                            U.awaitQuiet(commitLatch);

                            tx.commit();
                        }
                        catch (Exception e) {
                            // assertTrue(e.getClass().getCanonicalName(), X.hasCause(e, TransactionTimeoutException.class));
                            System.out.println();
                        }

                        break;
                    case 1:
                        try (Transaction tx = client2.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
                            ref.set(tx);

                            U.awaitQuiet(lockLatch);

                            // Put lock in candidate queue.
                            client2.cache(DEFAULT_CACHE_NAME).get(key);

                            tx.commit();
                        }
                        catch (Exception e) {
                            System.out.println();
                        }

                        break;
                    case 2:
                        try (Transaction tx = client3.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
                            ref2.set(tx);

                            U.awaitQuiet(lockLatch);

                            // Put lock in candidate queue.
                            client3.cache(DEFAULT_CACHE_NAME).get(key);

                            tx.commit();
                        }
                        catch (Exception e) {
                            System.out.println();
                        }

                        break;

                    default:
                        fail();
                }
            }
        }, cnt, "tx-thread");

        U.awaitQuiet(lockLatch);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                Collection<IgniteInternalTx> txs = node.context().cache().context().tm().activeTransactions();

                IgniteInternalTx locTx = txs.iterator().next();

                IgniteTxEntry ent = locTx.writeEntries().iterator().next();

                GridDhtCacheEntry cached = (GridDhtCacheEntry)ent.cached();

                try {
                    Collection<GridCacheMvccCandidate> candidates = cached.localCandidates();

                    return candidates.size() == cnt;
                }
                catch (GridCacheEntryRemovedException e) {
                    // No-op.
                }

                return false;
            }
        }, 5000);

        commitLatch.countDown();

        try {
            U.await(GridDhtLockFuture.finishL2, 5000, TimeUnit.SECONDS);
        }
        catch (IgniteInterruptedCheckedException e) {
            fail("Unexpected interruption");
        }

        IgniteInternalFuture fut2 = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                ref.get().rollback();
            }
        });

        U.awaitQuiet(GridDhtLockFuture.finishL3);

        GridDhtLockFuture.finishL.countDown();

        ref2.get().rollback();

        //GridDhtLockFuture.finishL4.countDown();

        fut.get();
        fut2.get();

        checkFutures();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }
}
