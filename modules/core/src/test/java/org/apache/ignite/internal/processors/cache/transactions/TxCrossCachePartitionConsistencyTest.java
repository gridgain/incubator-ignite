/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 */
public class TxCrossCachePartitionConsistencyTest extends GridCommonAbstractTest {
    /** Cache 1. */
    private static final String CACHE1 = DEFAULT_CACHE_NAME;

    /** Cache 2. */
    private static final String CACHE2 = DEFAULT_CACHE_NAME + "2";

    /** */
    private static final int MB = 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode("client".equals(igniteInstanceName));
        cfg.setCacheConfiguration(cacheConfiguration(CACHE1, 2), cacheConfiguration(CACHE2, 1));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().
                setInitialSize(100 * MB).setMaxSize(300 * MB)));

        return cfg;
    }

    /**
     * @param name Name.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(String name, int backups) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(name);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(backups);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setOnheapCacheEnabled(false);

        return ccfg;
    }

    /** */
    public void testCrossCacheTxFailover() throws Exception {
        try {
            IgniteEx crd = startGrids(3);

            awaitPartitionMapExchange();

            Ignite client = startGrid("client");

            AtomicBoolean stop = new AtomicBoolean();

            Random r = new Random();

            BooleanSupplier stopPred = stop::get;

            IgniteInternalFuture<?> restartFut = multithreadedAsync(() -> {
                while (!stopPred.getAsBoolean()) {
                    doSleep(2_000);

                    Ignite restartNode = grid(r.nextInt(3));

                    String name = restartNode.name();

                    stopGrid(name, true);

                    try {
                        doSleep(2_000);

                        startGrid(name);
                    }
                    catch (Exception e) {
                        fail(X.getFullStackTrace(e));
                    }
                }
            }, 1, "node-restarter");

//            IgniteInternalFuture clFut = GridTestUtils.runAsync(new Runnable() {
//                @Override public void run() {
//                    while (!stopPred.getAsBoolean()) {
//                        try {
//                            Ignite c2 = startGrid("client2");
//                            c2.close();
//                        }
//                        catch (Exception e) {
//                            fail(X.getFullStackTrace(e));
//                        }
//                    }
//                }
//            });

            List<Integer> keys = IntStream.range(0, 1024).boxed().collect(Collectors.toList());

            IgniteInternalFuture<?> txFut = doRandomUpdates(r, client, keys, stopPred);

            doSleep(10_000);

            stop.set(true);

            txFut.get();
            restartFut.get();
            //clFut.get();

            awaitPartitionMapExchange();

            assertPartitionsSame(idleVerify(client, CACHE1, CACHE2));

            long s = 0;

            for (Integer key : keys) {
                Deposit o = (Deposit)client.cache(CACHE1).get(key);
                Deposit o2 = (Deposit)client.cache(CACHE2).get(key);

                s += o.getBalance();
                s += o2.getBalance();
            }

            assertEquals(keys.size() * 2L * 1_000_000_000, s);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param r Random.
     * @param near Near node.
     * @param keys Primary keys.
     * @param stopPred Stop predicate.
     * @return Finish future.
     */
    private IgniteInternalFuture<?> doRandomUpdates(Random r, Ignite near, List<Integer> keys, BooleanSupplier stopPred)
        throws Exception {
        try(IgniteDataStreamer<Object, Object> ds = near.dataStreamer(CACHE1)) {
            ds.allowOverwrite(true);

            for (Integer key : keys)
                ds.addData(key, new Deposit(key, 1_000_000_000));
        }

        try(IgniteDataStreamer<Object, Object> ds = near.dataStreamer(CACHE2)) {
            ds.allowOverwrite(true);

            for (Integer key : keys)
                ds.addData(key, new Deposit(key, 1_000_000_000));
        }

        IgniteCache<Integer, Deposit> cache1 = near.cache(CACHE1);
        IgniteCache<Integer, Deposit> cache2 = near.cache(CACHE2);

        return multithreadedAsync(() -> {
            while (!stopPred.getAsBoolean()) {
                int key = r.nextInt(keys.size());

                try (Transaction tx = near.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 0)) {
                    IgniteCache<Integer, Deposit> first = cache1;
                    IgniteCache<Integer, Deposit> second = cache2;

                    Deposit d1 = first.get(key);

                    assertNotNull(d1);

                    Deposit d2 = second.get(key);

                    d1.setBalance(d1.getBalance() + 20);
                    d2.setBalance(d2.getBalance() - 20);

//                    boolean rmv = r.nextFloat() < 0.4;
//                    if (rmv) {
//                        first.remove(key);
//                        second.remove(key2);
//
//                        first.put(-key, d1);
//                        second.put(-key2, d2);
//                    }
//                    else {
                        first.put(key, d1);
                        second.put(key, d2);
//                    }

                    tx.commit();
                }
                catch (Exception e) {
                    assertTrue(X.getFullStackTrace(e), X.hasCause(e, ClusterTopologyException.class) ||
                        X.hasCause(e, TransactionRollbackException.class));
                }
            }
        }, Runtime.getRuntime().availableProcessors() * 2, "tx-update-thread");
    }

    private static class Deposit {
        private long userId;

        private long balance;

        public Deposit(long userId, long balance) {
            this.userId = userId;
            this.balance = balance;
        }

        public long getUserId() {
            return userId;
        }

        public void setUserId(long userId) {
            this.userId = userId;
        }

        public long getBalance() {
            return balance;
        }

        public void setBalance(long balance) {
            this.balance = balance;
        }
    }
}
