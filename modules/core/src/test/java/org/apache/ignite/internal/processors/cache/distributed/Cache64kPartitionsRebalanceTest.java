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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 *
 */
public class Cache64kPartitionsRebalanceTest extends GridCommonAbstractTest {
    /** */
    private static final int loadDuration = 30_000;

    /** */
    private boolean persistenceEnabled;

    /** */
    public static final int GRID_CNT = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, CacheConfiguration.MAX_PARTITIONS_COUNT));

        ccfg.setBackups(2);

        cfg.setCacheConfiguration(ccfg);

        cfg.setClientMode("client".equals(igniteInstanceName));

        if (persistenceEnabled) {
            DataStorageConfiguration memCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true))
                .setWalMode(WALMode.LOG_ONLY);

            cfg.setDataStorageConfiguration(memCfg);
        }

        return cfg;
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebalanceNoPersistence() throws Exception {
        testRebalance();
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebalanceWithPersistence() throws Exception {
        persistenceEnabled = true;

        testRebalance();
    }

    /**
     * @throws Exception if failed.
     */
    private void testRebalance() throws Exception {
        startGridsMultiThreaded(GRID_CNT);

        Ignite client = startGrid("client");

        assertNotNull(client.cache(DEFAULT_CACHE_NAME));

        /** Generate desync for {@link loadDuration} */

        AtomicBoolean stop = new AtomicBoolean();

        Random r = new Random();

        LongAdder acc = new LongAdder();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                while(!stop.get()) {
                    int part = r.nextInt(CacheConfiguration.MAX_PARTITIONS_COUNT);

                    int cnt = 1 + r.nextInt(35);

                    int start = r.nextInt(100_000);

                    List<Integer> keys = new ArrayList<>();

                    while(keys.size() != cnt) {
                        if (cnt == 0)
                            break;


                        if (client.affinity(DEFAULT_CACHE_NAME).partition(start) == part)
                            keys.add(start);

                        start++;
                    }

                    assertEquals(cnt, keys.size());

                    try(Transaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ, 100, keys.size())) {
                        TreeMap<Integer, Integer> map = keys.stream().collect(Collectors.toMap(Function.identity(), Function.identity(), null, TreeMap::new));

                        client.cache(DEFAULT_CACHE_NAME).putAll(map);

                        tx.commit();
                    }
                    catch (Exception e) {
                        assertTrue(X.hasCause(e, TransactionTimeoutException.class));
                    }

                    if (acc.sum() % 1000 == 0)
                        log.info("Progress: " + acc.sum());
                }
            }
        }, Runtime.getRuntime().availableProcessors(), "load-thread");

        IgniteInternalFuture restartFut = runAsync(new Runnable() {
            @Override public void run() {
                while(!stop.get()) {
                    int stopIdx = r.nextInt(GRID_CNT);

                    stopGrid(stopIdx);

                    int sleep = 500 + r.nextInt(2000);

                    doSleep(sleep);

                    try {
                        startGrid(stopIdx);
                    }
                    catch (Exception e) {
                        log.error("Error", e);

                        fail();
                    }
                }
            }
        });

        doSleep(loadDuration);

        stop.set(true);

        fut.get();

        restartFut.get();

        verifyBackupPartitions(client, Collections.singleton(DEFAULT_CACHE_NAME));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }
}
