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
package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Partition counters test.
 */
@SuppressWarnings("unchecked")
public class CacheMvccPartitionCountersTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionCounters() throws Exception {
        persistence = true;
        disableScheduledVacuum = true;

        CacheConfiguration ccfg1 = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, 1024);
        CacheConfiguration ccfg2 = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, 1024);
        ccfg1.setGroupName("group1");
        ccfg2.setGroupName("group2");
        ccfg1.setName("cache1");
        ccfg2.setName("cache2");

        CacheConfiguration ccfg3 = cacheConfiguration(PARTITIONED, FULL_SYNC, 1, 2);
        ccfg3.setName("cache3");

        CacheConfiguration ccfg4 = cacheConfiguration(REPLICATED, FULL_SYNC, 0, 1024);
        ccfg4.setName("cache4");

        ccfgs = new CacheConfiguration[] {ccfg1, ccfg2, ccfg3, ccfg4};

        startGrids(3);

        client = true;

        startGrid(4);

        client = false;

        grid(0).cluster().active(true);

        final int SIZE = 100;

        for (Ignite ignite : G.allGrids()) {
            for (int i = 0; i < SIZE; i++)
                randomCache(ignite).put(i % 10, i);
        }

        verifyPartitionCounters();

        for (Ignite ignite : G.allGrids()) {
            IgniteCache cache = randomCache(ignite);

            for (int k = 0; k < 10; k++) {
                try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                    for (int i = 0; i < SIZE; i++)
                        if (ThreadLocalRandom.current().nextBoolean())
                            cache.put(i, i * 2);
                        else
                            cache.remove(i);

                    if (ThreadLocalRandom.current().nextBoolean())
                        tx.commit();
                    else
                        tx.rollback();
                }
            }
        }

        verifyPartitionCounters();

        stopGrid(0);

        awaitPartitionMapExchange();

        verifyPartitionCounters();

        startGrid(5);

        awaitPartitionMapExchange();

        verifyPartitionCounters();

        for (Ignite ignite : G.allGrids()) {
            IgniteCache cache = randomCache(ignite);

            for (int k = 0; k < 10; k++) {
                try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                    for (int i = 0; i < SIZE; i++)
                        if (ThreadLocalRandom.current().nextBoolean())
                            cache.put(i, i * 2);
                        else
                            cache.remove(i);

                    if (ThreadLocalRandom.current().nextBoolean())
                        tx.commit();
                    else
                        tx.rollback();
                }
            }
        }

        verifyPartitionCounters();

        startGrid(0);

        awaitPartitionMapExchange();

        verifyPartitionCounters();
    }

    /**
     * @param ignite Node.
     * @return Random cache.
     */
    private IgniteCache randomCache(Ignite ignite) {
        CacheConfiguration cfg = ccfgs[ThreadLocalRandom.current().nextInt(ccfgs.length)];

        return ignite.cache(cfg.getName());
    }
}
