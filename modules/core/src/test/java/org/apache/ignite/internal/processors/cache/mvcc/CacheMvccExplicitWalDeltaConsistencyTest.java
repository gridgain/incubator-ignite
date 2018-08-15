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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.memtracker.PageMemoryTrackerPluginProvider;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.GET;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.WriteMode.PUT;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Mvcc WAL delta records consistency test.
 */
public class CacheMvccExplicitWalDeltaConsistencyTest extends CacheMvccAbstractWalDeltaConsistencyTest {
    /**
     * @throws Exception If failed.
     */
    public final void testPutRemoveAfterCheckpoint() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 1, DFLT_PARTITION_COUNT);

        IgniteEx ignite = startGrid(2);

        ignite.cluster().active(true);

        IgniteCache<Integer, Object> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 0; i < 5_000; i++)
                cache.put(i, "Cache value " + i);

            tx.commit();
        }

        for (int i = 1_000; i < 2_000; i++) {
            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(i, i);

                tx.commit();
            }
        }

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 500; i < 1_500; i++)
                cache.remove(i);

            tx.commit();
        }

        for (int i = 1_000; i < 2_000; i++) {
            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(i, i);

                tx.rollback();
            }
        }

        assertTrue(PageMemoryTrackerPluginProvider.tracker(ignite).checkPages(true));
        assertTrue(PageMemoryTrackerPluginProvider.tracker(grid(2)).checkPages(true));

        forceCheckpoint();

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 3_000; i < 10_000; i++)
                cache.put(i, "Changed cache value " + i);

            tx.commit();
        }

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 4_000; i < 7_000; i++)
                cache.remove(i);

            tx.commit();
        }

        assertTrue(PageMemoryTrackerPluginProvider.tracker(ignite).checkPages(true));
        assertTrue(PageMemoryTrackerPluginProvider.tracker(grid(2)).checkPages(true));

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public final void testNotEmptyPds() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 0, DFLT_PARTITION_COUNT);

        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Integer, Object> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 3_000; i++)
            cache.put(i, "Cache value " + i);

        stopGrid(0);

        ignite = startGrid(0);

        ignite.cluster().active(true);

        cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 2_000; i < 5_000; i++)
                cache.put(i, "Changed cache value " + i);

            tx.commit();
        }

        for (int i = 1_000; i < 4_000; i++) {
            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.remove(i);

                tx.commit();
            }
        }

        assertTrue(PageMemoryTrackerPluginProvider.tracker(ignite).checkPages(true));

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentWrites() throws Exception {
        int srvs = 4;
        int clients = 2;

        accountsTxReadAll(srvs, clients, 2, 2, null, true, GET, PUT, 5_000, null);

        for (Ignite g : G.allGrids()) {
            if (!g.configuration().isClientMode())
                assertTrue(PageMemoryTrackerPluginProvider.tracker(g).checkPages(true));
        }
    }
}
