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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.memtracker.PageMemoryTrackerPluginProvider;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.SQL;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.WriteMode.DML;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Sql WAL delta records consistency test.
 */
public class CacheMvccSqlSysPropWalDeltaConsistencyTest extends CacheMvccAbstractWalDeltaConsistencyTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(PageMemoryTrackerPluginProvider.IGNITE_ENABLE_PAGE_MEMORY_TRACKER, "true");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(PageMemoryTrackerPluginProvider.IGNITE_ENABLE_PAGE_MEMORY_TRACKER);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPluginConfigurations();

        return cfg;
    }

    /**
     * @throws Exception If fail.
     */
    public void testSimplePartitioned() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class);

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        forceCheckpoint();

        IgniteCache<Integer, Object> cache0 = ignite0.getOrCreateCache(DEFAULT_CACHE_NAME);

        final int scale = 500;

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 0; i < 3 * scale; i++) {
                String qry = "INSERT INTO Integer (_key, _val) VALUES (" + i + ", " + 2 * i + ")";

                cache0.query(new SqlFieldsQuery(qry)).getAll();
            }

            tx.commit();
        }

        forceCheckpoint();

        IgniteEx ignite1 = startGrid(1);

        for (int i = 0; i < 3 * scale; i++) {
            try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                String qry = "MERGE INTO Integer (_key, _val) VALUES (" + (i % 5) + ", " + 2 * i + ")";

                cache0.query(new SqlFieldsQuery(qry)).getAll();

                tx.commit();
            }
        }

        forceCheckpoint();

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            StringBuilder sb = new StringBuilder("MERGE INTO Integer (_key, _val) VALUES ");

            boolean first = true;

            for (int i = 2 * scale; i < 5 * scale; i++) {
                if (first)
                    first = false;
                else
                    sb.append(", ");

                sb.append("(").append(i).append(", ").append(3 * i).append(")");
            }

            cache0.query(new SqlFieldsQuery(sb.toString())).getAll();

            tx.commit();
        }

        forceCheckpoint();

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {

            for (int i = scale; i < 4 * scale; i++) {
                String qry = "DELETE FROM Integer WHERE _key=" + i;

                cache0.query(new SqlFieldsQuery(qry)).getAll();
            }

            tx.commit();
        }

        forceCheckpoint();

        IgniteCache<Integer, Object> cache1 = ignite1.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < scale; i++) {
            String qry = "MERGE INTO Integer (_key, _val) VALUES (" + i + ", " + 4 * i + ")";

            cache1.query(new SqlFieldsQuery(qry)).getAll();
        }

        for (int i = 0; i < scale; i++) {
            String qry = "DELETE FROM Integer WHERE _key=" + i;

            cache1.query(new SqlFieldsQuery(qry)).getAll();
        }

        forceCheckpoint();

        stopAllGrids();
    }

    /**
     * @throws Exception If fail.
     */
    public void testSimpleReplicated() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 0, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, Integer.class)
            .setCacheMode(REPLICATED);

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        forceCheckpoint();

        IgniteCache<Integer, Object> cache0 = ignite0.getOrCreateCache(DEFAULT_CACHE_NAME);

        final int scale = 500;

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 0; i < 3 * scale; i++) {
                String qry = "INSERT INTO Integer (_key, _val) VALUES (" + i + ", " + 2 * i + ")";

                cache0.query(new SqlFieldsQuery(qry)).getAll();
            }

            tx.commit();
        }

        forceCheckpoint();

        IgniteEx ignite1 = startGrid(1);

        for (int i = 0; i < 3 * scale; i++) {
            try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                String qry = "MERGE INTO Integer (_key, _val) VALUES (" + (i % 5) + ", " + 2 * i + ")";

                cache0.query(new SqlFieldsQuery(qry)).getAll();

                tx.commit();
            }
        }

        forceCheckpoint();

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            StringBuilder sb = new StringBuilder("MERGE INTO Integer (_key, _val) VALUES ");

            boolean first = true;

            for (int i = 2 * scale; i < 5 * scale; i++) {
                if (first)
                    first = false;
                else
                    sb.append(", ");

                sb.append("(").append(i).append(", ").append(3 * i).append(")");
            }

            cache0.query(new SqlFieldsQuery(sb.toString())).getAll();

            tx.commit();
        }

        forceCheckpoint();

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {

            for (int i = scale; i < 4 * scale; i++) {
                String qry = "DELETE FROM Integer WHERE _key=" + i;

                cache0.query(new SqlFieldsQuery(qry)).getAll();
            }

            tx.commit();
        }

        forceCheckpoint();

        IgniteCache<Integer, Object> cache1 = ignite1.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < scale; i++) {
            String qry = "MERGE INTO Integer (_key, _val) VALUES (" + i + ", " + 4 * i + ")";

            cache1.query(new SqlFieldsQuery(qry)).getAll();
        }

        for (int i = 0; i < scale; i++) {
            String qry = "DELETE FROM Integer WHERE _key=" + i;

            cache1.query(new SqlFieldsQuery(qry)).getAll();
        }

        forceCheckpoint();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentWrites() throws Exception {
        int srvs = 4;
        int clients = 2;

        accountsTxReadAll(srvs, clients, 2, 2,
            new InitIndexing(Integer.class, MvccTestAccount.class), true, SQL, DML, 5_000, null);

        forceCheckpoint();
    }
}
