/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests for transactional SQL.
 */
public class CacheMvccSqlTxQueriesWithReducerTest extends CacheMvccAbstractTest  {
    /** */
    private static final int TIMEOUT = 3000;

    /**
     * @throws Exception If failed.
     */
    public void testInsertQueryWithReducer() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1,new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(1),
            2,new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(2),
            3,new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(3)));

        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(2), cache.get(2));
        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(3), cache.get(3));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TIMEOUT);

            String sqlText = "INSERT INTO MvccTestSqlIndexValue (_key, idxVal1) " +
                "SELECT DISTINCT _key + 3, idxVal1 + 3 FROM MvccTestSqlIndexValue";

            SqlFieldsQuery qry = new SqlFieldsQuery(sqlText);

            qry.setDistributedJoins(true);

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            tx.commit();
        }

        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(2), cache.get(2));
        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(3), cache.get(3));
        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(4), cache.get(4));
        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(5), cache.get(5));
        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(6), cache.get(6));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInsertQueryWithReducerDuplicateKey() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1,new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(1),
            2,new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(2),
            3,new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(3)));

        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(2), cache.get(2));
        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(3), cache.get(3));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TIMEOUT);

            String sqlText = "INSERT INTO MvccTestSqlIndexValue (_key, idxVal1) " +
                "SELECT DISTINCT _key, idxVal1 FROM MvccTestSqlIndexValue";

            SqlFieldsQuery qry = new SqlFieldsQuery(sqlText);

            qry.setDistributedJoins(true);

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                @Override public Object call() {
                    return cache0.query(qry);
                }
            }, IgniteSQLException.class, "Duplicate key");

            tx.rollback();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeQueryWithReducer() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        cache.putAll(F.asMap(
            1,new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(1),
            2,new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(2),
            3,new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(3)));

        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(2), cache.get(2));
        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(3), cache.get(3));

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TIMEOUT);

            String sqlText = "MERGE INTO MvccTestSqlIndexValue (_key, idxVal1) " +
                "SELECT DISTINCT _key * 2, idxVal1 FROM MvccTestSqlIndexValue";

            SqlFieldsQuery qry = new SqlFieldsQuery(sqlText);

            qry.setDistributedJoins(true);

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals(3L, cur.iterator().next().get(0));
            }

            tx.commit();
        }

        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(1), cache.get(1));
        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(1), cache.get(2));
        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(3), cache.get(3));
        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(2), cache.get(4));
        assertEquals(new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(3), cache.get(6));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiBatchPerNode() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 2, DFLT_PARTITION_COUNT)
            .setIndexedTypes(Integer.class, CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue.class);

        startGridsMultiThreaded(4);

        Random rnd = ThreadLocalRandom.current();

        Ignite checkNode  = grid(rnd.nextInt(4));
        Ignite updateNode = grid(rnd.nextInt(4));

        IgniteCache cache = checkNode.cache(DEFAULT_CACHE_NAME);

        final int count = 6;

        Map<Integer, CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue> vals = new HashMap<>(count);

        for (int idx = 1; idx <= count; ++idx)
            vals.put(idx, new CacheMvccSqlTxQueriesTest.MvccTestSqlIndexValue(idx));

        cache.putAll(vals);

        try (Transaction tx = updateNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            tx.timeout(TIMEOUT);

            String sqlText = "INSERT INTO MvccTestSqlIndexValue (_key, idxVal1) " +
                "SELECT DISTINCT _key + 6, idxVal1 + 6 FROM MvccTestSqlIndexValue";

            SqlFieldsQuery qry = new SqlFieldsQuery(sqlText);

            qry.setDistributedJoins(true);
            qry.setPageSize(1);

            IgniteCache<Object, Object> cache0 = updateNode.cache(DEFAULT_CACHE_NAME);

            try (FieldsQueryCursor<List<?>> cur = cache0.query(qry)) {
                assertEquals((long)count, cur.iterator().next().get(0));
            }

            tx.commit();
        }
    }

}
