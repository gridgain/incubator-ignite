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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.connect;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.execute;

/**
 * Test for {@code SELECT FOR UPDATE} queries.
 */
public class CacheMvccSelectForUpdateQueryTest extends CacheMvccAbstractTest {
    /** */
    private static final int CACHE_SIZE = 50;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(3);

        CacheConfiguration seg = new CacheConfiguration("segmented*");

        seg.setQueryParallelism(4);

        grid(0).addCacheConfiguration(seg);

        try (Connection c = connect(grid(0))) {
            execute(c, "create table person (id int primary key, firstName varchar, lastName varchar) " +
                "with \"atomicity=transactional,cache_name=Person\"");

            execute(c, "create table person_seg (id int primary key, firstName varchar, lastName varchar) " +
                "with \"atomicity=transactional,cache_name=PersonSeg,template=segmented\"");

            try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                TransactionIsolation.REPEATABLE_READ)) {

                for (int i = 1; i <= CACHE_SIZE; i++) {
                    execute(c, "insert into person(id, firstName, lastName) values(" + i + ",'" + i + "','" + i + "')");

                    execute(c, "insert into person_seg(id, firstName, lastName) " +
                        "values(" + i + ",'" + i + "','" + i + "')");
                }

                tx.commit();
            }
        }

        AffinityTopologyVersion curVer = grid(0).context().cache().context().exchange().readyAffinityVersion();

        AffinityTopologyVersion nextVer = curVer.nextMinorVersion();

        // Let's wait for rebalance to complete.
        for (int i = 0; i < 3; i++) {
            IgniteEx node = grid(i);

            IgniteInternalFuture<AffinityTopologyVersion> fut =
                node.context().cache().context().exchange().affinityReadyFuture(nextVer);

            if (fut != null)
                fut.get();
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try (Connection c = connect(grid(0))) {
            execute(c, "drop table if exists person");

            execute(c, "drop table if exists person_seg");
        }

        super.afterTest();
    }

    /**
     *
     */
    public void testSelectForUpdateDistributed() throws Exception {
        doTestSelectForUpdateDistributed("Person");
    }

    /**
     *
     */
    public void testSelectForUpdateDistributedSegmented() throws Exception {
        doTestSelectForUpdateDistributed("PersonSeg");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void doTestSelectForUpdateDistributed(String cacheName) throws Exception {
        IgniteEx node = grid(0);

        IgniteCache<Integer, ?> cache = node.cache(cacheName);

        try (Transaction ignored = node.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
            TransactionIsolation.REPEATABLE_READ)) {
            SqlFieldsQuery qry = new SqlFieldsQuery("select id from " + tableName(cache) + " order by id for update")
                .setPageSize(10);

            List<List<?>> res = cache.query(qry).getAll();

            assertEquals(CACHE_SIZE, res.size());

            List<Integer> keys = new ArrayList<>();

            for (int i = 1; i <= CACHE_SIZE; i++)
                keys.add(i);

            checkLocks(cacheName, keys);
        }
    }

    /**
     *
     */
    public void testSelectForUpdateLocal() throws Exception {
        doTestSelectForUpdateLocal("Person");
    }

    /**
     *
     */
    public void testSelectForUpdateLocalSegmented() throws Exception {
        doTestSelectForUpdateLocal("PersonSeg");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void doTestSelectForUpdateLocal(String cacheName) throws Exception {
        Ignite node = grid(0);

        IgniteCache<Integer, ?> cache = node.cache(cacheName);

        try (Transaction ignored = node.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
            TransactionIsolation.REPEATABLE_READ)) {

            SqlFieldsQuery qry = new SqlFieldsQuery("select id, * from " + tableName(cache) + " order by id for update")
                .setLocal(true);

            List<List<?>> res = cache.query(qry).getAll();

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res)
                keys.add((Integer) r.get(0));

            checkLocks(cacheName, keys);
        }
    }

    /**
     *
     */
    public void testSelectForUpdateWithUnion() {
        assertQueryThrows("select id from person union select 1 for update",
            "SELECT UNION FOR UPDATE is not supported.");
    }

    /**
     *
     */
    public void testSelectForUpdateWithJoin() {
        assertQueryThrows("select p1.id from person p1 join person p2 on p1.id = p2.id for update",
            "SELECT FOR UPDATE with joins is not supported.");
    }

    /**
     *
     */
    public void testSelectForUpdateWithLimit() {
        assertQueryThrows("select id from person limit 0,5 for update",
            "LIMIT/OFFSET clauses are not supported for SELECT FOR UPDATE.");
    }

    /**
     *
     */
    public void testSelectForUpdateWithGroupings() {
        assertQueryThrows("select count(*) from person for update",
            "SELECT FOR UPDATE with aggregates and/or GROUP BY is not supported.");

        assertQueryThrows("select lastName, count(*) from person group by lastName for update",
            "SELECT FOR UPDATE with aggregates and/or GROUP BY is not supported.");
    }

    /**
     * Check that an attempt to get a lock on any key from given list fails by timeout.
     *
     * @param cacheName Cache name to check.
     * @param keys Keys to check.
     * @throws Exception if failed.
     */
    @SuppressWarnings({"ThrowableNotThrown", "unchecked"})
    private void checkLocks(String cacheName, List<Integer> keys) throws Exception {
        ExecutorService svc = Executors.newFixedThreadPool(4);

        List<Callable<Integer>> calls = new ArrayList<>();

        Ignite node = ignite(2);

        IgniteCache cache = node.cache(cacheName);

        for (int key : keys) {
            calls.add(new Callable<Integer>() {
                /** {@inheritDoc} */
                @Override public Integer call() {
                    try (Transaction ignored = node.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                        TransactionIsolation.REPEATABLE_READ)) {
                        List<List<?>> res = cache
                            .query(
                                new SqlFieldsQuery("select * from " + tableName(cache) +
                                    " where id = " + key + " for update").setTimeout(2, TimeUnit.SECONDS)
                            )
                            .getAll();

                        return (Integer) res.get(0).get(0);
                    }
                }
            });
        }

        Iterator<Future<Integer>> res = svc.invokeAll(calls).iterator();

        Map<Integer, Exception> errs = new HashMap<>();

        try {
            for (int key : keys) {
                assertTrue(res.hasNext());

                Future<Integer> fut = res.next();

                try {
                    fut.get();
                }
                catch (ExecutionException e) {
                    errs.put(key, (Exception)e.getCause());
                }
            }
        }
        finally {
            svc.shutdownNow();
        }

        for (int key : keys) {
            Exception e = errs.get(key);

            assertNotNull("Concurrent transaction has managed to get lock on key " + key + '.', e);

            GridTestUtils.assertThrows(null, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    throw e;
                }
            }, CacheException.class, "IgniteTxTimeoutCheckedException");
        }
    }

    /**
     * @param cache Cache.
     * @return Name of the table contained by this cache.
     */
    @SuppressWarnings("unchecked")
    private static String tableName(IgniteCache<?, ?> cache) {
        return ((Collection<QueryEntity>)cache.getConfiguration(CacheConfiguration.class).getQueryEntities())
            .iterator().next().getTableName();
    }

    /**
     * Test that query throws exception with expected message.
     * @param qry SQL.
     * @param exMsg Expected message.
     */
    private void assertQueryThrows(String qry, String exMsg) {
        assertQueryThrows(qry, exMsg, false);

        assertQueryThrows(qry, exMsg, true);
    }

    /**
     * Test that query throws exception with expected message.
     * @param qry SQL.
     * @param exMsg Expected message.
     * @param loc Local query flag.
     */
    private void assertQueryThrows(String qry, String exMsg, boolean loc) {
        Ignite node = grid(0);

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() {
                return node.cache("Person").query(new SqlFieldsQuery(qry).setLocal(loc)).getAll();
            }
        }, IgniteSQLException.class, exMsg);
    }
}
