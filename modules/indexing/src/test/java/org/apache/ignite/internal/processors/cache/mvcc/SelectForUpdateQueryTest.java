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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.connect;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.execute;

/**
 * Test for {@code SELECT FOR UPDATE} queries.
 */
public class SelectForUpdateQueryTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(3);

        try (Connection c = connect(grid(0))) {
            execute(c, "create table person (id int primary key, firstName varchar, lastName varchar) " +
                "with \"atomicity=transactional,cache_name=Person\"");

            for (int i = 1; i <= 15; i++)
                execute(c, "insert into person(id, firstName, lastName) values(" + i + ",'" + i + "','"  + i + "')");
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
        }

        super.afterTest();
    }

    /**
     *
     */
    public void testSelectForUpdate() throws Exception {
        IgniteEx node = grid(0);

        IgniteCache<Integer, ?> cache = node.cache("Person");

        try (Transaction ignored = node.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
            TransactionIsolation.REPEATABLE_READ)) {
            SqlFieldsQuery qry = new SqlFieldsQuery("select id from person order by id for update").setPageSize(2);

            List<List<?>> res = cache.query(qry).getAll();

            assertEquals(15, res.size());

            List<Integer> keys = new ArrayList<>();

            for (int i = 1; i <= 15; i++)
                keys.add(i);

            checkLocks(keys);
        }
    }

    /**
     *
     */
    public void testSelectForUpdateLocal() throws Exception {
        Ignite node = grid(0);

        try (Transaction ignored = node.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
            TransactionIsolation.REPEATABLE_READ)) {

            SqlFieldsQuery qry = new SqlFieldsQuery("select id from person order by id for update").setLocal(true);

            List<List<?>> res = node.cache("Person").query(qry).getAll();

            List<Integer> keys = new ArrayList<>();

            for (List<?> r : res)
                keys.add((Integer) r.get(0));

            checkLocks(keys);
        }
    }

    /**
     * Check that an attempt to get a lock on any key from given list fails by timeout.
     * @param keys Keys to check.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    private void checkLocks(List<Integer> keys) throws Exception {
        ExecutorService svc = Executors.newFixedThreadPool(4);

        List<Callable<Integer>> calls = new ArrayList<>();

        Ignite node = ignite(2);

        for (int key : keys) {
            calls.add(new Callable<Integer>() {
                /** {@inheritDoc} */
                @Override public Integer call() {
                    try (Transaction ignored = node.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                        TransactionIsolation.REPEATABLE_READ)) {
                        List<List<?>> res = node.cache("Person")
                            .query(
                                new SqlFieldsQuery("select * from person where id = " + key + " for update")
                                    .setTimeout(2, TimeUnit.SECONDS)
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
}
