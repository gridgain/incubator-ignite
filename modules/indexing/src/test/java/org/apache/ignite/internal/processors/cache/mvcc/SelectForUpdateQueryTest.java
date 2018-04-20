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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.X;
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

            for (int i = 1; i <= 100; i++)
                execute(c, "insert into person(id, firstName, lastName) values(" + i + ",'" + i + "','"  + i + "')");
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try (Connection c = connect(grid(0))) {
            execute(c, "drop table if exists person");
        }

        super.afterTest();
    }

    @Override
    protected long getTestTimeout() {
        return TimeUnit.MINUTES.toMillis(5);
    }

    /**
     *
     */
    public void testSelectForUpdate() throws Exception {
        IgniteEx node = grid(0);

        IgniteCache<Integer, ?> cache = node.cache("Person");

        cache.query(new SqlFieldsQuery("begin"));

        try {
            SqlFieldsQuery qry = new SqlFieldsQuery("select id from person order by id for update").setPageSize(10);

            List<List<?>> res = cache.query(qry).getAll();

            assertEquals(100, res.size());

            List<Integer> keys = new ArrayList<>();

            for (int i = 1; i <= 10; i++)
                keys.add(i);

            checkLocks(keys);
        }
        finally {
            node.cache("Person").query(new SqlFieldsQuery("rollback"));
        }
    }

    private void checkLocks(List<Integer> keys) throws Exception {
        ExecutorService svc = Executors.newSingleThreadExecutor();

        List<KeyCheckCallable> calls = new ArrayList<>();

        Ignite node = ignite(2);

        for (int i : keys)
            calls.add(new KeyCheckCallable(i, node));

        Iterator<Future<Integer>> res = svc.invokeAll(calls).iterator();

        X.println("**CHKNG");

        Map<Integer, Exception> errs = new HashMap<>();

        try {
            for (int i : keys) {
                assertTrue(res.hasNext());

                Future<Integer> fut = res.next();

                try {
                    fut.get();
                }
                catch (ExecutionException e) {
                    errs.put(i, (Exception)e.getCause());
                }
            }
        }
        finally {
            svc.shutdownNow();
        }

        for (int i : keys) {
            Exception e = errs.get(i);

            assertNotNull("Concurrent transaction managed to get lock on key " + i, e);
        }
    }

    private static class KeyCheckCallable implements Callable<Integer> {
        private final int key;

        private final Ignite node;

        private final CountDownLatch startLatch = new CountDownLatch(1);

        private KeyCheckCallable(int key, Ignite node) {
            this.key = key;
            this.node = node;
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            try (Transaction ignored = node.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                TransactionIsolation.REPEATABLE_READ)) {
                List<List<?>> res = node.cache("Person")
                    .query(
                        new SqlFieldsQuery("select * from person where id = " + key + " for update")
                            .setTimeout(2, TimeUnit.SECONDS)
                    )
                    .getAll();

                return (Integer)res.get(0).get(0);
            }
        }
    }

    /**
     *
     */
    public void testSelectForUpdateLocal() {
        Ignite node = grid(0);

        node.cache("Person").query(new SqlFieldsQuery("begin"));

        SqlFieldsQuery qry = new SqlFieldsQuery("select id from person order by id for update").setLocal(true);

        List<List<?>> res = node.cache("Person").query(qry).getAll();

        Set<Integer> keys = new HashSet<>();

        for (List<?> r : res)
            X.println(r.get(0).toString());

        node.cache("Person").query(new SqlFieldsQuery("rollback"));
    }
}
