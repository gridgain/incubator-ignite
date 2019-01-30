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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Complex concurrent drop and create table test.
 */
public class IgniteCacheDropTableBlockingSelfTest extends GridCommonAbstractTest {
    private static volatile boolean testFinished;

    /** Cache name. */
    private static final String CACHE_NAME = "THE_CACHE";

    /** Ignite name. */
    private static final String IGNITE_NAME = "THE_NAME";

    private final List<Thread> bgTasks = new ArrayList<>();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(getConfiguration(IGNITE_NAME));
    }

    protected CacheConfiguration <?, ?> newCacheConfiguration() {
        CacheConfiguration<?,?> ccfg = defaultCacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setSqlFunctionClasses(TestFunctions.class);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * We need to refresh latches before each test execution.
     */
    @Before
    public void refresh() {
        grid(IGNITE_NAME).destroyCache(CACHE_NAME);

        TestFunctions.resetLatches();

        testFinished = false;
    }

    /**
     * Release latch.
     */
    @After
    public void stop() {
        testFinished = true;

        TestFunctions.qryStarted.countDown();
        TestFunctions.testIsDone.countDown();

        for (Thread task : bgTasks)
            task.interrupt();
    }

    private void executeHanging(Runnable cl) {
        Thread th = new Thread(()-> {
            try {
                cl.run();
            }
            catch (Exception e) {
                if (!testFinished)
                     throw new AssertionError(
                         "Hanging action has been interrupted by exception before test finished.", e);
            }

            if (!testFinished)
                throw new AssertionError("Hanging action finished before test finished.");
        });

        th.start();

        bgTasks.add(th);
    }

    @Test
    public void testNativeDrop() throws Exception {
        IgniteCache cache = grid(IGNITE_NAME).createCache(newCacheConfiguration());

        final SqlFieldsQuery CREATE_TAB = new SqlFieldsQuery(
            "CREATE TABLE TEST_DROP_AND_CREATE (id INT PRIMARY KEY, name VARCHAR)").setSchema(CACHE_NAME);

        final SqlFieldsQuery DROP_TAB = new SqlFieldsQuery("DROP TABLE TEST_DROP_AND_CREATE").setSchema(CACHE_NAME);

        cache.query(CREATE_TAB).getAll();

        for (int i = 0; i < 10; i++)
            cache.query(
                new SqlFieldsQuery("INSERT INTO TEST_DROP_AND_CREATE VALUES (?, ?)")
                    .setArgs(i, "Name#" + i)
                    .setSchema(CACHE_NAME)
            );

        // 1. Run hanging SELECT:
        Thread hangingSel = new Thread(() ->
            cache.query(
                new SqlFieldsQuery("SELECT * FROM TEST_DROP_AND_CREATE WHERE ID = notifyQueryStartedAndSleep(1)")
                    .setSchema(CACHE_NAME)
            ).getAll()
        );

        hangingSel.start();

        TestFunctions.qryStarted.await();

        //2.
        Thread hangingDrop1 = new Thread(() -> grid(IGNITE_NAME).destroyCache("SQL_THE_CACHE_TEST_DROP_AND_CREATE"));

        hangingDrop1.start();

        GridTestUtils.assertThrows(log(),
            () -> getTheCache().query(CREATE_TAB).getAll(),
            IgniteSQLException.class,
            "Table already exists: TEST_DROP_AND_CREATE");

        grid(IGNITE_NAME).destroyCache("SQL_THE_CACHE_TEST_DROP_AND_CREATE");
    }

    /**
     * Check that drop table is just blocked by the background SELECT execution.
     */
    @Test
    public void testDropAndCreateWithRunningSelect() throws Exception {
        IgniteCache cache = grid(IGNITE_NAME).createCache(newCacheConfiguration());

        final SqlFieldsQuery CREATE_TAB = new SqlFieldsQuery(
            "CREATE TABLE TEST_DROP_AND_CREATE (id INT PRIMARY KEY, name VARCHAR)").setSchema(CACHE_NAME);

        final SqlFieldsQuery DROP_TAB = new SqlFieldsQuery("DROP TABLE TEST_DROP_AND_CREATE").setSchema(CACHE_NAME);

        cache.query(CREATE_TAB).getAll();

        for (int i = 0; i < 10; i++)
            cache.query(
                new SqlFieldsQuery("INSERT INTO TEST_DROP_AND_CREATE VALUES (?, ?)")
                    .setArgs(i, "Name#" + i)
                    .setSchema(CACHE_NAME)
            );

        // 1. Run hanging SELECT:
        Thread hangingSel = new Thread(() ->
            cache.query(
                new SqlFieldsQuery("SELECT * FROM TEST_DROP_AND_CREATE WHERE ID = notifyQueryStartedAndSleep(1)")
                .setSchema(CACHE_NAME)
            ).getAll()
        );

        hangingSel.start();

        TestFunctions.qryStarted.await();

        //2. 
        Thread hangingDrop1 = new Thread(() -> getTheCache().query(DROP_TAB).getAll());

        hangingDrop1.start();

        GridTestUtils.assertThrows(log(),
            () -> getTheCache().query(CREATE_TAB).getAll(),
            IgniteSQLException.class,
            "Table already exists: TEST_DROP_AND_CREATE");

        getTheCache().query(DROP_TAB).getAll();
    }

    private IgniteCache<Object, Object> getTheCache() {
        return grid(IGNITE_NAME).cache(CACHE_NAME);
    }

    public static class TestFunctions {
        /** Need to suspend thread of the test till SELECT query starts.*/
        static CountDownLatch qryStarted;

        /** Need to suspend query execution thread till test is done. */
        static CountDownLatch testIsDone;

        /**
         * Reset the latches.
         */
        static void resetLatches(){
            qryStarted = new CountDownLatch(1);
            testIsDone = new CountDownLatch(1);
        }

        @QuerySqlFunction
        public static int notifyQueryStartedAndSleep(int ret){
            try {
                qryStarted.countDown();

                testIsDone.await();
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();

                throw new RuntimeException("Test have been interrupted", ie);
            }

            return ret;
        }
    }
}
