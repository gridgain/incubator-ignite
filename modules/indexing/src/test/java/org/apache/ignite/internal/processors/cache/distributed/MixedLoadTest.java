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

import java.math.BigDecimal;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import javax.cache.Cache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 */
public class MixedLoadTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_ATOMIC_PARTITIONED = "cache10";

    /** */
    public static final String CACHE_ATOMIC_REPLICATED = "cache11";

    /** */
    public static final String CACHE_TX_PARTITIONED = "cache20";

    /** */
    public static final String CACHE_TX_REPLICATED = "cache21";

    public static final String[] CACHES = new String[] {
        CACHE_ATOMIC_PARTITIONED,
        CACHE_ATOMIC_REPLICATED,
        CACHE_TX_PARTITIONED,
        CACHE_TX_REPLICATED
    };

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(
            cacheConfiguration("pGrp", CACHE_ATOMIC_PARTITIONED, PARTITIONED, ATOMIC),
            cacheConfiguration("rGrp", CACHE_ATOMIC_REPLICATED, REPLICATED, ATOMIC),
            cacheConfiguration("pGrp", CACHE_TX_PARTITIONED, PARTITIONED, TRANSACTIONAL),
            cacheConfiguration("rGrp", CACHE_TX_REPLICATED, REPLICATED, TRANSACTIONAL));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setPageSize(1024).
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().
                setInitialSize(1024 * 1024 * 1024).
                setMaxSize(1024 * 1024 * 1024)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        System.setProperty(IgniteSystemProperties.IGNITE_ENABLE_MESSAGE_STATS, "true");

        startGrids(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        System.clearProperty(IgniteSystemProperties.IGNITE_ENABLE_MESSAGE_STATS);
    }

    /**
     * @param name Name.
     * @param cacheMode Cache mode.
     */
    private CacheConfiguration<Integer, TestValue> cacheConfiguration(String grpName, String name, CacheMode cacheMode, CacheAtomicityMode atomicityMode) {
        return new CacheConfiguration<Integer, TestValue>(name).
            setGroupName(grpName).
            setAtomicityMode(atomicityMode).
            setCacheMode(cacheMode).
            setWriteSynchronizationMode(FULL_SYNC).
            setIndexedTypes(Integer.class, TestValue.class).
            setBackups(2);
    }

    /**
     *
     */
    public void testMixedLoad() throws Exception {
        LongAdder delCnt = new LongAdder();
        LongAdder insCnt = new LongAdder();
        LongAdder readCnt = new LongAdder();

        final int preload = 2000;

        Random r = new Random(0);

        AtomicInteger idx = new AtomicInteger();
        IgniteInternalFuture<?> preloadFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int idx0 = idx.getAndIncrement();

                try(IgniteDataStreamer<Integer, Object> streamer = grid(0).dataStreamer(CACHES[idx0])) {
                    for (int i = 0; i < preload; i++)
                        streamer.addData(i, new TestValue(i));
                }
            }
        }, CACHES.length, "loader");

        preloadFut.get(); // Preload keys.

        AtomicBoolean stopped = new AtomicBoolean();

        AtomicInteger uidx = new AtomicInteger();
        IgniteInternalFuture<?> updaterFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int idx = uidx.getAndIncrement();

                AtomicInteger inc = new AtomicInteger(preload);

                while(!stopped.get()) {
                    int op = r.nextInt(5);
                    int nodeIdx = r.nextInt(3);

                    switch (op) {
                        case 0: // remove one of inserted keys.
                            int k = preload + r.nextInt(inc.get());
                            grid(nodeIdx).cache(CACHES[idx]).remove(k);

                            delCnt.increment();

                            break;
                        default: // insert new key.
                            k = inc.getAndIncrement();
                            grid(nodeIdx).cache(CACHES[idx]).put(k, new TestValue(k));

                            insCnt.increment();

                            break;
                    }
                }
            }
        }, CACHES.length, "updater");

        AtomicInteger ridx = new AtomicInteger();
        IgniteInternalFuture<?> readerFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int idx = ridx.getAndIncrement();

                while(!stopped.get()) {
                    int nodeIdx = r.nextInt(3);

                    QueryCursor<Cache.Entry<Integer, TestValue>> qry = grid(nodeIdx).cache(CACHES[idx]).query(new ScanQuery<>());

                    for (Cache.Entry<Integer, TestValue> ignored : qry)
                        readCnt.increment();
                }
            }
        }, CACHES.length, "reader");

        doSleep(35_000);

        stopped.set(true);

        updaterFut.get();
        readerFut.get();

        log.info("Performance score: dels=" + delCnt.sum() + ", inserts=" + insCnt.sum() + ", reads=" + readCnt.sum());
    }

    private static class TestValue {
        /** */
        @QuerySqlField(index = true)
        private int id;

        @QuerySqlField(index = true)
        private String login;

        @QuerySqlField(index = true)
        private String email;

        @QuerySqlField(index = true)
        private BigDecimal salary;

        public TestValue(int id) {
            this.id = id;
            this.login = "test" + id;
            this.email = "test" + id + "@test.ru";
            this.salary = BigDecimal.valueOf((id + 1) * 1_000);
        }

        /**
         * @return Id.
         */
        public int id() {
            return id;
        }

        /**
         * @param id New id.
         */
        public void id(int id) {
            this.id = id;
        }

        /**
         * @return Login.
         */
        public String login() {
            return login;
        }

        /**
         * @param login New login.
         */
        public void login(String login) {
            this.login = login;
        }

        /**
         * @return Email.
         */
        public String email() {
            return email;
        }

        /**
         * @param email New email.
         */
        public void email(String email) {
            this.email = email;
        }

        /**
         * @return Salary.
         */
        public BigDecimal salary() {
            return salary;
        }

        /**
         * @param salary New salary.
         */
        public void salary(BigDecimal salary) {
            this.salary = salary;
        }
    }
}
