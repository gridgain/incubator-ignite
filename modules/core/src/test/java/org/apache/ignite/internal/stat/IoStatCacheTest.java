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
 *
 */

package org.apache.ignite.internal.stat;

import com.google.common.collect.Sets;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

/**
 * Tests for cache IO statistics for inmemory mode.
 */
public class IoStatCacheTest extends GridCommonAbstractTest {
    /** */
    protected final static String ATOMIC_CACHE_NAME = "ATOMIC_CACHE";

    /** */
    protected final static String MVCC_CACHE_NAME = "MVCC_CACHE";

    /** */
    protected final static String TRANSACTIONAL_CACHE_NAME = "TRANSACTIONAL_CACHE";

    /** */
    protected final static Set<String> ALL_CACHE_NAMES = Sets.newHashSet(GridCacheUtils.UTILITY_CACHE_NAME,
        ATOMIC_CACHE_NAME, MVCC_CACHE_NAME, TRANSACTIONAL_CACHE_NAME);

    /** */
    protected static final int RECORD_COUNT = 100;

    /** */
    private static IgniteEx ignite;

    /**
     * @return {@code true} in case persistence mode enabled.
     */
    protected boolean persist() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId("consistentId");

        final CacheConfiguration atomicCacheCfg = new CacheConfiguration()
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setName(ATOMIC_CACHE_NAME);

        final CacheConfiguration mvccCacheCfg = new CacheConfiguration()
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT)
            .setName(MVCC_CACHE_NAME);

        final CacheConfiguration transactionalCacheCfg = new CacheConfiguration()
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setName(TRANSACTIONAL_CACHE_NAME);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(100L * 1024 * 1024)
                    .setPersistenceEnabled(persist()))
            .setPageSize(4 * 1024)
            .setWalMode(WALMode.NONE);

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setCacheConfiguration(transactionalCacheCfg, atomicCacheCfg, mvccCacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        ignite = (IgniteEx)startGrid();

        if (persist())
            ignite.active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ignite.context().ioStats().resetStats();
    }

    /**
     * Test statistics for TRANSACTIONAL cache.
     */
    public void testTransactonalCache() {
        cacheTest(TRANSACTIONAL_CACHE_NAME, RECORD_COUNT, RECORD_COUNT * 3, RECORD_COUNT * 2);
    }

    /**
     * Test statistics for MVCC cache.
     */
    public void testMvccCache() {
        cacheTest(MVCC_CACHE_NAME, RECORD_COUNT, RECORD_COUNT * 6, RECORD_COUNT * 3);
    }

    /**
     * Test statistics for ATOMIC cache.
     */
    public void testAtomicCache() {
        cacheTest(ATOMIC_CACHE_NAME, RECORD_COUNT, RECORD_COUNT * 2, RECORD_COUNT);
    }

    /**
     * Test statistics for three caches in the same time.
     */
    public void testForThreeCaches() {
        prepareData(RECORD_COUNT, ATOMIC_CACHE_NAME, TRANSACTIONAL_CACHE_NAME, MVCC_CACHE_NAME);

        GridIoStatManager ioStatMgr = ignite.context().ioStats();

        Set<String> statisticCacheNames = ioStatMgr.deriveStatNames(StatType.CACHE);

        Assert.assertEquals(ALL_CACHE_NAMES, statisticCacheNames);

        Stream.of(ATOMIC_CACHE_NAME, TRANSACTIONAL_CACHE_NAME, MVCC_CACHE_NAME).forEach((cacheName) -> {
            long logicalReads = ioStatMgr.logicalReads(StatType.CACHE, cacheName);

            Assert.assertTrue(logicalReads > RECORD_COUNT);

        });
    }

    /**
     * @param cacheName Name of cache.
     * @param rowCnt Number of row need to put into cache.
     * @param dataPageReads How many data page reads operation expected.
     * @param idxPageReadsCnt How many index page reads scan expected.
     */
    protected void cacheTest(String cacheName, int rowCnt, int dataPageReads, int idxPageReadsCnt) {
        prepareData(rowCnt, cacheName);

        GridIoStatManager ioStatMgr = ignite.context().ioStats();

        Set<String> statisticCacheNames = ioStatMgr.deriveStatNames(StatType.CACHE);

        Assert.assertEquals(ALL_CACHE_NAMES, statisticCacheNames);

        Assert.assertTrue(statisticCacheNames.contains(cacheName));

        long logicalReadsCache = ioStatMgr.logicalReads(StatType.CACHE, cacheName);

        Assert.assertEquals(dataPageReads, logicalReadsCache);

        long logicalReadsIdx = ioStatMgr.logicalReads(StatType.INDEX, cacheName, "PK");

        Assert.assertEquals(idxPageReadsCnt, logicalReadsIdx);

    }

    /**
     * Warm up and fill cache.
     *
     * @param cacheNames Names of caches to populate.
     * @param cnt Number of entries to put.
     */
    private void prepareData(int cnt, String... cacheNames) {
        //Need to initialize partition and data memory pages
        for (String cacheName : cacheNames) {

            IgniteCache cache = ignite.cache(cacheName);

            for (int i = 0; i < cnt; i++) {
                cache.put(i, i);

                cache.put(i, i); //Second invocation required to warm up MVCC cache to fill old versions chains.
            }
        }

        ignite.context().ioStats().resetStats();

        for (String cacheName : cacheNames) {

            IgniteCache cache = ignite.cache(cacheName);

            for (int i = 0; i < cnt; i++)
                cache.put(i, i);
        }
    }

}
