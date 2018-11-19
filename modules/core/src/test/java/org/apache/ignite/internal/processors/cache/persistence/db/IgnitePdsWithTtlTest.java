/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_ENABLED;

/**
 * Test TTL worker with persistence enabled
 */
public class IgnitePdsWithTtlTest extends GridCommonAbstractTest {
    /** */
    public static final String EXPIRABLE_CACHE = "expirableCache";

    /** */
    public static final String NOT_EXPIRABLE_CACHE_NAME = "notExpirableCache";

    /** */
    private static final int EXPIRATION_TIMEOUT = 10;

    /** */
    public static final Duration EXPIRATION_DURATION = new Duration(TimeUnit.SECONDS, EXPIRATION_TIMEOUT);

    /** */
    public static final int ENTRIES = 40_000;


    /** */
    protected int lastId = 0;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_BASELINE_AUTO_ADJUST_ENABLED, "false");

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(IGNITE_BASELINE_AUTO_ADJUST_ENABLED);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EXPIRATION);

        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        //protection if test failed to finish, e.g. by error
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        final CacheConfiguration expCacheCfg = new CacheConfiguration();
        expCacheCfg.setName(EXPIRABLE_CACHE);
        expCacheCfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        expCacheCfg.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(EXPIRATION_DURATION));
        expCacheCfg.setEagerTtl(true);
        expCacheCfg.setGroupName("group1");
        expCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        expCacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        final CacheConfiguration notExpCacheCfg = new CacheConfiguration();
        notExpCacheCfg.setName(NOT_EXPIRABLE_CACHE_NAME);
        notExpCacheCfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        notExpCacheCfg.setEagerTtl(true);
        notExpCacheCfg.setGroupName("group2");
        notExpCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        notExpCacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);


        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(192L * 1024 * 1024)
                        .setPersistenceEnabled(true)
                ).setWalMode(WALMode.LOG_ONLY));

        cfg.setCacheConfiguration(expCacheCfg, notExpCacheCfg);

        return cfg;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testTtlIsApplied() throws Exception {
        loadAndWaitForCleanup(false, false);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testTtlIsAppliedAfterRestartWithCheckpoint() throws Exception {
        loadAndWaitForCleanup(true, true);
    }

    /**
     * @throws Exception if failed.
     */
    public void testTtlIsAppliedAfterRestartWithoutCheckpoint() throws Exception {
        loadAndWaitForCleanup(true, false);
    }

    /**
     * @throws Exception if failed.
     */
    private void loadAndWaitForCleanup(boolean restartGrid, boolean makeCheckpoint) throws Exception {
        IgniteEx srv = startGrid(0);
        srv.cluster().active(true);

        if (!makeCheckpoint)
            disableCheckpoints();

        fillCaches(srv);

        if (makeCheckpoint)
            forceCheckpoint(srv);

        if (restartGrid) {
            stopGrid(0);
            srv = startGrid(0);
            srv.cluster().active(true);

            fillCaches(srv);
        }

        waitedAndCheckExpired(srv);

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRebalancingWithTtlExpirable() throws Exception {
        IgniteEx srv = startGrid(0);
        srv.cluster().active(true);

        fillCaches(srv);

        srv = startGrid(1);

        //causes rebalancing start
        srv.cluster().setBaselineTopology(srv.cluster().topologyVersion());

        waitedAndCheckExpired(srv);

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testStartStopAfterRebalanceWithTtlExpirable() throws Exception {
        try {
            IgniteEx srv = startGrid(0);
            startGrid(1);
            srv.cluster().active(true);

            fillCaches(srv);

            srv = startGrid(2);

            IgniteCache<Integer, byte[]> expCache = srv.cache(NOT_EXPIRABLE_CACHE_NAME);
            IgniteCache<Integer, byte[]> notExpCache = srv.cache(EXPIRABLE_CACHE);

            //causes rebalancing start
            srv.cluster().setBaselineTopology(srv.cluster().topologyVersion());

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return Boolean.TRUE.equals(expCache.rebalance().get()) && expCache.localSizeLong(CachePeekMode.ALL) > 0
                        && Boolean.TRUE.equals(notExpCache.rebalance().get()) && notExpCache.localSizeLong(CachePeekMode.ALL) > 0;
                }
            }, 20_000);

            //check if pds is consistent
            stopGrid(0);
            startGrid(0);

            stopGrid(1);
            startGrid(1);

            waitedAndCheckExpired(srv);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private void fillCache(IgniteCache<Object, Object> cache) {
        cache.putAll(new TreeMap<Integer, byte[]>() {{
            for (int i = 0; i < ENTRIES; i++)
                put(++lastId, new byte[1024]);
        }});
    }

    /** */
    private void fillCaches(Ignite ignite) {
        fillCache(ignite.cache(EXPIRABLE_CACHE));

        IgniteCache<Object, Object> cache1 = ignite.cache(NOT_EXPIRABLE_CACHE_NAME);

        fillCache(cache1.withExpiryPolicy(CreatedExpiryPolicy.factoryOf(EXPIRATION_DURATION).create()));
        fillCache(cache1.withExpiryPolicy(CreatedExpiryPolicy.factoryOf(EXPIRATION_DURATION).create()));
        fillCache(cache1.withExpiryPolicy(CreatedExpiryPolicy.factoryOf(EXPIRATION_DURATION).create()));
        fillCache(cache1.withExpiryPolicy(CreatedExpiryPolicy.factoryOf(EXPIRATION_DURATION).create()));
    }

    /** */
    private void waitedAndCheckExpired(Ignite ignite) throws IgniteInterruptedCheckedException {
        IgniteCache<Object, Object> expCache = ignite.cache(EXPIRABLE_CACHE);
        IgniteCache<Object, Object> notExpCache = ignite.cache(NOT_EXPIRABLE_CACHE_NAME);

        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                System.out.println("expCache.size():"+expCache.size());
                System.out.println("notExpCache.size():"+notExpCache.size());
                return expCache.size() == 0 && notExpCache.size() == 0;
            }
        }, TimeUnit.SECONDS.toMillis(EXPIRATION_TIMEOUT) + 2*60000);

        pringStatistics((IgniteCacheProxy)expCache, "After timeout");
        pringStatistics((IgniteCacheProxy)notExpCache, "After timeout");

        assertEquals("Cache size should equals to 0", 0, expCache.size());
        assertEquals("Cache size should equals to 0", 0, notExpCache.size());
    }

    /** */
    private void pringStatistics(IgniteCacheProxy cache, String msg) {
        System.out.println(msg + " {{");
        cache.context().printMemoryStats();
        System.out.println("}} " + msg);
    }
}
