/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class AffinityChangeTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "myCache";

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                            .setInitialSize(100 * 1024L * 1024L)
                            .setMaxSize(1024 * 1024L * 1024L)
                            .setMetricsEnabled(true)
                    )
            )
            .setCacheConfiguration(
                new CacheConfiguration()
                    .setName(CACHE_NAME)
                    .setBackups(1)
            );
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    @Test
    public void test() throws Exception {
        Ignite ignite = startGrids(3);

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = grid(1).getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        stopGrid(0);

        awaitPartitionMapExchange();

        grid(1).cluster().setBaselineTopology(Arrays.asList(grid(1).localNode(), grid(2).localNode()));

        for (int i = 100; i < 200; i++)
            cache.put(i, i);
    }
}
