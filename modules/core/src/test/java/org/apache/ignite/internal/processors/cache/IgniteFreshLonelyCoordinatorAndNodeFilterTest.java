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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Just a reproducer, need to rename class and rewrite everything.
 */
public class IgniteFreshLonelyCoordinatorAndNodeFilterTest extends GridCommonAbstractTest {
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(false));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setInitialSize(10500000)
                        .setMaxSize(6659883008L)
                        .setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU)
                        .setPersistenceEnabled(false)
                )
                .setDataRegionConfigurations(
                    new DataRegionConfiguration()
                        .setName("journeys-handled")
                        .setInitialSize(10500000)
                        .setMaxSize(6659883008L)
                        .setPageEvictionMode(DataPageEvictionMode.RANDOM_2_LRU)
                        .setPersistenceEnabled(false)
                )
                .setDataRegionConfigurations(
                    new DataRegionConfiguration()
                        .setName("no-evict")
                        .setInitialSize(10500000)
                        .setMaxSize(6659883008L)
                        .setPageEvictionMode(DataPageEvictionMode.DISABLED)
                        .setPersistenceEnabled(false)
                )
        );

        return cfg;
    }

    /**
     *
     */
    public void testFreshCoordinatorStartsSuccessfully() throws Exception {
        Ignite ig0 = startGrid("node01");

        ig0.getOrCreateCache(new CacheConfiguration<>("cache1")
            .setAffinity(new RendezvousAffinityFunction(false, 64))
            .setNodeFilter((IgnitePredicate<ClusterNode>)node -> node.consistentId().toString().contains("node01"))
        );

        fillCache(ig0);

        IgniteInternalFuture<Ignite> fut1 = GridTestUtils.runAsync(() -> startGrid("node02"), "nodeStarter");

        //tweak this delay if test doesn't fail in your environment
        Thread.sleep(1250);

        GridTestUtils.runAsync(() -> {
            stopGrid("node01");
        }, "nodeStopper");

        fut1.get();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    private void fillCache(Ignite ig) {
        try (IgniteDataStreamer cache1Ds = ig.dataStreamer("cache1")) {
            for (int i = 0; i < 10_000; i++)
                cache1Ds.addData(i, "value_" + i);
        }
    }
}
