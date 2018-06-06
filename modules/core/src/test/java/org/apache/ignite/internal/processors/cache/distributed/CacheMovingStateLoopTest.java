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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheMovingStateLoopTest extends GridCommonAbstractTest {
    /** */
    public static final int PARTS = 1024;

    private boolean enableRecording;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode("client".equals(igniteInstanceName));

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS).setPartitions(64));

        ccfg.setOnheapCacheEnabled(false);

        ccfg.setBackups(1);

        ccfg.setRebalanceBatchSize(100000);

        TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(spi);

        if (enableRecording)
            spi.record(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message message) {
                    if (message instanceof GridDhtPartitionDemandMessage) {
                        GridDhtPartitionDemandMessage dm = (GridDhtPartitionDemandMessage)message;

                        return dm.groupId() == CU.cacheId(DEFAULT_CACHE_NAME) && node.consistentId().equals("node1");
                    }

                    return false;
                }
            });

        cfg.setCacheConfiguration(ccfg);

        cfg.setActiveOnStart(false);

        cfg.setConsistentId(igniteInstanceName);

        long sz = 100 * 1024 * 1024;

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setPageSize(1024)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true).setInitialSize(sz).setMaxSize(sz))
            .setWalMode(WALMode.LOG_ONLY).setCheckpointFrequency(24L * 60 * 60 * 1000);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName(int idx) {
        return "node" + idx;
    }

    /**
     *
     */
    public void testMovingStateLoopOnRebalance() throws Exception {
        try {
            IgniteEx g0 = startGrid(0);

            IgniteEx g1 = startGrid(1);

            g0.cluster().active(true);

            awaitPartitionMapExchange();

            List<Integer> partIds = evictingPartitionsAfterJoin(g0, g0.cache(DEFAULT_CACHE_NAME), 2);

            Integer firstEvicting = partIds.get(0);

            int toEvictPart = partIds.get(1);

            putKeys(g0, toEvictPart, 0, 10_000);

            GridDhtPartitionTopology top0 = dht(g0.cache(DEFAULT_CACHE_NAME)).topology();
            GridDhtPartitionTopology top1 = dht(g1.cache(DEFAULT_CACHE_NAME)).topology();

            GridDhtLocalPartition part = top0.localPartition(toEvictPart);

            assertNotNull(part);

            IgniteEx g2 = startGrid(2);

            g0.cluster().setBaselineTopology(3);

            awaitPartitionMapExchange();

            List<GridDhtLocalPartition> parts0 = new ArrayList<>(top0.localPartitions());
            List<GridDhtLocalPartition> parts1 = top1.localPartitions();

            parts0.retainAll(parts1);

            GridDhtLocalPartition primOn0 = null;

            for (GridDhtLocalPartition partition : parts0) {
                if (partition.primary(g0.context().cache().context().exchange().readyAffinityVersion())) {
                    primOn0 = partition;

                    break;
                }
            }

            assertNotNull(primOn0);

            assertFalse(toEvictPart == primOn0.id());

            putKeys(g0, primOn0.id(), 0, 100);

            // find partition where g0 prim owner, g1 - backup owner
            // put data to the partition.
            // stop g0
            // put more data to partition
            // after restart partition should be in moving state

            stopGrid(0);

            awaitPartitionMapExchange();

            putKeys(g1, primOn0.id(), 100, 10000);

            enableRecording = true;

            g0 = startGrid(0);

            awaitPartitionMapExchange();

            TestRecordingCommunicationSpi spi = (TestRecordingCommunicationSpi)g0.configuration().getCommunicationSpi();

            List<Object> objects = spi.recordedMessages(true);

            //printPartitionState(g0.cache(DEFAULT_CACHE_NAME));

            int size = g0.cache(DEFAULT_CACHE_NAME).size();

            assertEquals(20000, size);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param g0 G 0.
     * @param part Partition.
     * @param skip Skip.
     * @param total Total.
     */
    private void putKeys(IgniteEx g0, int part, int skip, int total) {
        int k = 0;
        int c = 0;

        try(IgniteDataStreamer ds = g0.dataStreamer(DEFAULT_CACHE_NAME)) {
            while (c < total) {
                if (g0.affinity(DEFAULT_CACHE_NAME).partition(k) == part) {
                    if (c >= skip)
                        ds.addData(k, k);

                    c++;
                }

                k++;
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        U.IGNITE_TEST_FEATURES_ENABLED = true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();

        U.IGNITE_TEST_FEATURES_ENABLED = false;
    }
}
