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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

public class GridTransactionsRemoveTest extends GridCommonAbstractTest {
    /**
     *
     */
    private static final String CACHE_NAME = "test";

    /**
     *
     */
    private static final String CLIENT = "client";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        boolean isClient = igniteInstanceName.contains(CLIENT);

        cfg.setClientMode(isClient);

        if (!isClient) {
            cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                            .setInitialSize(100 * 1024L * 1024L)
                            .setMaxSize(500 * 1024L * 1024L)
                    )
            );

            CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(1);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /**
     *
     */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     *
     */
    @Test
    public void testTransactions() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().active(true);

        Ignite client = startGrid(CLIENT);

        assertTrue(client.configuration().isClientMode());

        IgniteCache<Integer, Integer> cache = client.getOrCreateCache(CACHE_NAME);

        for (int i = 0; i < 10; i++)
            cache.put(i, i);

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, SERIALIZABLE, 10000, 1)) {
            cache.put(100, 100);

            tx.commit();
        }

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, SERIALIZABLE, 10000, 1)) {
            cache.remove(1);

            tx.commit();
        }
    }
}
