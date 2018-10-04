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

package org.apache.ignite.internal.processors.trace;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class GridCacheTxTracingSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "default";

    /** */
    private static final int GRID_CNT = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setBackups(1)
            .setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        cfg.setConsistentId(igniteInstanceName);

        if (getTestIgniteInstanceName(GRID_CNT - 1).equals(igniteInstanceName))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception if failed.
     */
    public void testTracing() throws Exception {
        Ignite client = ignite(GRID_CNT - 1);

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 0; i < 9; i++)
                cache.put(i, i);

            tx.commit();

            if (GRID_CNT == 4)
                System.out.println("A");
        }
    }
}
