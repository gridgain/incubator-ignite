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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Vacuum test.
 */
public class CacheMvccVacuumTest extends CacheMvccAbstractTest {

    /**
     * @throws Exception If failed.
     */
    public void testBasicVacuumCleanup() throws Exception {
        Ignite node = startGrids(1);

        final IgniteCache<Object, Object> cache = node.createCache(cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 64));

        final int KEYS = 100;

        final int ITERATIONS = 100;

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            for (int i = 0; i < KEYS; i++) {
                if (iteration == 0)
                    cache.put(i, 0);
                else if (rnd.nextBoolean())
                    cache.remove(i);
                else
                    cache.put(i, i * iteration + 1);
            }
        }

        // TODO run vacuum manually. Plus semaphore.
        waitVacuumFinished();

        checkDataState();
    }

    /**
     * Check versions presence in index tree.
     * @param srv Node.
     * @param mvccEnabled MVCC flag.
     * @param afterRebuild Whether index rebuild has occurred.
     * @throws IgniteCheckedException if failed.
     */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
    private void checkDataState() throws IgniteCheckedException {
        for (Ignite node : G.allGrids()) {
            for (String cacheName: node.cacheNames()) {
                IgniteInternalCache cache = ((IgniteEx)node).cachex(cacheName);

                assertNotNull(cache);

                for (IgniteCacheOffheapManager.CacheDataStore store : cache.context().offheap().cacheDataStores()) {
                    GridCursor<? extends CacheDataRow> cur = store.cursor();

                    while (cur.next()) {
                        CacheDataRow row = cur.get();

                        int key  = row.key().value(cache.context().cacheObjectContext(), false);

                        List<T2<Object, MvccVersion>> vers = store.mvccFindAllVersions(cache.context(), row.key());

                        System.out.println("vers=" + vers);

                        assertEquals(1, vers.size());
                    }
                }
            }
        }
    }

    /**
     * @throws InterruptedException If failed.
     */
    private void waitVacuumFinished() throws InterruptedException {
        Ignite node = grid(0);

        final MvccProcessor crd = ((IgniteKernal)node).context().cache().context().coordinators();

        Object vacuumEndNotifier = GridTestUtils.getFieldValue(crd, "vacuumEndNotifier");

        doSleep(node.configuration().getMvccVacuumTimeInterval());

        synchronized (vacuumEndNotifier) {
            System.out.println("FIRST!!!!");
            vacuumEndNotifier.wait();
        }

        synchronized (vacuumEndNotifier) {

            System.out.println("SECOND!!!!");
            vacuumEndNotifier.wait();
        }
    }
}
