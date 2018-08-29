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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Basic test for local recovery with MVCC.
 */
@SuppressWarnings("unchecked")
public class CacheMvccLocalRecoveryTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimple() throws Exception {
        persistence = true;
        disableScheduledVacuum = true;

        ccfg = cacheConfiguration(PARTITIONED, FULL_SYNC, 0, 2);

        IgniteEx grid = startGrid(1);

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)grid.context().cache().context().database();

        grid.cluster().active(true);

        db.enableCheckpoints(false).get();

        IgniteCache cache = grid.getOrCreateCache(DEFAULT_CACHE_NAME);

        Map entries = new HashMap();

        for (int k = 0; k < 10; k++) {
            for (int i = 0; i < 10; i++) {
                cache.put(i, k);

                entries.put(i, k);
            }
        }

        for (int k = 0; k < 10; k++) {
            for (int i = 0; i < 10; i++) {
                cache.put(1, 1);

                entries.put(i, k);
            }
        }

        Map res1 = allVersions(cache);

        stopGrid(1, false);

        grid = startGrid(1);

        cache = grid.cache(DEFAULT_CACHE_NAME);

        Map res2 = allVersions(cache);

        assertVersionsEquals(res1, res2);
    }
}
