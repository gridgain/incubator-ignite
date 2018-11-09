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

package org.apache.ignite.yardstick.cache;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs getAll operations.
 */
public class IgniteGetAllBenchmark extends IgniteCacheAbstractBenchmark<Long, byte[]> {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        Set<Long> keys = U.newHashSet(args.batch());

        while (keys.size() < args.batch()) {
            int key = nextRandom(args.range());

            keys.add(new Long(key));
        }

        IgniteCache<Long, byte[]> cache = cacheForOperation();

        cache.getAll(keys);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected void loadCacheData(String cacheName) {
        loadByteArrays(cacheName, args.preloadAmount());
    }
    /** {@inheritDoc} */
    @Override protected IgniteCache<Long, byte[]> cache() {
        return ignite().cache("atomic");
    }

    /**
     * @param cacheName Cache name.
     * @param cnt Number of entries to load.
     */
    protected final void loadByteArrays(String cacheName, int cnt) {
        try (IgniteDataStreamer<Long, byte[]> dataLdr = ignite().dataStreamer(cacheName)) {
            for (long i = 0; i < cnt; i++) {

                byte[] arr = new byte[450];

                Arrays.fill(arr,(byte)8);

                dataLdr.addData(i, arr);

                if (i % 100000 == 0) {
                    if (Thread.currentThread().isInterrupted())
                        break;

                    println("Loaded entries [cache=" + cacheName + ", cnt=" + i + ']');
                }
            }
        }

        println("Load entries done [cache=" + cacheName + ", cnt=" + cnt + ']');
    }
}
