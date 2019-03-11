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

package org.apache.ignite.examples;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;

/**
 * Starts up an empty node with example compute configuration.
 */
public class ExampleNodeStartup {
    /**
     * Start up an empty node with example compute configuration.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If failed.
     */
    public static void main(String[] args) throws Exception {
        Ignite ignite = Cluster.ignite("Initiator", false);

        ignite.cluster().active(true);

        IgniteCache<Integer, UUID> cache = ignite.getOrCreateCache(Cluster.CACHE);

        UUID uid = UUID.randomUUID();

        for (int i = 0; i < 5_000; ++i)
            cache.put(i, uid);

        int i = 0;
        while (true) {
            if (ignite.cluster().active()) {
                if (cache == null)
                    cache = ignite.cache(Cluster.CACHE);

                int all = cache.size(CachePeekMode.ALL);
                int primary = cache.size(CachePeekMode.PRIMARY);
                int backup = cache.size(CachePeekMode.BACKUP);

                System.out.format("%s :: [%d] all=%d, primary=%d, backup=%d%n",
                    LocalDateTime.now(), i, all, primary, backup);

                i++;
            }

            TimeUnit.SECONDS.sleep(10);
        }
    }
}
