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

package org.apache.ignite.examples.ml.util.benchmark.thinclient;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;

public class ServerMock {
    public static final String CACHE_NAME = "THIN_CLIENT_IMITATION_CACHE";
    public static final int COUNT_OF_PARTITIONS = 10;

    private static final int VALUE_OBJECT_SIZE_IN_BYTES = 1024 * 1024;
    private static final int COUNT_OF_ROWS = 500;

    public static void main(String ... args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {

            CacheConfiguration<Integer, byte[]> cacheConfiguration = new CacheConfiguration<>();
            cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, COUNT_OF_PARTITIONS));
            cacheConfiguration.setName(CACHE_NAME);

            IgniteCache<Integer, byte[]> cache = ignite.getOrCreateCache(cacheConfiguration);

            for (int i = 0; i < COUNT_OF_ROWS; i++) {
                byte[] val = new byte[VALUE_OBJECT_SIZE_IN_BYTES];
                Arrays.fill(val, (byte) i);
                cache.put(i, val);
            }

            System.out.println("Cache is ready!");

            Thread.currentThread().join();
        }
    }

}
