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

package org.apache.ignite.console.db;

import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Helper class to support unique indexes.
 */
public class UniqueIndex {
    /** */
    private final IgniteCache<String, Boolean> cache;

    /**
     * Constructor.
     *
     * @param ignite Ignite.
     * @param idxName Index name.
     */
    public UniqueIndex(Ignite ignite, String idxName) {
        CacheConfiguration<String, Boolean> ccfg = new CacheConfiguration<>(idxName);
        ccfg.setCacheMode(REPLICATED);
        ccfg.setAtomicityMode(TRANSACTIONAL);

        cache = ignite.getOrCreateCache(ccfg);
    }

    /**
     * @param key
     */
    public void put(String key) {
        cache.put(key, Boolean.TRUE);
    }

    /**
     *
     * @param key
     */
    public void remove(String key) {
        cache.remove(key);
    }

    public boolean contains(String key) {
        return cache.containsKey(key);
    }
}
