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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Cache holder.
 *
 * @param <K>
 * @param <V>
 */
public class CacheHolder<K, V> {
    /** */
    protected final Ignite ignite;

    /** */
    protected final String cacheName;

    /** */
    protected IgniteCache<K, V> cache;

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    protected CacheHolder(Ignite ignite, String cacheName) {
        this.ignite = ignite;
        this.cacheName = cacheName;

        CacheConfiguration<K, V> ccfg = new CacheConfiguration<K, V>(cacheName)
            .setAtomicityMode(TRANSACTIONAL)
            .setCacheMode(REPLICATED);

        cache = ignite.getOrCreateCache(ccfg);
    }

    /**
     * @return Underlying cache.
     */
    public IgniteCache cache() {
        return cache;
    }
}
