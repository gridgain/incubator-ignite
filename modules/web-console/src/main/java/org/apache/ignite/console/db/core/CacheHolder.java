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

package org.apache.ignite.console.db.core;

import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;

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
    public CacheHolder(Ignite ignite, String cacheName) {
        this.ignite = ignite;
        this.cacheName = cacheName;
    }

    /**
     * Prepare cache.
     */
    public void prepare() {
        if (cache == null) {
            CacheConfiguration<K, V> ccfg = new CacheConfiguration<>(cacheName);
            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setCacheMode(REPLICATED);

            cache = ignite.getOrCreateCache(ccfg);
        }
    }

    /**
     * Get values for specified keys.
     *
     * @param keys Keys to get.
     * @return Map with entries.
     */
    public Map<K, V> getAll(Set<? extends K> keys) {
        return cache.getAll(keys);
    }

    /**
     * Removes entries for the specified keys.
     *
     * @param keys Keys to remove.
     */
    public void removeAll(Set<? extends K> keys) {
        cache.removeAll(keys);
    }

    /**
     * Put value to cache.
     *
     * @param key Key.
     * @param val Value.
     */
    public void put(K key, V val) {
        cache.put(key, val);
    }

    /**
     * Get value from cache.
     *
     * @param key key.
     * @return Value.
     */
    public V get(K key) {
        return cache.get(key);
    }

    /**
     * Remove value from cache.
     *
     * @param key key.
     * @return {@code false} if there was no matching key.
     */
    public boolean remove(K key) {
        return cache.remove(key);
    }

    /**
     * Remove value from cache.
     *
     * @param key Key.
     * @return Previous value.
     */
    public V getAndRemove(K key) {
        return cache.getAndRemove(key);
    }
}
