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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.dto.DataObject;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Table for data objects.
 *
 * @param <V>
 */
public class Table<V extends DataObject> extends CacheHolder<UUID, V> {
    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    public Table(Ignite ignite, String cacheName) {
        super(ignite, cacheName);
    }

    /**
     * @param id
     * @return
     */
    @Nullable public V load(UUID id) {
        return cache.get(id);
    }

    /**
     * @param ids
     * @return
     */
    public Collection<V> loadAll(TreeSet<UUID> ids) {
        Map<UUID, V> res = cache.getAll(ids);

        return F.isEmpty(res) ? Collections.emptyList() : res.values();
    }

    /**
     * @param data
     * @return
     */
    public V save(V data) {
        cache.put(data.id(), data);

        return data;
    }

    /**
     *
     * @param items
     */
    public void saveAll(Map<UUID, V> items) {
        cache.putAll(items);
    }

    /**
     *
     * @param id
     * @return
     */
    @Nullable public V delete(UUID id) {
        return cache.getAndRemove(id);
    }

    /**
     *
     * @param keys
     */
    public void deleteAll(TreeSet<UUID> keys) {
        cache.removeAll(keys);
    }
}
