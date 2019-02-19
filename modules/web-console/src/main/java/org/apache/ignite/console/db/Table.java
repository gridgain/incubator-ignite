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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.console.dto.AbstractDto;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.toSet;

/**
 * Table for data objects.
 *
 * @param <T>
 */
public class Table<T extends AbstractDto> extends CacheHolder<UUID, T> {
    /** Unique indexes. */
    private List<UniqueIndexEx<T>> uniqueIndexes = new ArrayList<>();

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    public Table(Ignite ignite, String cacheName) {
        super(ignite, cacheName);
    }

    /**
     * @param keyGenerator Key generator.
     * @param msgGenerator Message generator.
     */
    public Table<T> addUniqueIndex(Function<T, Object> keyGenerator, Function<T, String> msgGenerator) {
        uniqueIndexes.add(new UniqueIndexEx<>(keyGenerator, msgGenerator));

        return this;
    }

    /** */
    @SuppressWarnings("unchecked")
    private IgniteCache<Object, UUID> indexCache() {
        IgniteCache cache = cache();

        return (IgniteCache<Object, UUID>)cache;
    }

    /**
     * @param id ID.
     * @return DTO.
     */
    @Nullable public T load(UUID id) {
        return cache().get(id);
    }

    /**
     * @param key Indexed key.
     * @return DTO.
     */
    @Nullable public T getByIndex(Object key) {
        UUID id = indexCache().get(key);

        return cache().get(id);
    }

    /**
     * @param ids IDs.
     * @return Collection of DTOs.
     */
    public Collection<T> loadAll(TreeSet<UUID> ids) {
        Map<UUID, T> res = cache.getAll(ids);

        return F.isEmpty(res) ? Collections.emptyList() : res.values();
    }

    /**
     * @param data DTO.
     * @return Saved DTO.
     */
    public T save(T data) throws IgniteException {
        for (UniqueIndexEx<T> idx : uniqueIndexes) {
            UUID prevId = indexCache().getAndPutIfAbsent(idx.key(data), data.id());

            if (prevId != null && !data.id().equals(prevId))
                throw new IgniteException(idx.message(data));
        }

        cache().put(data.id(), data);

        return data;
    }

    /**
     * @param items Map of DTOs.
     */
    public void saveAll(Map<UUID, T> items) throws IgniteException {
        if (uniqueIndexes.isEmpty())
            cache().putAll(items);

        for (T item : items.values())
            save(item);
    }

    /**
     * @param id ID.
     * @return Previous value.
     */
    @Nullable public T delete(UUID id) {
        T payload = cache().getAndRemove(id);

        indexCache().removeAll(uniqueIndexes.stream().map(idx -> idx.key(payload)).collect(toSet()));

        return payload;
    }

    /**
     * @param ids IDs.
     */
    public void deleteAll(Set<UUID> ids) {
        if (uniqueIndexes.isEmpty())
            cache().removeAll(ids);

        for (UUID item : ids)
            delete(item);
    }

    /**
     * Index for unique constraint.
     */
    private static class UniqueIndexEx<T> {
        /** */
        private final Function<T, Object> keyGenerator;

        /** */
        private final Function<T, String> msgGenerator;

        /**
         * Constructor.
         */
        UniqueIndexEx(Function<T, Object> keyGenerator, Function<T, String> msgGenerator) {
            this.keyGenerator = keyGenerator;
            this.msgGenerator = msgGenerator;
        }

        /**
         * @param data Payload.
         * @return Unique key.
         */
        public Object key(T data) {
            return keyGenerator.apply(data);
        }

        /**
         * @param data Data.
         */
        public String message(T data) {
            return msgGenerator.apply(data);
        }
    }
}
