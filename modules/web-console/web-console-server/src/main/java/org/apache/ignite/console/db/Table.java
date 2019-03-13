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
 * @param <T> Type of DTO.
 */
public class Table<T extends AbstractDto> extends CacheHolder<UUID, T> {
    /** Unique indexes. */
    private final List<UniqueIndex<T>> uniqueIndexes = new ArrayList<>();

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
        uniqueIndexes.add(new UniqueIndex<>(keyGenerator, msgGenerator));

        return this;
    }

    /** */
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

        if (id == null)
            return null;

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
     * @param val Value.
     */
    private void putToUniqueIndexes(T val) {
        for (UniqueIndex<T> idx : uniqueIndexes) {
            UUID prevId = indexCache().getAndPutIfAbsent(idx.key(val), val.id());

            if (prevId != null && !val.id().equals(prevId))
                throw new IgniteException(idx.message(val));
        }
    }

    /**
     * @param val DTO.
     * @return Saved DTO.
     */
    public T save(T val) throws IgniteException {
        putToUniqueIndexes(val);

        cache().put(val.id(), val);

        return val;
    }

    /**
     * @param values Map of DTOs.
     */
    public void saveAll(Map<UUID, T> values) throws IgniteException {
        for (T item : values.values())
            putToUniqueIndexes(item);

        cache().putAll(values);
    }

    /**
     * @param id ID.
     * @return Previous value.
     */
    @Nullable public T delete(UUID id) {
        T val = cache().getAndRemove(id);

        indexCache().removeAll(uniqueIndexes.stream().map(idx -> idx.key(val)).collect(toSet()));

        return val;
    }

    /**
     * @param ids IDs.
     */
    public void deleteAll(Set<UUID> ids) {
        Set<Object> idxIds = ids.stream()
            .map(cache()::getAndRemove)
            .flatMap((payload) -> uniqueIndexes.stream().map(idx -> idx.key(payload)))
            .collect(toSet());

        indexCache().removeAll(idxIds);
    }

    /**
     * Index for unique constraint.
     */
    private static class UniqueIndex<T> {
        /** */
        private final Function<T, Object> keyGenerator;

        /** */
        private final Function<T, String> msgGenerator;

        /**
         * Constructor.
         */
        UniqueIndex(Function<T, Object> keyGenerator, Function<T, String> msgGenerator) {
            this.keyGenerator = keyGenerator;
            this.msgGenerator = msgGenerator;
        }

        /**
         * @param val Value.
         * @return Unique key.
         */
        public Object key(T val) {
            return keyGenerator.apply(val);
        }

        /**
         * @param val Value.
         */
        public String message(T val) {
            return msgGenerator.apply(val);
        }
    }
}
