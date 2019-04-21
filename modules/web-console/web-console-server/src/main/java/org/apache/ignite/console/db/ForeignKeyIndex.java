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

import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.springframework.stereotype.Service;

/**
 * Service to check object ownership.
 */
@Service
public class ForeignKeyIndex extends CacheHolder<UUID, UUID> {
    /** */
    private static final String ERR_DATA_ACCESS_VIOLATION = "Data access violation";

    /**
     * @param ignite Ignite.
     */
    public ForeignKeyIndex(Ignite ignite, String idxName) {
        super(ignite, idxName);
    }

    /**
     * @param fk Foreign key.
     * @param key Key.
     * @return Previous foreign key value or {@code null}.
     */
    private UUID validate0(UUID fk, UUID key) {
        UUID prevFk = cache.get(key);

        if (prevFk != null && !prevFk.equals(fk))
            throw new IllegalStateException(ERR_DATA_ACCESS_VIOLATION);

        return prevFk;
    }

    /**
     * Add foreign key.
     *
     * @param fk Foreign key.
     * @param key Key.
     */
    public void add(UUID fk, UUID key) {
        UUID prevKk = validate0(fk, key);

        if (prevKk == null)
            cache.put(key, fk);
    }

    /**
     * Add foreign keys.
     *
     * @param fk Foreign key.
     * @param keys Keys.
     */
    public void addAll(UUID fk, Set<UUID> keys) {
        keys.forEach(key -> add(fk, key));
    }

    /**
     * Validate foreign key.
     *
     * @param fk Foreign key.
     * @param key Key.
     */
    public void validate(UUID fk, UUID key) {
        validate0(fk, key);
    }

    /**
     * Validate foreign keys.
     *
     * @param fk Foreign key.
     * @param keys Keys.
     */
    public void validateAll(UUID fk, TreeSet<UUID> keys) {
        boolean valid = cache
            .getAll(keys)
            .values()
            .stream()
            .allMatch(fk::equals);

        if (!valid)
            throw new IllegalStateException(ERR_DATA_ACCESS_VIOLATION);
    }

    /**
     * Delete foreign key.
     *
     * @param fk Foreign key.
     * @param key Key.
     */
    public void delete(UUID fk, UUID key) {
        validate(fk, key);

        cache.remove(key);
    }

    /**
     * Delete foreign keys.
     *
     * @param fk Foreign key.
     * @param keys Keys.
     */
    public void deleteAll(UUID fk, TreeSet<UUID> keys) {
        validateAll(fk, keys);

        cache.removeAll(keys);
    }
}
