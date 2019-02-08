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

package org.apache.ignite.console.db.store;

import java.util.Collection;
import java.util.UUID;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.core.CacheHolder;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Base class for data object store.
 */
@SuppressWarnings("Duplicates")
public abstract class AbstractStore<K, V> extends CacheHolder<K, V> {
    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    protected AbstractStore(Ignite ignite, String cacheName) {
        super(ignite, cacheName);
    }

    /**
     * @return Transaction.
     */
    protected Transaction txStart() {
        if (!ready())
            prepare();

        return ignite.transactions().txStart(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * @param rawData Data object.
     * @return ID or {@code null} if object has no ID.
     */
    @Nullable protected UUID getId(JsonObject rawData) {
        boolean hasId = rawData.containsKey("_id");

        return hasId ? UUID.fromString(rawData.getString("_id")) : null;
    }

    /**
     * Ensure that object has ID.
     * If not, ID will be generated and added to object.
     *
     * @param rawData Data object.
     * @return Object ID.
     */
    protected UUID ensureId(JsonObject rawData) {
        UUID id = getId(rawData);

        if (id == null) {
            id = UUID.randomUUID();

            rawData.put("_id", id.toString());
        }

        return id;
    }

    /**
     * @param user User.
     * @return User ID.
     */
    protected UUID getUserId(JsonObject user) {
        UUID userId = getId(user);

        if (userId == null)
            throw new IllegalStateException("User ID not found");

        return userId;
    }

    /**
     * @param user User.
     * @return List of objects.
     */
    public abstract Collection<V> list(JsonObject user);

    /**
     * Put object to store.
     *
     * @param user User.
     * @param rawData Object raw data.
     * @return Object instance.
     */
    public abstract V put(JsonObject user, JsonObject rawData);

    /**
     * Remove object from store.
     *
     * @param user User.
     * @param rawData Object raw data.
     * @return {@code true} if object was removed from store.
     */
    public abstract boolean remove(JsonObject user, JsonObject rawData);
}
