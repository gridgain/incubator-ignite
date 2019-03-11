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

package org.apache.ignite.console.services;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static org.apache.ignite.console.common.Utils.errorMessage;

/**
 * Base class for routers.
 */
public abstract class AbstractService implements AutoCloseable {
    /** */
    protected final Ignite ignite;

    /** */
    private final Set<MessageConsumer<?>> consumers = new HashSet<>();

    /**
     * @param ignite Ignite.
     */
    protected AbstractService(Ignite ignite) {
        this.ignite = ignite;
    }

    /**
     * Initialize event bus.
     */
    public abstract AbstractService install(Vertx vertx);

    /** {@inheritDoc} */
    @Override public void close() {
        consumers.forEach(MessageConsumer::unregister);

        ignite.log().info("Service stopped: " + getClass().getSimpleName());
    }

    /**
     * @param addr Address.
     * @param supplier Data supplier.
     */
    protected <R> void addConsumer(Vertx vertx, String addr, Function<JsonObject, R> supplier) {
        MessageConsumer<?> consumer = vertx
            .eventBus()
            .consumer(addr, (Message<JsonObject> msg) -> {
                JsonObject params = msg.body();

                ignite.log().info("Received message [from=" + msg.address() + ", params=" + params + "]");

                vertx.executeBlocking(
                    fut -> {
                        try {
                            fut.complete(supplier.apply(params));
                        }
                        catch (Throwable e) {
                            fut.fail(e);
                        }
                    },
                    asyncRes -> {
                        if (asyncRes.succeeded())
                            msg.reply(asyncRes.result());
                        else
                            msg.fail(HTTP_INTERNAL_ERROR, errorMessage(asyncRes.cause()));
                    }
                );

            });

        consumers.add(consumer);
    }

    /**
     *
     * @param params Params in JSON format.
     * @param key Property name.
     * @return JSON for specified property.
     * @throws IllegalStateException If property not found.
     */
    protected JsonObject getProperty(JsonObject params, String key) {
        JsonObject prop = params.getJsonObject(key);

        if (prop == null)
            throw new IllegalStateException("Message does not contain property: " + key);

        return prop;
    }

    /**
     * @param json JSON object.
     * @return ID or {@code null} if object has no ID.
     */
    @Nullable protected UUID getId(JsonObject json) {
        String s = json.getString("_id");

        return F.isEmpty(s) ? null : UUID.fromString(s);
    }

    /**
     * @param params Params in JSON format.
     * @return User ID.
     * @throws IllegalStateException If user ID not found.
     */
    protected UUID getUserId(JsonObject params) {
        JsonObject user = getProperty(params, "user");

        UUID userId = getId(user);

        if (userId == null)
            throw new IllegalStateException("User ID not found");

        return userId;
    }

    /**
     * @param rows Number of rows.
     * @return JSON with number of affected rows.
     */
    protected JsonObject rowsAffected(int rows) {
        return new JsonObject()
            .put("rowsAffected", rows);
    }
}
