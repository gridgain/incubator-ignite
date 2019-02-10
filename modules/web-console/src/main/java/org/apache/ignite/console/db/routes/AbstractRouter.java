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

package org.apache.ignite.console.db.routes;

import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.core.CacheHolder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.apache.ignite.console.common.Utils.errorMessage;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Base class for routers.
 */
public abstract class AbstractRouter<K, V> extends CacheHolder<K, V> {
    /** */
    private static final List<CharSequence> HTTP_CACHE_CONTROL = Arrays.asList(
        HttpHeaderValues.NO_CACHE,
        HttpHeaderValues.NO_STORE,
        HttpHeaderValues.MUST_REVALIDATE);

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    protected AbstractRouter(Ignite ignite, String cacheName) {
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
     * @param rawData Data object.
     * @param key Key with IDs.
     * @return Set of IDs
     */
    protected TreeSet<UUID> getIds(JsonObject rawData, String key) {
        TreeSet<UUID> ids = new TreeSet<>();

        if (rawData.containsKey(key)) {
            rawData
                .getJsonArray(key)
                .stream()
                .map(item -> UUID.fromString(item.toString()))
                .sequential()
                .collect(Collectors.toCollection(() -> ids));
        }

        return ids;
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
     * @param ctx Context.
     * @param paramName Parameter name.
     * @return Parameter value
     */
    protected String getParam(RoutingContext ctx, String paramName) {
        String param = ctx.request().getParam(paramName);

        if (F.isEmpty(param))
            throw new IllegalStateException("Parameter not found: " + paramName);

        return param;
    }

    /**
     * @param ctx Context.
     * @param status Status to send.
     */
    private void sendStatus(RoutingContext ctx, int status) {
        ctx.response().setStatusCode(status).end();
    }

    /**
     * @param ctx Context.
     * @param msg Error message to send.
     * @param e Error to send.
     */
    protected void sendError(RoutingContext ctx, String msg, Throwable e) {
        ignite.log().error(msg, e);

        ctx
            .response()
            .setStatusCode(HTTP_INTERNAL_ERROR)
            .end(msg + ": " + errorMessage(e));
    }

    /**
     * @param ctx Context.
     * @param data Data to send.
     */
    protected void sendResult(RoutingContext ctx, Buffer data) {
        ctx
            .response()
            .putHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
            .putHeader(HttpHeaderNames.CACHE_CONTROL, HTTP_CACHE_CONTROL)
            .putHeader(HttpHeaderNames.PRAGMA, HttpHeaderValues.NO_CACHE)
            .putHeader(HttpHeaderNames.EXPIRES, "0")
            .setStatusCode(HTTP_OK)
            .end(data);
    }

    /**
     * @param ctx Context.
     * @param data Data to send.
     */
    protected void sendResult(RoutingContext ctx, JsonObject data) {
        sendResult(ctx, data.toBuffer());
    }

    /**
     * Get the authenticated user (if any).
     * If user not found, send {@link HttpURLConnection#HTTP_UNAUTHORIZED}.
     *
     * @param ctx Context
     * @return User or {@code null} if the current user is not authenticated.
     */
    @Nullable protected User checkUser(RoutingContext ctx) {
        User user = ctx.user();

        if (user == null)
            sendStatus(ctx, HTTP_UNAUTHORIZED);

        return user;
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
