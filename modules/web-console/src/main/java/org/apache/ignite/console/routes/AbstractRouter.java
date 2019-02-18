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

package org.apache.ignite.console.routes;

import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.DataObject;
import org.apache.ignite.console.dto.JsonBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.apache.ignite.console.common.Utils.errorMessage;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Base class for routers.
 */
public abstract class AbstractRouter implements RestApiRouter {
    /** */
    protected final Ignite ignite;

    /** */
    private volatile boolean ready;

    /** */
    private static final List<CharSequence> HTTP_CACHE_CONTROL = Arrays.asList(
        HttpHeaderValues.NO_CACHE,
        HttpHeaderValues.NO_STORE,
        HttpHeaderValues.MUST_REVALIDATE);

    /**
     * @param ignite Ignite.
     */
    protected AbstractRouter(Ignite ignite) {
        this.ignite = ignite;
    }

    /**
     * Initialize caches.
     */
    protected abstract void initializeCaches();

    /**
     * Start transaction.
     *
     * @return Transaction.
     */
    protected Transaction txStart() {
        if (!ready) {
            initializeCaches();

            ready = true;
        }

        return ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);
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
     * Ensure that object has ID.
     * If not, ID will be generated and added to object.
     *
     * @param json JSON object.
     * @return Object ID.
     */
    protected UUID ensureId(JsonObject json) {
        UUID id = getId(json);

        if (id == null) {
            id = UUID.randomUUID();

            json.put("_id", id.toString());
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
    protected String requestParam(RoutingContext ctx, String paramName) {
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
     * @param ctx Context.
     * @param data Data to send.
     */
    protected void sendResult(RoutingContext ctx, JsonArray data) {
        sendResult(ctx, data.toBuffer());
    }

    /**
     * @param ctx Context.
     * @param data Data to send.
     */
    protected void sendResult(RoutingContext ctx, String data) {
        sendResult(ctx, Buffer.buffer(data));
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

    /**
     * Ensure that transaction was started explicitly.
     */
    protected void ensureTx() {
        if (ignite.transactions().tx() == null)
            throw new IllegalStateException("Transaction was not started explicitly");
    }

    /**
     * Load short list of DTOs.
     *
     * @param ctx Context.
     * @param tbl Table with DTOs.
     * @param idx Index with DTOs IDs.
     * @param errMsg Message to show in case of error.
     */
    protected void loadList(RoutingContext ctx, Table<? extends DataObject> tbl, OneToManyIndex idx, String errMsg) {
        try {
            UUID clusterId = UUID.fromString(requestParam(ctx, "id"));

            try (Transaction tx = txStart()) {
                TreeSet<UUID> ids = idx.load(clusterId);

                Collection<? extends DataObject> items = tbl.loadAll(ids);

                tx.commit();

                sendResult(ctx, new JsonBuilder().addArray(items).buffer());
            }
        }
        catch (Throwable e) {
            sendError(ctx, errMsg, e);
        }
    }

    /**
     * Load short list of DTOs.
     *
     * @param ctx Context.
     * @param tbl Table with DTOs.
     * @param idx Index with DTOs IDs.
     * @param errMsg Message to show in case of error.
     */
    protected void loadShortList(RoutingContext ctx, Table<? extends DataObject> tbl, OneToManyIndex idx, String errMsg) {
        try {
            UUID clusterId = UUID.fromString(requestParam(ctx, "id"));

            try (Transaction tx = txStart()) {
                TreeSet<UUID> ids = idx.load(clusterId);

                Collection<? extends DataObject> items = tbl.loadAll(ids);

                tx.commit();

                JsonArray res = new JsonArray(items.stream().map(DataObject::shortView).collect(Collectors.toList()));

                sendResult(ctx, res);
            }
        }
        catch (Throwable e) {
            sendError(ctx, errMsg, e);
        }
    }
}
