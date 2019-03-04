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
import java.util.Map;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import org.apache.ignite.Ignite;
import org.jetbrains.annotations.Nullable;

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.apache.ignite.console.common.Utils.sendError;
import static org.apache.ignite.console.common.Utils.sendResult;
import static org.apache.ignite.console.common.Utils.sendStatus;

/**
 * Base class for routers.
 */
public abstract class AbstractRouter implements RestApiRouter {
    /** */
    protected final Ignite ignite;

    /** */
    protected final Vertx vertx;

    /**
     * @param ignite Ignite.
     * @param vertx Vertx.
     */
    protected AbstractRouter(Ignite ignite, Vertx vertx) {
        this.ignite = ignite;
        this.vertx = vertx;
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
     * @param ctx Context.
     * @return Request params.
     */
    protected JsonObject requestParams(RoutingContext ctx) {
        JsonObject params = new JsonObject();

        for (Map.Entry<String, String> entry : ctx.request().params().entries())
            params.put(entry.getKey(), entry.getValue());

        return params;
    }

    /**
     * @param addr Address where to send message.
     * @param msg Message to send.
     * @param ctx Context.
     * @param errMsg Error message.
     */
    public <T> void send(String addr, Object msg, RoutingContext ctx, String errMsg) {
        vertx.eventBus().send(addr, msg, asyncRes -> {
            if (asyncRes.succeeded())
                sendResult(ctx, asyncRes.result().body());
            else {
                ignite.log().error(errMsg, asyncRes.cause());

                sendError(ctx, HTTP_INTERNAL_ERROR, errMsg, asyncRes.cause());
            }
        });
    }

    /**
     * @param ctx Context.
     */
    protected void replyOk(RoutingContext ctx) {
        sendStatus(ctx, HTTP_OK);
    }

    /**
     * @param ctx Context.
     */
    protected void replyWithResult(RoutingContext ctx, Object res) {
        sendResult(ctx, res);
    }

    /**
     * @param ctx Context.
     * @param errMsg Error message to send.
     * @param e Error to send.
     */
    protected void replyWithError(RoutingContext ctx, int errCode, String errMsg, Throwable e) {
        ignite.log().error(errMsg, e);

        sendError(ctx, errCode, errMsg, e);
    }

    /**
     * @param ctx Context.
     * @param errMsg Error message to send.
     * @param e Error to send.
     */
    protected void replyWithError(RoutingContext ctx, String errMsg, Throwable e) {
        replyWithError(ctx, HTTP_INTERNAL_ERROR, errMsg, e);
    }
}
