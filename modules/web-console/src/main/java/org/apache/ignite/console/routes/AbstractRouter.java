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

import java.util.Map;
import java.util.function.Function;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.auth.ContextAccount;
import org.apache.ignite.console.common.NotAuthorizedException;

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
     * @param router The router.
     * @param mtd The HTTP method to match.
     * @param path URI paths that begin with this path will match.
     * @param reqHnd Request handler.
     * @return the Route to enable fluent use
     */
    protected Route publicRoute(Router router, HttpMethod mtd, String path, Handler<RoutingContext> reqHnd) {
        return router.route(mtd, path).handler(reqHnd).failureHandler(this:: failureHandler);
    }

    /**
     * @param router The router.
     * @param mtd The HTTP method to match.
     * @param path URI paths that begin with this path will match.
     * @param reqHnd Handler that will be called if user is authenticated.
     * @return the Route to enable fluent use.
     */
    protected Route authenticatedRoute(Router router, HttpMethod mtd, String path, Handler<RoutingContext> reqHnd) {
        return publicRoute(router, mtd, path, ctx -> {
            if (ctx.user() == null)
                throw new NotAuthorizedException("Access denied. You are not authorized to access this page.");

            reqHnd.handle(ctx);
        });
    }

    /**
     * @param router The router.
     * @param mtd The HTTP method to match.
     * @param path URI paths that begin with this path will match.
     * @param authority The authority - what this really means is determined by the specific implementation.
     * @param reqHnd Handler that will be called if the they has the authority.
     * @return the Route to enable fluent use.
     */
    protected Route authorityRoute(Router router, HttpMethod mtd, String path, String authority, Handler<RoutingContext> reqHnd) {
        return authenticatedRoute(router, mtd, path, ctx -> {
            User account = ctx.user();

            account.isAuthorized(authority, isAdminHandler -> {
                if (isAdminHandler.failed() && isAdminHandler.result() == Boolean.FALSE)
                    throw new NotAuthorizedException("Access denied. You are not authorized to access this page.");

                reqHnd.handle(ctx);
            });
        });
    }

    /**
     * @param router The router.
     * @param mtd The HTTP method to match.
     * @param path URI paths that begin with this path will match.
     * @param reqHnd Handler that will be called if the they has the admin authority.
     * @return the Route to enable fluent use
     */
    protected Route adminRoute(Router router, HttpMethod mtd, String path, Handler<RoutingContext> reqHnd) {
        return authorityRoute(router, mtd, path, "admin", reqHnd);
    }

    /**
     * @param ctx Context.
     */
    protected void failureHandler(RoutingContext ctx) {
        Throwable cause = ctx.failure();

        if (cause instanceof NotAuthorizedException)
            sendError(ctx, HTTP_UNAUTHORIZED, null, cause);
        else
            sendError(ctx, HTTP_INTERNAL_ERROR, "Unhandled error", cause);
    }

    /**
     * Get the authenticated user (if any).
     *
     * @param ctx Context
     * @return Current authenticated user.
     * @throws NotAuthorizedException If current user not found in context.
     */
    protected ContextAccount getContextAccount(RoutingContext ctx) throws NotAuthorizedException {
        User user = ctx.user();

        if (user instanceof ContextAccount)
            return (ContextAccount)user;

        throw new NotAuthorizedException("Access denied. You are not authorized to access this page.");
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
    public <T, R> void send(String addr, Object msg, RoutingContext ctx, String errMsg, Function<T, R> mapper) {
        vertx.eventBus().<T>send(addr, msg, asyncRes -> {
            if (asyncRes.succeeded())
                sendResult(ctx, mapper.apply(asyncRes.result().body()));
            else {
                ignite.log().error(errMsg, asyncRes.cause());

                sendError(ctx, HTTP_INTERNAL_ERROR, errMsg, asyncRes.cause());
            }
        });
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
