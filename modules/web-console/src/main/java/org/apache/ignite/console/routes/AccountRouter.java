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

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.UserSessionHandler;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.auth.IgniteAuth;
import org.apache.ignite.console.common.Addresses;

import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

/**
 * Router to handle REST API for configurations.
 */
public class AccountRouter extends AbstractRouter {
    /** */
    private static final String E_SIGN_UP_FAILED = "Sign up failed";

    /** */
    private static final String E_FAILED_TO_GET_USER = "Failed to get user";

    /** */
    private final IgniteAuth authProvider;

    /**
     * @param ignite Ignite.
     * @param vertx Vertx.
     */
    public AccountRouter(Ignite ignite, Vertx vertx) {
        super(ignite, vertx);

        authProvider = new IgniteAuth(ignite, vertx);
    }

    /** {@inheritDoc} */
    @Override public void install(Router router) {
        router.route().handler(UserSessionHandler.create(authProvider));

        router.post("/api/v1/user").handler(this::getUser);
        router.post("/api/v1/signup").handler(this::signUp);
        router.post("/api/v1/signin").handler(this::signIn);
        router.post("/api/v1/logout").handler(this::logout);

        router.post("/api/v1/password/forgot").handler(this::forgotPassword);
        router.post("/api/v1/password/reset").handler(this::resetPassword);
        router.post("/api/v1/password/validate/token").handler(this::validateToken);
    }

    /**
     * @param ctx Context
     */
    private void getUser(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null)
            send(Addresses.ACCOUNT_GET_BY_ID, user.principal(), ctx, E_FAILED_TO_GET_USER);
    }

    /**
     * @param ctx Context
     */
    private void signUp(RoutingContext ctx) {
        try {
            JsonObject body = ctx.getBodyAsJson();

            authProvider.registerAccount(body, asyncRes -> {
                if (asyncRes.succeeded())
                    signIn(ctx);
                else
                    replyWithError(ctx, E_SIGN_UP_FAILED, asyncRes.cause());
            });
        }
        catch (Throwable e) {
            replyWithError(ctx, E_SIGN_UP_FAILED, e);
        }
    }

    /**
     * @param ctx Context
     */
    private void signIn(RoutingContext ctx) {
        authProvider.authenticate(ctx.getBody().toJsonObject(), asyncRes -> {
            if (asyncRes.succeeded()) {
                ctx.setUser(asyncRes.result());

                replyOk(ctx);
            }
            else
                replyWithError(ctx, HTTP_UNAUTHORIZED, "Sign in failed", asyncRes.cause());
        });
    }

    /**
     * @param ctx Context
     */
    private void logout(RoutingContext ctx) {
        ctx.clearUser();

        replyOk(ctx);
    }

    /**
     * @param ctx Context
     */
    private void forgotPassword(RoutingContext ctx) {
        try {
            throw new IllegalStateException("Not implemented yet");
        }
        catch (Throwable e) {
            replyWithError(ctx, "Failed to restore password", e);
        }
    }

    /**
     * @param ctx Context
     */
    private void resetPassword(RoutingContext ctx) {
        try {
            throw new IllegalStateException("Not implemented yet");
        }
        catch (Throwable e) {
            replyWithError(ctx, "Failed to reset password", e);
        }
    }

    /**
     * @param ctx Context
     */
    private void validateToken(RoutingContext ctx) {
        try {
            throw new IllegalStateException("Not implemented yet");
        }
        catch (Throwable e) {
            replyWithError(ctx, "Failed to validate token", e);
        }
    }
}
