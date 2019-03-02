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
import org.apache.ignite.IgniteException;
import org.apache.ignite.console.auth.IgniteAuth;
import org.apache.ignite.console.services.AccountsService;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.apache.ignite.console.common.Utils.errorMessage;
import static org.apache.ignite.console.common.Utils.getBoolean;

/**
 * Router to handle REST API for configurations.
 */
public class AccountRouter extends AbstractRouter {
    /** */
    private final IgniteAuth authProvider;

    /**
     * @param ignite Ignite.
     * @param vertx Vertx.
     * @param accSrvc Accounts service.
     */
    public AccountRouter(Ignite ignite, Vertx vertx, AccountsService accSrvc) {
        super(ignite, vertx);

        authProvider = new IgniteAuth(ignite, vertx, accSrvc);
    }

    /** {@inheritDoc} */
    @Override public void install(Router router) {
        router.route().handler(UserSessionHandler.create(authProvider));

        router.route("/api/v1/user").handler(this::getAccount);
        router.route("/api/v1/signup").handler(this::signUp);
        router.route("/api/v1/signin").handler(this::signIn);
        router.route("/api/v1/logout").handler(this::logout);

//        router.route("/api/v1/password/forgot").handler(this::handleDummy);
//        router.route("/api/v1/password/reset").handler(this::handleDummy);
//        router.route("/api/v1/password/validate/token").handler(this::handleDummy);
    }

    /**
     * @param ctx Context
     */
    private void getAccount(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null)
            sendResult(ctx, user.principal());
    }

    /**
     * @param ctx Context
     */
    private void signUp(RoutingContext ctx) {
        try {
            JsonObject body = ctx.getBodyAsJson();

            // TODO WC-960 WRONG logic, we need to check that current User is ADMIN!
            if (getBoolean(body, "user.admin", false)) {
                sendResult(ctx, authProvider.registerAccount(body).principal());

                return;
            }

            authProvider.registerAccount(body);

            signIn(ctx);
        }
        catch (IgniteException e) {
            sendError(ctx, e.getMessage(), e);
        }
        catch (Throwable e) {
            sendError(ctx, "Sign up failed", e);
        }
    }

    /**
     * @param ctx Context
     */
    private void signIn(RoutingContext ctx) {
        authProvider.authenticate(ctx.getBody().toJsonObject(), asyncRes -> {
            if (asyncRes.succeeded()) {
                ctx.setUser(asyncRes.result());

                sendStatus(ctx, HTTP_OK);
            }
            else
                sendStatus(ctx, HTTP_UNAUTHORIZED, errorMessage(asyncRes.cause()));
        });
    }

    /**
     * @param ctx Context
     */
    private void logout(RoutingContext ctx) {
        ctx.clearUser();

        sendStatus(ctx, HTTP_OK);
    }
}
