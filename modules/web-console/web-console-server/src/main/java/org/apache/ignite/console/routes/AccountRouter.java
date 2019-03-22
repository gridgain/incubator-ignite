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

import org.apache.ignite.Ignite;
import org.apache.ignite.console.auth.ContextAccount;
import org.apache.ignite.console.auth.IgniteAuth;
import org.apache.ignite.console.common.Addresses;
import org.apache.ignite.internal.util.typedef.F;

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
     */
    public AccountRouter(Ignite ignite) {
        super(ignite);

        authProvider = new IgniteAuth(ignite);
    }

//    /** {@inheritDoc} */
//    @Override public void install(Router router) {
//        router.route().handler(UserSessionHandler.create(authProvider));
//
//        authenticatedRoute(router, GET, "/api/v1/user", this::getAccount);
//        authenticatedRoute(router, POST, "/api/v1/logout", this::logout);
//
//        publicRoute(router, POST, "/api/v1/signup", this::signUp);
//        publicRoute(router, POST, "/api/v1/signin", this::signIn);
//
//        publicRoute(router, POST, "/api/v1/password/forgot", this::forgotPassword);
//        publicRoute(router, POST, "/api/v1/password/reset", this::resetPassword);
//        publicRoute(router, POST, "/api/v1/password/validate/token", this::validateToken);
//    }
//
//    /**
//     * @param ctx Context
//     */
//    private void getAccount(RoutingContext ctx) {
//        ContextAccount acc = getContextAccount(ctx);
//
//        String viewedAccountId = ctx.session().get("viewedAccountId");
//
//        if (F.isEmpty(viewedAccountId)) {
//            send(Addresses.ACCOUNT_GET_BY_ID, acc.accountId(), ctx, E_FAILED_TO_GET_USER);
//
//            return;
//        }
//
//        acc.isAuthorized("admin", isAdminHandler -> {
//            if (isAdminHandler.failed() && isAdminHandler.result() == Boolean.FALSE) {
//                send(Addresses.ACCOUNT_GET_BY_ID, acc.accountId(), ctx, E_FAILED_TO_GET_USER);
//
//                return;
//            }
//
//            this.<JsonObject, JsonObject>send(Addresses.ACCOUNT_GET_BY_ID, viewedAccountId, ctx, E_FAILED_TO_GET_USER, viewedAccount -> {
//                viewedAccount.put("becomeUsed", true);
//
//                return viewedAccount;
//            });
//        });
//    }
//
//    /**
//     * @param ctx Context
//     */
//    private void signUp(RoutingContext ctx) {
//        JsonObject body = ctx.getBodyAsJson();
//
//        authProvider.registerAccount(body, asyncRes -> {
//            if (asyncRes.failed()) {
//                replyWithError(ctx, E_SIGN_UP_FAILED, asyncRes.cause());
//
//                return;
//            }
//
//            signIn(ctx);
//        });
//    }
//
//    /**
//     * @param ctx Context
//     */
//    private void signIn(RoutingContext ctx) {
//        authProvider.authenticate(ctx.getBodyAsJson(), asyncRes -> {
//            if (asyncRes.failed()) {
//                replyWithError(ctx, HTTP_UNAUTHORIZED, "Sign in failed", asyncRes.cause());
//
//                return;
//            }
//
//            ctx.setUser(asyncRes.result());
//
//            replyOk(ctx);
//        });
//    }
//
//    /**
//     * @param ctx Context
//     */
//    private void logout(RoutingContext ctx) {
//        ctx.clearUser();
//
//        replyOk(ctx);
//    }
//
//    /**
//     * @param ctx Context
//     */
//    private void forgotPassword(RoutingContext ctx) {
//        try {
//            throw new IllegalStateException("Not implemented yet");
//        }
//        catch (Throwable e) {
//            replyWithError(ctx, "Failed to restore password", e);
//        }
//    }
//
//    /**
//     * @param ctx Context
//     */
//    private void resetPassword(RoutingContext ctx) {
//        try {
//            throw new IllegalStateException("Not implemented yet");
//        }
//        catch (Throwable e) {
//            replyWithError(ctx, "Failed to reset password", e);
//        }
//    }
//
//    /**
//     * @param ctx Context
//     */
//    private void validateToken(RoutingContext ctx) {
//        try {
//            throw new IllegalStateException("Not implemented yet");
//        }
//        catch (Throwable e) {
//            replyWithError(ctx, "Failed to validate token", e);
//        }
//    }
}
