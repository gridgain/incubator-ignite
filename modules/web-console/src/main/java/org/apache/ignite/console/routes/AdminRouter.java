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
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.common.Addresses;

import static io.vertx.core.http.HttpMethod.DELETE;
import static io.vertx.core.http.HttpMethod.GET;
import static io.vertx.core.http.HttpMethod.PATCH;
import static io.vertx.core.http.HttpMethod.PUT;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static org.apache.ignite.console.common.Utils.boolParam;
import static org.apache.ignite.console.common.Utils.pathParam;
import static org.apache.ignite.console.common.Utils.sendError;

/**
 * Admin router.
 */
public class AdminRouter extends AbstractRouter {
    /**
     * @param ignite Ignite.
     * @param vertx Vertx.
     */
    public AdminRouter(Ignite ignite, Vertx vertx) {
        super(ignite, vertx);
    }

    /** {@inheritDoc} */
    @Override public void install(Router router) {
        adminRoute(router, GET, "/api/v1/admin/account", this::list);
        adminRoute(router, DELETE, "/api/v1/admin/account/:accountId", this::delete);
        adminRoute(router, PATCH, "/api/v1/admin/account/:accountId", this::update);
        adminRoute(router, GET, "/api/v1/admin/become/:accountId", this::become);
        adminRoute(router, GET, "/api/v1/admin/revert/identity", this::revertIdentity);

        adminRoute(router, PUT, "/api/v1/admin/notifications", this::notifications);
    }

    /**
     * @param ctx Context.
     */
    private void list(RoutingContext ctx) {
        getContextAccount(ctx);

        JsonObject msg = new JsonObject();

        send(Addresses.ADMIN_LOAD_ACCOUNTS, msg, ctx, "Failed to load users list");
    }

    /**
     * @param ctx Context.
     */
    private void update(RoutingContext ctx) {
        String accId = pathParam(ctx, "accountId");
        Boolean admin = boolParam(ctx.getBodyAsJson(), "admin");

        JsonObject msg = new JsonObject()
            .put("accountId", accId)
            .put("admin", admin);

        send(Addresses.ADMIN_CHANGE_ADMIN_STATUS, msg, ctx, "Failed to change admin status");
    }

    /**
     * @param ctx Context.
     */
    private void delete(RoutingContext ctx) {
        String accId = pathParam(ctx, "accountId");

        send(Addresses.ADMIN_DELETE_ACCOUNT, accId, ctx, "Failed to delete account");
    }

    /**
     * @param ctx Context.
     */
    private void become(RoutingContext ctx) {
        String viewedAccountId = pathParam(ctx, "accountId");

        vertx.eventBus().send(Addresses.ACCOUNT_GET_BY_ID, viewedAccountId, asyncRes -> {
            if (asyncRes.succeeded()) {
                ctx.session().put("viewedAccountId", viewedAccountId);

                replyOk(ctx);
            }
            else {
                ignite.log().error("Failed to become user", asyncRes.cause());

                sendError(ctx, HTTP_INTERNAL_ERROR, "Failed to become user", asyncRes.cause());
            }
        });
    }

    /**
     * @param ctx Context.
     */
    private void revertIdentity(RoutingContext ctx) {
        ctx.session().remove("viewedAccountId");

        replyOk(ctx);
    }

    /**
     * @param ctx Context.
     */
    private void notifications(RoutingContext ctx) {
        replyWithError(ctx, "Failed to change notifications", new IllegalStateException("Not implemented yet"));
    }
}
