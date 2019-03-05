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
import org.apache.ignite.Ignite;
import org.apache.ignite.console.common.Addresses;

import static io.vertx.core.http.HttpMethod.GET;
import static io.vertx.core.http.HttpMethod.POST;
import static io.vertx.core.http.HttpMethod.PUT;

/**
 * Admin router.
 */
public class AdminRouter extends AbstractRouter {
    /** */
    private static final String E_FAILED_TO_LOAD_USERS = "Failed to load users list";

    /** */
    private static final String E_FAILED_TO_CHANGE_ADMIN_STATUS = "Failed to change admin status";

    /**
     * @param ignite Ignite.
     * @param vertx Vertx.
     */
    public AdminRouter(Ignite ignite, Vertx vertx) {
        super(ignite, vertx);
    }

    /** {@inheritDoc} */
    @Override public void install(Router router) {
        registerRout(router, POST, "/api/v1/admin/list", this::list);
        registerRout(router, POST, "/api/v1/admin/remove", this::remove);
        registerRout(router, POST, "/api/v1/admin/toggle", this::toggle);
        registerRout(router, GET, "/api/v1/admin/become", this::become);
        registerRout(router, GET, "/api/v1/admin/revert/identity", this::revertIdentity);
        registerRout(router, PUT, "/api/v1/admin/notifications", this::notifications);
    }

    /**
     * @param ctx Context.
     */
    private void list(RoutingContext ctx) {
        checkUser(ctx);

        JsonObject msg = new JsonObject();

        send(Addresses.ADMIN_LOAD_ACCOUNTS, msg, ctx, E_FAILED_TO_LOAD_USERS);
    }

    /**
     * @param ctx Context.
     */
    private void toggle(RoutingContext ctx) {
        User user = checkUser(ctx);

        JsonObject msg = new JsonObject()
            .put("user", user.principal())
            .put("admin", requestParams(ctx));

        send(Addresses.ADMIN_CHANGE_ADMIN_STATUS, msg, ctx, E_FAILED_TO_CHANGE_ADMIN_STATUS);
    }

    /**
     * @param ctx Context.
     */
    private void remove(RoutingContext ctx) {
        replyWithError(ctx, "Failed to remove user", new IllegalStateException("Not implemented yet"));
    }

    /**
     * @param ctx Context.
     */
    private void become(RoutingContext ctx) {
        replyWithError(ctx, "Failed to become user", new IllegalStateException("Not implemented yet"));
    }

    /**
     * @param ctx Context.
     */
    private void revertIdentity(RoutingContext ctx) {
        replyWithError(ctx, "Failed to revert to your identity", new IllegalStateException("Not implemented yet"));
    }

    /**
     * @param ctx Context.
     */
    private void notifications(RoutingContext ctx) {
        replyWithError(ctx, "Failed to change notifications", new IllegalStateException("Not implemented yet"));
    }
}
