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

import static org.apache.ignite.console.common.Utils.emptyJson;

/**
 * Admin router.
 */
public class AdminRouter extends AbstractRouter {
    /** */
    private static final String E_FAILED_TO_LOAD_USERS = "Failed to load users list";

    /**
     * @param ignite Ignite.
     * @param vertx Vertx.
     */
    public AdminRouter(Ignite ignite, Vertx vertx) {
        super(ignite, vertx);
    }

    /** {@inheritDoc} */
    @Override public void install(Router router) {
        router.post("/api/v1/admin/list").handler(this::list);
        router.post("/api/v1/admin/remove").handler(this::remove);
        router.post("/api/v1/admin/toggle").handler(this::toggle);
        router.get("/api/v1/admin/become").handler(this::become);
        router.get("/api/v1/admin/revert/identity").handler(this::revertIdentity);
        router.put("/api/v1/admin/notifications").handler(this::notifications);
    }

    /**
     * @param ctx Context.
     */
    private void list(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null)
            send(Addresses.ACCOUNT_LIST, emptyJson(), ctx, E_FAILED_TO_LOAD_USERS);
    }

    /**
     * @param ctx Context.
     */
    private void toggle(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                JsonObject msg = new JsonObject();

//                UUID userId = UUID.fromString(requestParam(ctx, "userId"));
//                boolean adminFlag = Boolean.parseBoolean(requestParam(ctx, "adminFlag"));

                // .toggle(userId, adminFlag);
            }
            catch (Throwable e) {
                replyWithError(ctx, "Failed to change admin status", e);
            }
        }
    }

    /**
     * @param ctx Context.
     */
    private void remove(RoutingContext ctx) {
        replyWithResult(ctx, "[]");
    }

    /**
     * @param ctx Context.
     */
    private void become(RoutingContext ctx) {
        replyWithResult(ctx, "[]");
    }

    /**
     * @param ctx Context.
     */
    private void revertIdentity(RoutingContext ctx) {
        replyWithResult(ctx, "[]");
    }

    /**
     * @param ctx Context.
     */
    private void notifications(RoutingContext ctx) {
        replyWithResult(ctx, "[]");
    }
}
