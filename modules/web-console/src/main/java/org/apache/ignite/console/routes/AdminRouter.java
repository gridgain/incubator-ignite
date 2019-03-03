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

import java.util.UUID;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.ignite.Ignite;

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

        if (user != null) {
            try {
                JsonArray res = null; // accSrvc.list();

                sendResult(ctx, res);
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to load users list", e);
            }
        }
    }

    /**
     * @param ctx Context.
     */
    private void toggle(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID userId = UUID.fromString(requestParam(ctx, "userId"));
                boolean adminFlag = Boolean.parseBoolean(requestParam(ctx, "adminFlag"));

                // accSrvc.toggle(userId, adminFlag);
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to change admin status", e);
            }
        }
    }

    /**
     * @param ctx Context.
     */
    private void remove(RoutingContext ctx) {
        sendResult(ctx, "[]");
    }

    /**
     * @param ctx Context.
     */
    private void become(RoutingContext ctx) {
        sendResult(ctx, "[]");
    }

    /**
     * @param ctx Context.
     */
    private void revertIdentity(RoutingContext ctx) {
        sendResult(ctx, "[]");
    }

    /**
     * @param ctx Context.
     */
    private void notifications(RoutingContext ctx) {
        sendResult(ctx, "[]");
    }
}
