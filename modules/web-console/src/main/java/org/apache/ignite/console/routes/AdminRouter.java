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

import java.util.List;
import java.util.UUID;
import javax.cache.Cache;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.services.Services;

/**
 * Admin router.
 */
public class AdminRouter extends AbstractRouter {
    /** */
    private final Services srvcs;

    /**
     * @param ignite Ignite.
     * @param srvcs Services.
     */
    public AdminRouter(Ignite ignite, Services srvcs) {
        super(ignite);

        this.srvcs = srvcs;
    }

    /** {@inheritDoc} */
    @Override public void install(Router router) {
        router.post("/api/v1/admin/list").handler(this::list);
        router.post("/api/v1/admin/remove").handler(this::dummy);
        router.post("/api/v1/admin/toggle").handler(this::toggle);
        router.get("/api/v1/admin/become").handler(this::dummy);
        router.get("/api/v1/admin/revert/identity").handler(this::dummy);
        router.put("/api/v1/admin/notifications").handler(this::dummy);
    }

    /**
     * @param ctx Context.
     */
    private void  list(RoutingContext ctx) {
        try {
            IgniteCache<UUID, Account> cache = null; // tblAccounts.cache();

            List<Cache.Entry<UUID, Account>> users = cache.query(new ScanQuery<UUID, Account>()).getAll();

            JsonArray res = new JsonArray();

            users.forEach(entry -> {
                Object v = entry.getValue();

                if (v instanceof Account) {
                    Account user = (Account)v;

                    res.add(new JsonObject()
                        .put("_id", user._id())
                        .put("firstName", user.firstName())
                        .put("lastName", user.lastName())
                        .put("admin", user.admin())
                        .put("email", user.email())
                        .put("company", user.company())
                        .put("country", user.country())
                        .put("lastLogin", user.lastLogin())
                        .put("lastActivity", user.lastActivity())
                        .put("activated", user.activated())
                        .put("counters", new JsonObject()
                            .put("clusters", 0)
                            .put("caches", 0)
                            .put("models", 0)
                            .put("igfs", 0))
                        .put("activitiesTotal", 0)
                        .put("activitiesDetail", 0)
                    );
                }
            });

            sendResult(ctx, res);
        }
        catch (Throwable e) {
            sendError(ctx, "Failed to load users list", e);
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

//                try(Transaction tx = txStart()) {
//                    Account acc = tblAccounts.load(userId);
//
//                    if (acc == null)
//                        throw new IllegalStateException("Account not found for id: " + userId);
//
//                    acc.admin(adminFlag);
//
//                    tblAccounts.save(acc);
//
//                    tx.commit();
//                }

            }
            catch (Throwable e) {
                sendError(ctx, "Failed to change admin status", e);
            }
        }
    }

    /**
     * @param ctx Context.
     */
    private void dummy(RoutingContext ctx) {
        sendResult(ctx, "[]");
    }
}
