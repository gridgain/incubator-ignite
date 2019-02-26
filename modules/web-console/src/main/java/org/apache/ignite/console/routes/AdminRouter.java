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

import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.ignite.Ignite;

/**
 * Admin router.
 */
public class AdminRouter extends AbstractRouter {
    /**
     * @param ignite Ignite.
     */
    public AdminRouter(Ignite ignite) {
        super(ignite);
    }

    /** {@inheritDoc} */
    @Override protected void initializeCaches() {
        // No-op so far.
    }

    /** {@inheritDoc} */
    @Override public void install(Router router) {
        router.post("/api/v1/admin/list").handler(this::dummy);
        router.post("/api/v1/admin/remove").handler(this::dummy);
        router.post("/api/v1/admin/toggle").handler(this::dummy);
        router.get("/api/v1/admin/become").handler(this::dummy);
        router.get("/api/v1/admin/revert/identity").handler(this::dummy);
        router.put("/api/v1/admin/notifications").handler(this::dummy);
    }

    /**
     * @param ctx Context.
     */
    private void dummy(RoutingContext ctx) {
        sendResult(ctx, "[]");
    }
}
