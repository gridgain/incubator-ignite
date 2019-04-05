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

/**
 * Router to handle REST API for notebooks.
 */
public class NotebooksRouter extends AbstractRouter {
    /** */
    private static final String E_FAILED_TO_LOAD_NOTEBOOKS = "Failed to load notebooks";

    /** */
    private static final String E_FAILED_TO_SAVE_NOTEBOOK = "Failed to save notebook";

    /** */
    private static final String E_FAILED_TO_DELETE_NOTEBOOK = "Failed to delete notebook";

    /**
     * @param ignite Ignite.
     */
    public NotebooksRouter(Ignite ignite) {
        super(ignite);
    }

//    /** {@inheritDoc} */
//    @Override public void install(Router router) {
//        authenticatedRoute(router, GET, "/api/v1/notebooks", this::load);
//        authenticatedRoute(router, POST, "/api/v1/notebooks/save", this::save);
//        authenticatedRoute(router, POST, "/api/v1/notebooks/remove", this::delete);
//    }

//    /**
//     * Load notebooks.
//     *
//     * @param ctx Context.
//     */
//    private void load(RoutingContext ctx) {
//        User user = getContextAccount(ctx);
//
//        JsonObject msg = new JsonObject()
//            .put("user", user.principal());
//
//        send(Addresses.NOTEBOOK_LIST, msg, ctx, E_FAILED_TO_LOAD_NOTEBOOKS);
//    }

//    /**
//     * Save notebook.
//     *
//     * @param ctx Context.
//     */
//    private void save(RoutingContext ctx) {
//        User user = getContextAccount(ctx);
//
//        JsonObject msg = new JsonObject()
//            .put("user", user.principal())
//            .put("notebook", ctx.getBodyAsJson());
//
//        send(Addresses.NOTEBOOK_SAVE, msg, ctx, E_FAILED_TO_SAVE_NOTEBOOK);
//    }

//    /**
//     * Remove notebook.
//     *
//     * @param ctx Context.
//     */
//    private void delete(RoutingContext ctx) {
//        User user = getContextAccount(ctx);
//
//        JsonObject msg = new JsonObject()
//            .put("user", user.principal())
//            .put("notebook", ctx.getBodyAsJson());
//
//        send(Addresses.NOTEBOOK_DELETE, msg, ctx, E_FAILED_TO_DELETE_NOTEBOOK);
//    }
}
