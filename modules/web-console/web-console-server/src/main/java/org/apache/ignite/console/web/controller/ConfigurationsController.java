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

package org.apache.ignite.console.web.controller;

import org.apache.ignite.Ignite;

/**
 * Router to handle REST API for configurations.
 */
public class ConfigurationsController {
    /** */
    private static final String E_FAILED_TO_LOAD_CONFIGURATION = "Failed to load configuration";

    /** */
    private static final String E_FAILED_TO_LOAD_CLUSTERS = "Failed to load clusters";

    /** */
    private static final String E_FAILED_TO_LOAD_CLUSTER = "Failed to load cluster";

    /** */
    private static final String E_FAILED_TO_LOAD_CLUSTER_CACHES = "Failed to load cluster caches";

    /** */
    private static final String E_FAILED_TO_LOAD_CLUSTER_MODELS = "Failed to load cluster models";

    /** */
    private static final String E_FAILED_TO_LOAD_CLUSTER_IGFSS = "Failed to load cluster IGFSs";

    /** */
    private static final String E_FAILED_TO_LOAD_CACHE = "Failed to load cache";

    /** */
    private static final String E_FAILED_TO_LOAD_MODEL = "Failed to load model";

    /** */
    private static final String E_FAILED_TO_LOAD_IGFS = "Failed to load IGFS";

    /** */
    private static final String E_FAILED_TO_SAVE_CLUSTER = "Failed to save cluster";

    /** */
    private static final String E_FAILED_TO_DELETE_CLUSTER = "Failed to delete cluster";

    /**
     * @param ignite Ignite.
     */
    public ConfigurationsController(Ignite ignite) {

    }

//    /** {@inheritDoc} */
//    @Override public void install(Router router) {
//        authenticatedRoute(router, GET, "/api/v1/configuration/:clusterId", this::loadConfiguration);
//        authenticatedRoute(router, GET, "/api/v1/configuration/clusters", this::loadClustersShortList);
//        authenticatedRoute(router, GET, "/api/v1/configuration/clusters/:clusterId", this::loadCluster);
//        authenticatedRoute(router, GET, "/api/v1/configuration/clusters/:clusterId/caches", this::loadCachesShortList);
//        authenticatedRoute(router, GET, "/api/v1/configuration/clusters/:clusterId/models", this::loadModelsShortList);
//        authenticatedRoute(router, GET, "/api/v1/configuration/clusters/:clusterId/igfss", this::loadIgfssShortList);
//
//        authenticatedRoute(router, GET, "/api/v1/configuration/caches/:cacheId", this::loadCache);
//        authenticatedRoute(router, GET, "/api/v1/configuration/domains/:modelId", this::loadModel);
//        authenticatedRoute(router, GET, "/api/v1/configuration/igfs/:igfsId", this::loadIgfs);
//
//        authenticatedRoute(router, PUT, "/api/v1/configuration/clusters", this::saveAdvancedCluster);
//        authenticatedRoute(router, PUT, "/api/v1/configuration/clusters/basic", this::saveBasicCluster);
//        authenticatedRoute(router, POST, "/api/v1/configuration/clusters/remove", this::deleteClusters);
//    }

//    /**
//     * @param ctx Context.
//     */
//    private void loadConfiguration(RoutingContext ctx) {
//        User user = getContextAccount(ctx);
//
//        if (user != null)
//            send(Addresses.CONFIGURATION_LOAD, requestParams(ctx), ctx, E_FAILED_TO_LOAD_CONFIGURATION);
//    }
//
//    /**
//     * Load clusters short list.
//     *
//     * @param ctx Context.
//     */
//    private void loadClustersShortList(RoutingContext ctx) {
//        ContextAccount acc = getContextAccount(ctx);
//
//        JsonObject msg = new JsonObject()
//            .put("user", acc.principal());
//
//        send(Addresses.CONFIGURATION_LOAD_SHORT_CLUSTERS, msg, ctx, E_FAILED_TO_LOAD_CLUSTERS);
//    }
//
//    /**
//     * @param ctx Cluster.
//     */
//    private void loadCluster(RoutingContext ctx) {
//        getContextAccount(ctx);
//
//        JsonObject msg = new JsonObject()
//            .put("cluster", requestParams(ctx));
//
//        send(Addresses.CONFIGURATION_LOAD_CLUSTER, msg, ctx, E_FAILED_TO_LOAD_CLUSTER);
//    }
//
//    /**
//     * Load cluster caches short list.
//     *
//     * @param ctx Context.
//     */
//    private void loadCachesShortList(RoutingContext ctx) {
//        getContextAccount(ctx);
//
//        JsonObject msg = new JsonObject()
//            .put("cluster", requestParams(ctx));
//
//        send(Addresses.CONFIGURATION_LOAD_SHORT_CACHES, msg, ctx, E_FAILED_TO_LOAD_CLUSTER_CACHES);
//    }
//
//    /**
//     * Load cluster models short list.
//     *
//     * @param ctx Context.
//     */
//    private void loadModelsShortList(RoutingContext ctx) {
//        getContextAccount(ctx);
//
//        JsonObject msg = new JsonObject()
//            .put("cluster", requestParams(ctx));
//
//        send(Addresses.CONFIGURATION_LOAD_SHORT_MODELS, msg, ctx, E_FAILED_TO_LOAD_CLUSTER_MODELS);
//    }
//
//    /**
//     * Get cluster IGFSs short list.
//     *
//     * @param ctx Context.
//     */
//    private void loadIgfssShortList(RoutingContext ctx) {
//        getContextAccount(ctx);
//
//        JsonObject msg = new JsonObject()
//            .put("cluster", requestParams(ctx));
//
//        send(Addresses.CONFIGURATION_LOAD_SHORT_IGFSS, msg, ctx, E_FAILED_TO_LOAD_CLUSTER_IGFSS);
//    }
//
//    /**
//     * @param ctx Context.
//     */
//    private void loadCache(RoutingContext ctx) {
//        getContextAccount(ctx);
//
//        JsonObject msg = requestParams(ctx);
//
//        send(Addresses.CONFIGURATION_LOAD_CACHE, msg, ctx, E_FAILED_TO_LOAD_CACHE);
//    }
//
//    /**
//     * @param ctx Context.
//     */
//    private void loadModel(RoutingContext ctx) {
//        getContextAccount(ctx);
//
//        JsonObject msg = requestParams(ctx);
//
//        send(Addresses.CONFIGURATION_LOAD_MODEL, msg, ctx, E_FAILED_TO_LOAD_MODEL);
//    }
//
//    /**
//     * @param ctx Context.
//     */
//    private void loadIgfs(RoutingContext ctx) {
//        getContextAccount(ctx);
//
//        JsonObject msg = requestParams(ctx);
//
//        send(Addresses.CONFIGURATION_LOAD_IGFS, msg, ctx, E_FAILED_TO_LOAD_IGFS);
//    }
//
//    /**
//     * Save cluster.
//     *
//     * @param ctx Context.
//     */
//    private void saveAdvancedCluster(RoutingContext ctx) {
//        User user = getContextAccount(ctx);
//
//        JsonObject msg = new JsonObject()
//            .put("user", user.principal())
//            .put("cluster", ctx.getBodyAsJson());
//
//        send(Addresses.CONFIGURATION_SAVE_CLUSTER_ADVANCED, msg, ctx, E_FAILED_TO_SAVE_CLUSTER);
//    }
//
//    /**
//     * Save basic cluster.
//     *
//     * @param ctx Context.
//     */
//    private void saveBasicCluster(RoutingContext ctx) {
//        User user = getContextAccount(ctx);
//
//        JsonObject msg = new JsonObject()
//            .put("user", user.principal())
//            .put("cluster", ctx.getBodyAsJson());
//
//        send(Addresses.CONFIGURATION_SAVE_CLUSTER_BASIC, msg, ctx, E_FAILED_TO_SAVE_CLUSTER);
//    }
//
//    /**
//     * Delete clusters.
//     *
//     * @param ctx Context.
//     */
//    private void deleteClusters(RoutingContext ctx) {
//        User user = getContextAccount(ctx);
//
//        JsonObject msg = new JsonObject()
//            .put("user", user.principal())
//            .put("cluster", ctx.getBodyAsJson());
//
//        send(Addresses.CONFIGURATION_DELETE_CLUSTER, msg, ctx, E_FAILED_TO_DELETE_CLUSTER);
//    }
}

