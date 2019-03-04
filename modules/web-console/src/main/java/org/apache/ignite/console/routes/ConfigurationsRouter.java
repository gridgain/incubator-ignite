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

/**
 * Router to handle REST API for configurations.
 */
public class ConfigurationsRouter extends AbstractRouter {
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
     * @param vertx Vertx.
     */
    public ConfigurationsRouter(Ignite ignite, Vertx vertx) {
        super(ignite, vertx);
    }

    /** {@inheritDoc} */
    @Override public void install(Router router) {
        router.get("/api/v1/configuration/:clusterId").handler(this::loadConfiguration);
        router.get("/api/v1/configuration/clusters").handler(this::loadClustersShortList);
        router.get("/api/v1/configuration/clusters/:clusterId").handler(this::loadCluster);
        router.get("/api/v1/configuration/clusters/:clusterId/caches").handler(this::loadCachesShortList);
        router.get("/api/v1/configuration/clusters/:clusterId/models").handler(this::loadModelsShortList);
        router.get("/api/v1/configuration/clusters/:clusterId/igfss").handler(this::loadIgfssShortList);

        router.get("/api/v1/configuration/caches/:cacheId").handler(this::loadCache);
        router.get("/api/v1/configuration/domains/:modelId").handler(this::loadModel);
        router.get("/api/v1/configuration/igfs/:igfsId").handler(this::loadIgfs);

        router.put("/api/v1/configuration/clusters").handler(this::saveAdvancedCluster);
        router.put("/api/v1/configuration/clusters/basic").handler(this::saveBasicCluster);
        router.post("/api/v1/configuration/clusters/remove").handler(this::deleteClusters);
    }

    /**
     * @param ctx Context.
     */
    private void loadConfiguration(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null)
            send(Addresses.CONFIGURATION_LOAD, requestParams(ctx), ctx, E_FAILED_TO_LOAD_CONFIGURATION);
    }

    /**
     * Load clusters short list.
     *
     * @param ctx Context.
     */
    private void loadClustersShortList(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            JsonObject msg = new JsonObject()
                .put("user", user.principal());

            send(Addresses.CONFIGURATION_LOAD_SHORT_CLUSTERS, msg, ctx, E_FAILED_TO_LOAD_CLUSTERS);
        }
    }

    /**
     * @param ctx Cluster.
     */
    private void loadCluster(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            JsonObject msg = new JsonObject()
                .put("cluster", requestParams(ctx));

            send(Addresses.CONFIGURATION_LOAD_CLUSTER, msg, ctx, E_FAILED_TO_LOAD_CLUSTER);
        }
    }

    /**
     * Load cluster caches short list.
     *
     * @param ctx Context.
     */
    private void loadCachesShortList(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            JsonObject msg = new JsonObject()
                .put("cluster", requestParams(ctx));

            send(Addresses.CONFIGURATION_LOAD_SHORT_CACHES, msg, ctx, E_FAILED_TO_LOAD_CLUSTER_CACHES);
        }
    }

    /**
     * Load cluster models short list.
     *
     * @param ctx Context.
     */
    private void loadModelsShortList(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            JsonObject msg = new JsonObject()
                .put("cluster", requestParams(ctx));

            send(Addresses.CONFIGURATION_LOAD_SHORT_MODELS, msg, ctx, E_FAILED_TO_LOAD_CLUSTER_MODELS);
        }
    }

    /**
     * Get cluster IGFSs short list.
     *
     * @param ctx Context.
     */
    private void loadIgfssShortList(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            JsonObject msg = new JsonObject()
                .put("cluster", requestParams(ctx));

            send(Addresses.CONFIGURATION_LOAD_SHORT_IGFSS, msg, ctx, E_FAILED_TO_LOAD_CLUSTER_IGFSS);
        }
    }

    /**
     * @param ctx Context.
     */
    private void loadCache(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            JsonObject msg = requestParams(ctx);

            send(Addresses.CONFIGURATION_LOAD_CACHE, msg, ctx, E_FAILED_TO_LOAD_CACHE);
        }
    }

    /**
     * @param ctx Context.
     */
    private void loadModel(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            JsonObject msg = requestParams(ctx);

            send(Addresses.CONFIGURATION_LOAD_MODEL, msg, ctx, E_FAILED_TO_LOAD_MODEL);
        }
    }

    /**
     * @param ctx Context.
     */
    private void loadIgfs(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            JsonObject msg = requestParams(ctx);

            send(Addresses.CONFIGURATION_LOAD_IGFS, msg, ctx, E_FAILED_TO_LOAD_IGFS);
        }
    }

    /**
     * Save cluster.
     *
     * @param ctx Context.
     */
    private void saveAdvancedCluster(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            JsonObject msg = new JsonObject()
                .put("user", user.principal())
                .put("cluster", ctx.getBodyAsJson());

            send(Addresses.CONFIGURATION_SAVE_CLUSTER_ADVANCED, msg, ctx, E_FAILED_TO_SAVE_CLUSTER);
        }
    }

    /**
     * Save basic cluster.
     *
     * @param ctx Context.
     */
    private void saveBasicCluster(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            JsonObject msg = new JsonObject()
                .put("user", user.principal())
                .put("cluster", ctx.getBodyAsJson());

            send(Addresses.CONFIGURATION_SAVE_CLUSTER_BASIC, msg, ctx, E_FAILED_TO_SAVE_CLUSTER);
        }
    }

    /**
     * Delete clusters.
     *
     * @param ctx Context.
     */
    private void deleteClusters(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            JsonObject msg = new JsonObject()
                .put("user", user.principal())
                .put("cluster", ctx.getBodyAsJson());

            send(Addresses.CONFIGURATION_DELETE_CLUSTER, msg, ctx, E_FAILED_TO_DELETE_CLUSTER);
        }
    }
}

