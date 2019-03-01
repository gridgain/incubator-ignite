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

import java.util.TreeSet;
import java.util.UUID;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.dto.Cache;
import org.apache.ignite.console.dto.Cluster;
import org.apache.ignite.console.dto.Igfs;
import org.apache.ignite.console.dto.Model;
import org.apache.ignite.console.services.ConfigurationsService;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.console.common.Utils.idsFromJson;

/**
 * Router to handle REST API for configurations.
 */
public class ConfigurationsRouter extends AbstractRouter {
    /** */
    private final ConfigurationsService cfgSrvc;

    /**
     * @param ignite Ignite.
     */
    public ConfigurationsRouter(Ignite ignite, ConfigurationsService cfgSrvc) {
        super(ignite);

        this.cfgSrvc = cfgSrvc;
    }

    /** {@inheritDoc} */
    @Override public void install(Router router) {
        router.get("/api/v1/configuration/:id").handler(this::loadConfiguration);
        router.get("/api/v1/configuration/clusters").handler(this::loadClustersShortList);
        router.get("/api/v1/configuration/clusters/:id").handler(this::loadCluster);
        router.get("/api/v1/configuration/clusters/:id/caches").handler(this::loadCachesShortList);
        router.get("/api/v1/configuration/clusters/:id/models").handler(this::loadModelsShortList);
        router.get("/api/v1/configuration/clusters/:id/igfss").handler(this::loadIgfssShortList);

        router.get("/api/v1/configuration/caches/:id").handler(this::loadCache);
        router.get("/api/v1/configuration/domains/:id").handler(this::loadModel);
        router.get("/api/v1/configuration/igfs/:id").handler(this::loadIgfs);

        router.put("/api/v1/configuration/clusters").handler(this::saveAdvancedCluster);
        router.put("/api/v1/configuration/clusters/basic").handler(this::saveBasicCluster);
        router.post("/api/v1/configuration/clusters/remove").handler(this::deleteClusters);
    }

    /**
     * @param ctx Context.
     */
    private void loadConfiguration(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID clusterId = UUID.fromString(requestParam(ctx, "id"));

                JsonObject json = cfgSrvc.loadConfiguration(clusterId);

                sendResult(ctx, json);
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to load configuration", e);
            }
        }
    }

    /**
     * Load clusters short list.
     *
     * @param ctx Context.
     */
    private void loadClustersShortList(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID userId = getUserId(user.principal());

                JsonArray clusters = cfgSrvc.loadClusters(userId);

                sendResult(ctx, clusters);
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to load clusters", e);
            }
        }
    }

    /**
     * @param ctx Cluster.
     */
    private void loadCluster(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID clusterId = UUID.fromString(requestParam(ctx, "id"));


                Cluster cluster = cfgSrvc.loadCluster(clusterId);

                if (cluster == null)
                    throw new IllegalStateException("Cluster not found for ID: " + clusterId);

                sendResult(ctx, cluster.json());
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to load cluster", e);
            }
        }
    }

    /**
     * Load cluster caches short list.
     *
     * @param ctx Context.
     */
    private void loadCachesShortList(RoutingContext ctx) {
        loadShortList(ctx, cfgSrvc::loadCaches, "Failed to load cluster caches");
    }

    /**
     * Load cluster models short list.
     *
     * @param ctx Context.
     */
    private void loadModelsShortList(RoutingContext ctx) {
        loadShortList(ctx, cfgSrvc::loadModels, "Failed to load cluster models");
    }

    /**
     * Get cluster IGFSs short list.
     *
     * @param ctx Context.
     */
    private void loadIgfssShortList(RoutingContext ctx) {
        loadShortList(ctx, cfgSrvc::loadIgfss, "Failed to load cluster models");
    }


    /**
     * @param ctx Context.
     */
    private void loadCache(RoutingContext ctx) {
        checkUser(ctx);

        try {
            UUID cacheId = UUID.fromString(requestParam(ctx, "id"));

            Cache cache = cfgSrvc.loadCache(cacheId);

            if (cache == null)
                throw new IllegalStateException("Cache not found for ID: " + cacheId);

            sendResult(ctx, cache.json());
        }
        catch (Throwable e) {
            sendError(ctx, "Failed to get cache", e);
        }
    }

    /**
     * @param ctx Context.
     */
    private void loadModel(RoutingContext ctx) {
        checkUser(ctx);

        try {
            UUID mdlId = UUID.fromString(requestParam(ctx, "id"));

            Model mdl = cfgSrvc.loadModel(mdlId);
            sendResult(ctx, mdl.json());
        }
        catch (Throwable e) {
            sendError(ctx, "Failed to get model", e);
        }
    }

    /**
     * @param ctx Context.
     */
    private void loadIgfs(RoutingContext ctx) {
        checkUser(ctx);

        try {
            UUID igfsId = UUID.fromString(requestParam(ctx, "id"));

            Igfs igfs = cfgSrvc.loadIgfs(igfsId);

            sendResult(ctx, igfs.json());
        }
        catch (Throwable e) {
            sendError(ctx, "Failed to get IGFS", e);
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
            try {
                UUID userId = getUserId(user.principal());

                JsonObject json = ctx.getBodyAsJson();

                cfgSrvc.saveAdvancedCluster(userId, json);

                sendResult(ctx, rowsAffected(1));
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to save cluster", e);
            }
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
            try {
                UUID userId = getUserId(user.principal());

                JsonObject json = ctx.getBodyAsJson();

                cfgSrvc.saveBasicCluster(userId, json);

                sendResult(ctx, rowsAffected(1));
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to save cluster", e);
            }
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
            try {
                UUID userId = getUserId(user.principal());

                TreeSet<UUID> clusterIds = idsFromJson(ctx.getBodyAsJson(), "_id");

                if (F.isEmpty(clusterIds))
                    throw new IllegalStateException("Cluster IDs not found");

                cfgSrvc.deleteClusters(userId, clusterIds);

                sendResult(ctx, rowsAffected(clusterIds.size()));
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to delete cluster", e);
            }
        }
    }
}

