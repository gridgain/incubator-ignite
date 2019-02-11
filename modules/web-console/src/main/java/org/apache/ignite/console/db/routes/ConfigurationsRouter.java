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

package org.apache.ignite.console.db.routes;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.core.CacheHolder;
import org.apache.ignite.console.db.dto.Cache;
import org.apache.ignite.console.db.dto.Cluster;
import org.apache.ignite.console.db.dto.Igfs;
import org.apache.ignite.console.db.dto.JsonBuilder;
import org.apache.ignite.console.db.dto.Model;
import org.apache.ignite.console.db.index.OneToManyIndex;
import org.apache.ignite.console.db.index.UniqueIndex;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.Transaction;

/**
 * Router to handle REST API for configurations.
 */
public class ConfigurationsRouter extends AbstractRouter {
    /** */
    private final CacheHolder<UUID, Cluster> accountClusters;

    /** */
    private final CacheHolder<UUID, Cache> clusterCaches;

    /** */
    private final CacheHolder<UUID, Model> clusterModels;

    /** */
    private final CacheHolder<UUID, Igfs> clusterIgfss;

    /** */
    private final OneToManyIndex accountClustersIdx;

    /** */
    private final UniqueIndex uniqueClusterNameIdx;

    /** */
    private final OneToManyIndex clusterCachesIdx;

    /** */
    private final OneToManyIndex clusterModelsIdx;

    /** */
    private final OneToManyIndex clusterIgfsIdx;

    /**
     * @param ignite Ignite.
     */
    public ConfigurationsRouter(Ignite ignite) {
        super(ignite);

        accountClusters = new CacheHolder<>(ignite, "wc_account_clusters");
        clusterCaches = new CacheHolder<>(ignite, "wc_cluster_caches");
        clusterModels = new CacheHolder<>(ignite, "wc_cluster_models");
        clusterIgfss = new CacheHolder<>(ignite, "wc_cluster_igfss");
        accountClustersIdx = new OneToManyIndex(ignite, "wc_account_clusters_idx");
        uniqueClusterNameIdx = new UniqueIndex(ignite, "wc_unique_cluster_name_idx");
        clusterCachesIdx = new OneToManyIndex(ignite, "wc_cluster_caches_idx");
        clusterModelsIdx = new OneToManyIndex(ignite, "wc_cluster_models_idx");
        clusterIgfsIdx = new OneToManyIndex(ignite, "wc_cluster_igfss_idx");
    }

    /** {@inheritDoc} */
    @Override protected void initializeCaches() {
        accountClusters.prepare();
        accountClustersIdx.prepare();
        uniqueClusterNameIdx.prepare();
        clusterCachesIdx.prepare();
        clusterModelsIdx.prepare();
        clusterIgfsIdx.prepare();
    }

    /** {@inheritDoc} */
    @Override public void install(Router router) {
        router.get("/api/v1/configuration/:id").handler(this::getConfiguration);
        router.get("/api/v1/configuration/clusters").handler(this::loadShortList);
        router.get("/api/v1/configuration/clusters/:id").handler(this::getCluster);
        router.get("/api/v1/configuration/clusters/:id/caches").handler(this::getClusterCaches);
        router.get("/api/v1/configuration/clusters/:id/models").handler(this::getClusterModels);
        router.get("/api/v1/configuration/clusters/:id/igfss").handler(this::getClusterIgfss);

        router.put("/api/v1/configuration/clusters").handler(this::saveAdvanced);
        router.put("/api/v1/configuration/clusters/basic").handler(this::saveBasic);
        router.post("/api/v1/configuration/clusters/remove").handler(this::remove);

        // router.route("/api/v1/configuration/domains").handler(this::handleDummy);
        // router.route("/api/v1/configuration/caches").handler(this::handleDummy);
        //  router.route("/api/v1/configuration/igfs").handler(this::handleDummy);
    }

    /**
     * Load clusters short list.
     *
     * @param ctx Context.
     */
    private void loadShortList(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID userId = getUserId(user.principal());

                try(Transaction tx = txStart()) {
                    TreeSet<UUID> clusterIds = accountClustersIdx.getIds(userId);

                    Collection<Cluster> clusters = accountClusters.getAll(clusterIds).values();

                    tx.commit();

                    JsonArray shortList = new JsonArray();

                    clusters.forEach(cluster -> {
                        // TODO IGNITE-5617 get counts...
                        int cachesCount = 0;
                        int modelsCount = 0;
                        int igfsCount = 0;

                        shortList.add(new JsonObject()
                            .put("_id", cluster._id())
                            .put("name", cluster.name())
                            .put("discovery", cluster.discovery())
                            .put("cachesCount", cachesCount)
                            .put("modelsCount", modelsCount)
                            .put("igfsCount", igfsCount));
                    });

                    sendResult(ctx, shortList.toBuffer());
                }
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to load clusters", e);
            }
        }
    }

    /**
     *
     * @param ctx
     * @param merge
     */
    private void saveCluster(RoutingContext ctx, Function<Cluster, Cluster> merge) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID userId = getUserId(user.principal());

                JsonObject rawData = ctx.getBodyAsJson();

                JsonObject rawCluster = rawData.getJsonObject("cluster");

                // rawData = Schemas.sanitize(Cluster.class, rawData);

                UUID clusterId = getId(rawCluster);

                if (clusterId == null)
                    throw new IllegalStateException("Cluster ID not found");

                String name = rawCluster.getString("name");

                if (F.isEmpty(name))
                    throw new IllegalStateException("Cluster name is empty");

                String discovery = rawCluster.getJsonObject("discovery").getString("kind");

                if (F.isEmpty(discovery))
                    throw new IllegalStateException("Cluster discovery not found");

                Cluster cluster = new Cluster(clusterId, null, name, discovery, rawCluster.encode());

                merge.apply(cluster);

                try(Transaction tx = txStart()) {
                    UUID prevId = uniqueClusterNameIdx.getAndPutIfAbsent(userId, cluster.name(), cluster.id());

                    if (prevId != null && !cluster.id().equals(prevId))
                        throw new IllegalStateException("Cluster with name '" + cluster.name() + "' already exits");

                    accountClustersIdx.putChild(userId, cluster.id());

                    accountClusters.put(clusterId, cluster);

                    tx.commit();
                }

                sendResult(ctx, cluster.json());
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to save cluster", e);
            }
        }
    }

    /**
     * Save cluster.
     *
     * @param ctx Context.
     */
    private void saveAdvanced(RoutingContext ctx) {
        saveCluster(ctx, cluster -> cluster);
    }

    /**
     * Save basic cluster.
     *
     * @param ctx Context.
     */
    private void saveBasic(RoutingContext ctx) {
        saveCluster(ctx, cluster -> {
            // TODO IGNITE-5617 UPSERT basic with full !!!

            return cluster;
        });
    }

    /**
     * @param ctx Context.
     */
    private void getConfiguration(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID clusterId = UUID.fromString(getParam(ctx, "id"));

                try(Transaction tx = txStart()) {
                    Cluster cluster = accountClusters.get(clusterId);

                    tx.commit();

                    if (cluster == null)
                        throw new IllegalStateException("Cluster not found for ID: " + clusterId);

                    TreeSet<UUID> cacheIds = clusterCachesIdx.get(clusterId);
                    Collection<Cache> caches = clusterCaches.getAll(cacheIds).values();

                    Collection<Model> models = Collections.emptyList();
                    Collection<Igfs> igfss = Collections.emptyList();

                    JsonBuilder json = new JsonBuilder()
                        .startObject()
                        .addProperty("cluster", cluster.json())
                        .addArray("caches", caches)
                        .addArray("models", models)
                        .addArray("igfss", igfss)
                        .endObject();

                    sendResult(ctx, json.buffer());
                }
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to load configuration", e);
            }
        }
    }

    /**
     *
     * @param ctx Cluster.
     */
    private void getCluster(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID clusterId = UUID.fromString(getParam(ctx, "id"));

                try(Transaction tx = txStart()) {
                    Cluster cluster = accountClusters.get(clusterId);

                    tx.commit();

                    if (cluster == null)
                        throw new IllegalStateException("Cluster not found for ID: " + clusterId);

                    sendResult(ctx, Buffer.buffer(cluster.json()));
                }
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to load cluster", e);
            }
        }
    }

    /**
     * Get cluster caches short list.
     *
     * @param ctx Context.
     */
    private void getClusterCaches(RoutingContext ctx) {
        sendResult(ctx, Buffer.buffer("[]"));
    }

    /**
     * Get cluster models short list.
     *
     * @param ctx Context.
     */
    private void getClusterModels(RoutingContext ctx) {
        sendResult(ctx, Buffer.buffer("[]"));
    }

    /**
     * Get cluster IGFSs short list.
     *
     * @param ctx Context.
     */
    private void getClusterIgfss(RoutingContext ctx) {
        sendResult(ctx, Buffer.buffer("[]"));
    }

    /**
     * Remove cluster.
     *
     * @param ctx Context.
     */
    private void remove(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID userId = getUserId(user.principal());

                TreeSet<UUID> clusterIds = getIds(ctx.getBodyAsJson(), "_id");

                if (F.isEmpty(clusterIds))
                    throw new IllegalStateException("Cluster IDs not found");

                try (Transaction tx = txStart()) {
                    Map<UUID, Cluster> clusters = accountClusters.getAll(clusterIds);

                    clusters.forEach((clusterId, cluster) -> {
                        accountClustersIdx.removeChild(userId, clusterId);
                        uniqueClusterNameIdx.remove(userId, cluster.name());
                        clusterCachesIdx.remove(clusterId);
                        clusterModelsIdx.remove(clusterId);
                        clusterIgfsIdx.remove(clusterId);
                    });

                    accountClusters.removeAll(clusterIds);

                    tx.commit();
                }

                sendResult(ctx, rowsAffected(clusterIds.size()));
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to delete cluster", e);
            }
        }
    }
}
