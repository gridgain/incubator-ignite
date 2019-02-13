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

import java.util.ArrayList;
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
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.console.common.Utils;
import org.apache.ignite.console.db.CacheHolder;
import org.apache.ignite.console.dto.Cache;
import org.apache.ignite.console.dto.Cluster;
import org.apache.ignite.console.dto.Igfs;
import org.apache.ignite.console.dto.JsonBuilder;
import org.apache.ignite.console.dto.Model;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.UniqueIndex;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.Transaction;

/**
 * Router to handle REST API for configurations.
 */
public class ConfigurationsRouter extends AbstractRouter {
    /** */
    private final CacheHolder<UUID, Cluster> clustersTbl;

    /** */
    private final CacheHolder<UUID, Cache> cachesTbl;

    /** */
    private final CacheHolder<UUID, Model> modelsTbl;

    /** */
    private final CacheHolder<UUID, Igfs> igfssTbl;

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

        clustersTbl = new CacheHolder<>(ignite, "wc_account_clusters");
        cachesTbl = new CacheHolder<>(ignite, "wc_cluster_caches");
        modelsTbl = new CacheHolder<>(ignite, "wc_cluster_models");
        igfssTbl = new CacheHolder<>(ignite, "wc_cluster_igfss");
        accountClustersIdx = new OneToManyIndex(ignite, "wc_account_clusters_idx");
        uniqueClusterNameIdx = new UniqueIndex(ignite, "wc_unique_cluster_name_idx", "Cluster '%s' already exits");
        clusterCachesIdx = new OneToManyIndex(ignite, "wc_cluster_caches_idx");
        clusterModelsIdx = new OneToManyIndex(ignite, "wc_cluster_models_idx");
        clusterIgfsIdx = new OneToManyIndex(ignite, "wc_cluster_igfss_idx");
    }

    /** {@inheritDoc} */
    @Override protected void initializeCaches() {
        clustersTbl.prepare();
        cachesTbl.prepare();
        modelsTbl.prepare();
        igfssTbl.prepare();

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

        router.put("/api/v1/configuration/clusters").handler(this::saveAdvancedCluster);
        router.put("/api/v1/configuration/clusters/basic").handler(this::saveBasicCluster);
        router.post("/api/v1/configuration/clusters/remove").handler(this::removeCluster);

        router.get("/api/v1/configuration/caches/:id").handler(this::getCache);
        router.get("/api/v1/configuration/domains/:id").handler(this::getModel);
        router.get("/api/v1/configuration/igfs/:id").handler(this::getIgfs);
    }

    /**
     *
     * @param parentId Parent ID.
     * @param children Map with children.
     * @return Number of children
     */
    private int countChildren(UUID parentId, Map<UUID, TreeSet<UUID>> children) {
        TreeSet<UUID> res = children.get(parentId);

        return F.isEmpty(res) ? 0 : res.size();
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

                try (Transaction tx = txStart()) {
                    TreeSet<UUID> clusterIds = accountClustersIdx.getChildren(userId);

                    Collection<Cluster> clusters = clustersTbl.getAll(clusterIds).values();
                    Map<UUID, TreeSet<UUID>>caches = clusterCachesIdx.getAll(clusterIds);
                    Map<UUID, TreeSet<UUID>>models = clusterModelsIdx.getAll(clusterIds);
                    Map<UUID, TreeSet<UUID>>igfss = clusterIgfsIdx.getAll(clusterIds);

                    tx.commit();

                    JsonArray shortList = new JsonArray();

                    clusters.forEach(cluster -> {
                        UUID clusterId = cluster.id();

                        int cachesCnt = countChildren(clusterId, caches);
                        int modelsCnt = countChildren(clusterId, models);
                        int igfsCnt = countChildren(clusterId, igfss);

                        shortList.add(new JsonObject()
                            .put("_id", cluster._id())
                            .put("name", cluster.name())
                            .put("discovery", cluster.discovery())
                            .put("cachesCount", cachesCnt)
                            .put("modelsCount", modelsCnt)
                            .put("igfsCount", igfsCnt));
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
     * @param rawData JSON data.
     * @return Cluster DTO.
     */
    private Cluster jsonToCluster(JsonObject rawData) {
        JsonObject rawCluster = rawData.getJsonObject("cluster");

        UUID clusterId = getId(rawCluster);

        if (clusterId == null)
            throw new IllegalStateException("Cluster ID not found");

        String name = rawCluster.getString("name");

        if (F.isEmpty(name))
            throw new IllegalStateException("Cluster name is empty");

        String discovery = rawCluster.getJsonObject("discovery").getString("kind");

        if (F.isEmpty(discovery))
            throw new IllegalStateException("Cluster discovery not found");

        return new Cluster(clusterId, null, name, discovery, rawCluster.encode());
    }

    /**
     *
     * @param rawData JSON data.
     * @return Cache DTOs.
     */
    private Collection<Cache> jsonToCaches(JsonObject rawData) {
        JsonArray rawCaches = rawData.getJsonArray("caches");

        int sz = rawCaches.size();

        Collection<Cache> res = new ArrayList<>(sz);


        for (int i = 0; i < sz; i++) {
            JsonObject json = rawCaches.getJsonObject(i);

            res.add(new Cache(
                getId(json),
                null,
                json.getString("name"),
                CacheMode.valueOf(json.getString("cacheMode", "PARTITIONED")),
                CacheAtomicityMode.valueOf(json.getString("atomicyMode", "ATOMIC")),
                json.getInteger("backups", 0),
                json.encode()));
        }

        return res;
    }

    /**
     *
     * @param rawData JSON data.
     * @return Model DTOs.
     */
    private Collection<Model> jsonToModels(JsonObject rawData) {
        JsonArray rawModels = rawData.getJsonArray("models");

        return Collections.emptyList();
    }

    /**
     *
     * @param rawData JSON data.
     * @return IGFS DTOs.
     */
    private Collection<Igfs> jsonToIgfss(JsonObject rawData) {
        JsonArray rawIgfss = rawData.getJsonArray("igfss");

        return Collections.emptyList();
    }

    /**
     * @param userId User ID.
     * @param cluster Cluster.
     */
    private void saveCluster(UUID userId, Cluster cluster) {
        uniqueClusterNameIdx.checkUnique(userId, cluster.name(), cluster, cluster.name());

        accountClustersIdx.putChild(userId, cluster.id());

        clustersTbl.put(cluster.id(), cluster);
    }

    /**
     * @param cluster Cluster.
     * @param clusterCaches Caches.
     */
    private void saveCaches(Cluster cluster, Collection<Cache> clusterCaches) {
        UUID clusterId = cluster.id();

        TreeSet<UUID> newCacheIds = Utils.getIds(clusterCaches);
        TreeSet<UUID> oldCacheIds = clusterCachesIdx.getChildren(clusterId);

        cachesTbl.removeAll(Utils.diff(oldCacheIds, newCacheIds));

        clusterCachesIdx.put(clusterId, newCacheIds);

        for (Cache cache : clusterCaches)
            cachesTbl.put(cache.id(), cache);
    }

    /**
     * @param cluster Cluster.
     * @param clusterModels Models.
     */
    private void saveModels(Cluster cluster, Collection<Model> clusterModels) {
        // TODO IGNITE-5617 implements.
    }

    /**
     * @param cluster Cluster.
     * @param clusterIgfss IGFSs.
     */
    private void saveIgfss(Cluster cluster, Collection<Igfs> clusterIgfss) {
        // TODO IGNITE-5617 implements.
    }

    /**
     * @param ctx Context.
     * @param merge Merge function.
     */
    private void saveCluster(RoutingContext ctx, Function<Cluster, Cluster> merge) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID userId = getUserId(user.principal());

                JsonObject rawData = ctx.getBodyAsJson();

                // TODO IGNITE-5617 rawData = Schemas.sanitize(Cluster.class, rawData); ?

                Cluster cluster = jsonToCluster(rawData);
                Collection<Cache> clusterCaches = jsonToCaches(rawData);
                Collection<Model> clusterModels = jsonToModels(rawData);
                Collection<Igfs> clusterIgfss = jsonToIgfss(rawData);

                merge.apply(cluster);

                try (Transaction tx = txStart()) {
                    saveCluster(userId, cluster);
                    saveCaches(cluster, clusterCaches);
                    saveModels(cluster, clusterModels);
                    saveIgfss(cluster, clusterIgfss);

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
    private void saveAdvancedCluster(RoutingContext ctx) {
        saveCluster(ctx, cluster -> cluster);
    }

    /**
     * Save basic cluster.
     *
     * @param ctx Context.
     */
    private void saveBasicCluster(RoutingContext ctx) {
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

                try (Transaction tx = txStart()) {
                    Cluster cluster = clustersTbl.get(clusterId);

                    tx.commit();

                    if (cluster == null)
                        throw new IllegalStateException("Cluster not found for ID: " + clusterId);

                    TreeSet<UUID> cacheIds = clusterCachesIdx.get(clusterId);
                    Collection<Cache> caches = cachesTbl.getAll(cacheIds).values();

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
     * @param ctx Cluster.
     */
    private void getCluster(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID clusterId = UUID.fromString(getParam(ctx, "id"));

                try (Transaction tx = txStart()) {
                    Cluster cluster = clustersTbl.get(clusterId);

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
        try {
            UUID clusterId = UUID.fromString(getParam(ctx, "id"));

            try (Transaction tx = txStart()) {
                TreeSet<UUID> cacheIds = clusterCachesIdx.getChildren(clusterId);

                Map<UUID, Cache> caches = cachesTbl.getAll(cacheIds);

                tx.commit();

                sendResult(ctx, new JsonBuilder().addArray(caches).buffer());
            }
        }
        catch (Throwable e) {
            sendError(ctx, "Failed to get cluster caches", e);
        }
    }

    /**
     * Get cluster models short list.
     *
     * @param ctx Context.
     */
    private void getClusterModels(RoutingContext ctx) {
        try {
            sendResult(ctx, Buffer.buffer("[]"));
        }
        catch (Throwable e) {
            sendError(ctx, "Failed to get cluster models", e);
        }
    }

    /**
     * Get cluster IGFSs short list.
     *
     * @param ctx Context.
     */
    private void getClusterIgfss(RoutingContext ctx) {
        try {
            sendResult(ctx, Buffer.buffer("[]"));
        }
        catch (Throwable e) {
            sendError(ctx, "Failed to get cluster IGFSs", e);
        }
    }

    /**
     * @param clusterId Cluster ID.
     */
    private void removeClusterCaches(UUID clusterId) {
        TreeSet<UUID> cacheIds = clusterCachesIdx.getAndRemove(clusterId);

        cachesTbl.removeAll(cacheIds);
    }

    /**
     * @param clusterId Cluster ID.
     */
    private void removeClusterModels(UUID clusterId) {
        TreeSet<UUID> cacheIds = clusterModelsIdx.getAndRemove(clusterId);

        modelsTbl.removeAll(cacheIds);
    }

    /**
     * @param clusterId Cluster ID.
     */
    private void removeClusterIgfss(UUID clusterId) {
        TreeSet<UUID> cacheIds = clusterIgfsIdx.getAndRemove(clusterId);

        igfssTbl.removeAll(cacheIds);
    }

    /**
     * Remove cluster.
     *
     * @param ctx Context.
     */
    private void removeCluster(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID userId = getUserId(user.principal());

                TreeSet<UUID> clusterIds = Utils.getIds(ctx.getBodyAsJson(), "_id");

                if (F.isEmpty(clusterIds))
                    throw new IllegalStateException("Cluster IDs not found");

                try (Transaction tx = txStart()) {
                    Map<UUID, Cluster> clusters = clustersTbl.getAll(clusterIds);

                    clusters.forEach((clusterId, cluster) -> {
                        accountClustersIdx.removeChild(userId, clusterId);
                        uniqueClusterNameIdx.removeUniqueKey(userId, cluster.name());

                        removeClusterCaches(clusterId);
                        removeClusterModels(clusterId);
                        removeClusterIgfss(clusterId);
                    });

                    clustersTbl.removeAll(clusterIds);

                    tx.commit();
                }

                sendResult(ctx, rowsAffected(clusterIds.size()));
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to delete cluster", e);
            }
        }
    }

    /**
     * @param ctx Context.
     */
    private void getCache(RoutingContext ctx) {
        try {
            UUID cacheId = UUID.fromString(getParam(ctx, "id"));

            try (Transaction tx = txStart()) {
                Cache cache = cachesTbl.get(cacheId);

                tx.commit();

                sendResult(ctx, cache.json());
            }
        }
        catch (Throwable e) {
            sendError(ctx, "Failed to get cache", e);
        }
    }

    /**
     * @param ctx Context.
     */
    private void getModel(RoutingContext ctx) {
        try {
            throw new IllegalStateException("Not implemented yet");
        }
        catch (Throwable e) {
            sendError(ctx, "Failed to get model", e);
        }
    }

    /**
     * @param ctx Context.
     */
    private void getIgfs(RoutingContext ctx) {
        try {
            throw new IllegalStateException("Not implemented yet");
        }
        catch (Throwable e) {
            sendError(ctx, "Failed to get IGFS", e);
        }
    }
}

