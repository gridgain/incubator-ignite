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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.db.UniqueIndex;
import org.apache.ignite.console.dto.Cache;
import org.apache.ignite.console.dto.Cluster;
import org.apache.ignite.console.dto.DataObject;
import org.apache.ignite.console.dto.Igfs;
import org.apache.ignite.console.dto.JsonBuilder;
import org.apache.ignite.console.dto.Model;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.console.common.Utils.diff;
import static org.apache.ignite.console.common.Utils.idsFromJson;

/**
 * Router to handle REST API for configurations.
 */
public class ConfigurationsRouter extends AbstractRouter {
    /** */
    private final Table<Cluster> clustersTbl;

    /** */
    private final Table<Cache> cachesTbl;

    /** */
    private final Table<Model> modelsTbl;

    /** */
    private final Table<Igfs> igfssTbl;

    /** */
    private final OneToManyIndex clustersIdx;

    /** */
    private final UniqueIndex clusterNameIdx;

    /** */
    private final OneToManyIndex cachesIdx;

    /** */
    private final OneToManyIndex modelsIdx;

    /** */
    private final OneToManyIndex igfssIdx;

    /**
     * @param ignite Ignite.
     */
    public ConfigurationsRouter(Ignite ignite) {
        super(ignite);

        clustersTbl = new Table<>(ignite, "wc_account_clusters");
        cachesTbl = new Table<>(ignite, "wc_cluster_caches");
        modelsTbl = new Table<>(ignite, "wc_cluster_models");
        igfssTbl = new Table<>(ignite, "wc_cluster_igfss");
        clustersIdx = new OneToManyIndex(ignite, "wc_account_clusters_idx");
        clusterNameIdx = new UniqueIndex(ignite, "wc_unique_cluster_name_idx", "Cluster '%s' already exits");
        cachesIdx = new OneToManyIndex(ignite, "wc_cluster_caches_idx");
        modelsIdx = new OneToManyIndex(ignite, "wc_cluster_models_idx");
        igfssIdx = new OneToManyIndex(ignite, "wc_cluster_igfss_idx");
    }

    /** {@inheritDoc} */
    @Override protected void initializeCaches() {
        clustersTbl.prepare();
        cachesTbl.prepare();
        modelsTbl.prepare();
        igfssTbl.prepare();

        clustersIdx.prepare();
        clusterNameIdx.prepare();
        cachesIdx.prepare();
        modelsIdx.prepare();
        igfssIdx.prepare();
    }

    /** {@inheritDoc} */
    @Override public void install(Router router) {
        router.get("/api/v1/configuration/:id").handler(this::loadConfiguration);
        router.get("/api/v1/configuration/clusters").handler(this::loadClustersShortList);
        router.get("/api/v1/configuration/clusters/:id").handler(this::loadCluster);
        router.get("/api/v1/configuration/clusters/:id/caches").handler(this::loadCachesShortList);
        router.get("/api/v1/configuration/clusters/:id/models").handler(this::loadModelsShortList);
        router.get("/api/v1/configuration/clusters/:id/igfss").handler(this::loadIgfssShortList);

        router.put("/api/v1/configuration/clusters").handler(this::saveAdvancedCluster);
        router.put("/api/v1/configuration/clusters/basic").handler(this::saveBasicCluster);
        router.post("/api/v1/configuration/clusters/remove").handler(this::deleteClusters);

        router.get("/api/v1/configuration/caches/:id").handler(this::loadCache);
        router.get("/api/v1/configuration/domains/:id").handler(this::loadModel);
        router.get("/api/v1/configuration/igfs/:id").handler(this::loadIgfs);
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
    private void loadClustersShortList(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID userId = getUserId(user.principal());

                try (Transaction tx = txStart()) {
                    TreeSet<UUID> clusterIds = clustersIdx.load(userId);

                    Collection<Cluster> clusters = clustersTbl.loadAll(clusterIds);
                    Map<UUID, TreeSet<UUID>>caches = cachesIdx.loadAll(clusterIds);
                    Map<UUID, TreeSet<UUID>>models = modelsIdx.loadAll(clusterIds);
                    Map<UUID, TreeSet<UUID>>igfss = igfssIdx.loadAll(clusterIds);

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
     * Handle objects that was deleted from cluster.
     *
     * @param oldCluster Old cluster JSON.
     * @param newCluster New cluster JSON.
     * @param fld Field name that holds IDs to check for deletion.
     */
    private void removedInCluster(
        Table<? extends DataObject> tbl,
        OneToManyIndex idx,
        UUID clusterId,
        JsonObject oldCluster,
        JsonObject newCluster,
        String fld
    ) {
        TreeSet<UUID> oldIds = idsFromJson(oldCluster, fld);
        TreeSet<UUID> newIds = idsFromJson(newCluster, fld);

        TreeSet<UUID> deletedIds = diff(oldIds, newIds);

        if (!F.isEmpty(deletedIds)) {
            tbl.deleteAll(deletedIds);
            idx.removeAll(clusterId, deletedIds);
        }
    }

    /**
     * @param userId User ID.
     * @param json JSON data.
     * @param basic {@code true} in case of saving basic cluster.
     * @return Saved cluster.
     */
    private Cluster saveCluster(UUID userId, JsonObject json, boolean basic) {
        ensureTx();

        JsonObject jsonCluster = json.getJsonObject("cluster");

        Cluster newCluster = Cluster.fromJson(jsonCluster);

        clusterNameIdx.checkUnique(userId, newCluster.name(), newCluster, newCluster.name());

        UUID clusterId = newCluster.id();

        Cluster oldCluster = clustersTbl.load(clusterId);

        if (oldCluster != null) {
            JsonObject oldClusterJson = new JsonObject(oldCluster.json());

            removedInCluster(cachesTbl, cachesIdx, clusterId, oldClusterJson, jsonCluster, "caches");

            if (basic) {
                oldClusterJson.mergeIn(jsonCluster);

                newCluster.json(oldClusterJson.encode());
            }
            else {
                removedInCluster(modelsTbl, modelsIdx, clusterId, oldClusterJson, jsonCluster, "models");
                removedInCluster(igfssTbl, igfssIdx, clusterId, oldClusterJson, jsonCluster, "igfss");
            }
        }

        clustersIdx.add(userId, clusterId);

        clustersTbl.save(newCluster);

        return newCluster;
    }

    /**
     * @param cluster Cluster.
     * @param json JSON data.
     * @param basic {@code true} in case of saving basic cluster.
     */
    private void saveCaches(Cluster cluster, JsonObject json, boolean basic) {
        JsonArray jsonCaches = json.getJsonArray("caches");

        if (F.isEmpty(jsonCaches))
            return;

        int sz = jsonCaches.size();

        Map<UUID, Cache> caches = new HashMap<>(sz);

        for (int i = 0; i < sz; i++) {
            Cache cache = Cache.fromJson(jsonCaches.getJsonObject(i));

            caches.put(cache.id(), cache);
        }

        if (basic) {
            Collection<Cache> oldCaches = cachesTbl.loadAll(new TreeSet<>(caches.keySet()));

            if (!F.isEmpty(oldCaches)) {
                // TODO IGNITE-5617 Merge with new one!!!
            }
        }

        cachesIdx.addAll(cluster.id(), caches.keySet());

        cachesTbl.saveAll(caches);
    }

    /**
     * @param cluster Cluster.
     * @param json JSON data.
     */
    private void saveModels(Cluster cluster, JsonObject json) {
        JsonArray jsonModels = json.getJsonArray("models");

        if (F.isEmpty(jsonModels))
            return;

        int sz = jsonModels.size();

        Map<UUID, Model> mdls = new HashMap<>(sz);

        for (int i = 0; i < sz; i++) {
            Model mdl = Model.fromJson(jsonModels.getJsonObject(i));

            mdls.put(mdl.id(), mdl);
        }

        modelsIdx.addAll(cluster.id(), mdls.keySet());

        modelsTbl.saveAll(mdls);
    }

    /**
     * @param cluster Cluster.
     * @param json JSON data.
     */
    private void saveIgfss(Cluster cluster, JsonObject json) {
        JsonArray jsonIgfss = json.getJsonArray("igfss");

        if (F.isEmpty(jsonIgfss))
            return;

        int sz = jsonIgfss.size();

        Map<UUID, Igfs> igfss = new HashMap<>(sz);

        for (int i = 0; i < sz; i++) {
            Igfs igfs = Igfs.fromJson(jsonIgfss.getJsonObject(i));

            igfss.put(igfs.id(), igfs);
        }

        igfssIdx.addAll(cluster.id(), igfss.keySet());

        igfssTbl.saveAll(igfss);
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

                try (Transaction tx = txStart()) {
                    Cluster cluster = saveCluster(userId, json, false);

                    saveCaches(cluster, json, false);
                    saveModels(cluster, json);
                    saveIgfss(cluster, json);

                    tx.commit();
                }

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

                try (Transaction tx = txStart()) {
                    Cluster cluster = saveCluster(userId, json, true);

                    saveCaches(cluster, json, true);

                    tx.commit();
                }

                sendResult(ctx, rowsAffected(1));
            }
            catch (Throwable e) {
                sendError(ctx, "Failed to save cluster", e);
            }
        }
    }

    /**
     * @param ctx Context.
     */
    private void loadConfiguration(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID clusterId = UUID.fromString(requestParam(ctx, "id"));

                try (Transaction tx = txStart()) {
                    Cluster cluster = clustersTbl.load(clusterId);

                    tx.commit();

                    if (cluster == null)
                        throw new IllegalStateException("Cluster not found for ID: " + clusterId);

                    TreeSet<UUID> cacheIds = cachesIdx.load(clusterId);
                    Collection<Cache> caches = cachesTbl.loadAll(cacheIds);

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
    private void loadCluster(RoutingContext ctx) {
        User user = checkUser(ctx);

        if (user != null) {
            try {
                UUID clusterId = UUID.fromString(requestParam(ctx, "id"));

                try (Transaction tx = txStart()) {
                    Cluster cluster = clustersTbl.load(clusterId);

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
     * Load cluster caches short list.
     *
     * @param ctx Context.
     */
    private void loadCachesShortList(RoutingContext ctx) {
        loadShortList(ctx, cachesTbl, cachesIdx, "Failed to load cluster caches");
    }

    /**
     * Load cluster models short list.
     *
     * @param ctx Context.
     */
    private void loadModelsShortList(RoutingContext ctx) {
        loadShortList(ctx, modelsTbl, modelsIdx, "Failed to load cluster models");
    }

    /**
     * Get cluster IGFSs short list.
     *
     * @param ctx Context.
     */
    private void loadIgfssShortList(RoutingContext ctx) {
        loadShortList(ctx, igfssTbl, igfssIdx, "Failed to load cluster IGFSs");
    }

    /**
     * @param clusterId Cluster ID.
     */
    private void removeClusterObjects(UUID clusterId, Table<? extends DataObject> tbl, OneToManyIndex idx) {
        TreeSet<UUID> ids = idx.delete(clusterId);

        tbl.deleteAll(ids);
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

                try (Transaction tx = txStart()) {
                    Collection<Cluster> clusters = clustersTbl.loadAll(clusterIds);

                    clusters.forEach(cluster -> {
                        UUID clusterId = cluster.id();

                        clustersIdx.remove(userId, clusterId);
                        clusterNameIdx.removeUniqueKey(userId, cluster.name());

                        removeClusterObjects(clusterId, cachesTbl, cachesIdx);
                        removeClusterObjects(clusterId, modelsTbl, modelsIdx);
                        removeClusterObjects(clusterId, igfssTbl, igfssIdx);
                    });

                    clustersTbl.deleteAll(clusterIds);

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
    private void loadCache(RoutingContext ctx) {
        try {
            UUID cacheId = UUID.fromString(requestParam(ctx, "id"));

            try (Transaction tx = txStart()) {
                Cache cache = cachesTbl.load(cacheId);

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
    private void loadModel(RoutingContext ctx) {
        try {
            UUID mdlId = UUID.fromString(requestParam(ctx, "id"));

            try (Transaction tx = txStart()) {
                Model mdl = modelsTbl.load(mdlId);

                tx.commit();

                sendResult(ctx, mdl.json());
            }
        }
        catch (Throwable e) {
            sendError(ctx, "Failed to get model", e);
        }
    }

    /**
     * @param ctx Context.
     */
    private void loadIgfs(RoutingContext ctx) {
        try {
            UUID igfsId = UUID.fromString(requestParam(ctx, "id"));

            try (Transaction tx = txStart()) {
                Igfs igfs = igfssTbl.load(igfsId);

                tx.commit();

                sendResult(ctx, igfs.json());
            }
        }
        catch (Throwable e) {
            sendError(ctx, "Failed to get IGFS", e);
        }
    }
}

