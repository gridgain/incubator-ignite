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

package org.apache.ignite.console.services;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Cache;
import org.apache.ignite.console.dto.Cluster;
import org.apache.ignite.console.dto.DataObject;
import org.apache.ignite.console.dto.Igfs;
import org.apache.ignite.console.dto.Model;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.console.common.Utils.diff;
import static org.apache.ignite.console.common.Utils.idsFromJson;
import static org.apache.ignite.console.common.Utils.toJsonArray;

/**
 * Service to handle notebooks.
 */
public class ConfigurationsService extends AbstractService {
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
    private final OneToManyIndex cachesIdx;

    /** */
    private final OneToManyIndex modelsIdx;

    /** */
    private final OneToManyIndex igfssIdx;

    /**
     * @param ignite Ignite.
     */
    public ConfigurationsService(Ignite ignite) {
        super(ignite);

        clustersTbl = new Table<Cluster>(ignite, "wc_account_clusters")
            .addUniqueIndex(Cluster::name, (cluster) -> "Cluster '" + cluster + "' already exits");

        cachesTbl = new Table<>(ignite, "wc_cluster_caches");
        modelsTbl = new Table<>(ignite, "wc_cluster_models");
        igfssTbl = new Table<>(ignite, "wc_cluster_igfss");

        clustersIdx = new OneToManyIndex(ignite, "wc_account_clusters_idx");
        cachesIdx = new OneToManyIndex(ignite, "wc_cluster_caches_idx");
        modelsIdx = new OneToManyIndex(ignite, "wc_cluster_models_idx");
        igfssIdx = new OneToManyIndex(ignite, "wc_cluster_igfss_idx");
    }

    /** {@inheritDoc} */
    @Override protected void initialize() {
        clustersTbl.cache();
        cachesTbl.cache();
        modelsTbl.cache();
        igfssTbl.cache();

        clustersIdx.cache();
        cachesIdx.cache();
        modelsIdx.cache();
        igfssIdx.cache();
    }

    /**
     * @param clusterId Cluster ID.
     * @return Configuration.
     */
    public JsonObject loadConfiguration(UUID clusterId) {
        try (Transaction ignored = txStart()) {
            Cluster cluster = clustersTbl.load(clusterId);

            if (cluster == null)
                throw new IllegalStateException("Cluster not found for ID: " + clusterId);

            Collection<Cache> caches = cachesTbl.loadAll(cachesIdx.load(clusterId));
            Collection<Model> models = modelsTbl.loadAll(modelsIdx.load(clusterId));
            Collection<Igfs> igfss = igfssTbl.loadAll(igfssIdx.load(clusterId));

            return new JsonObject()
                .put("cluster", new JsonObject(cluster.json()))
                .put("caches", toJsonArray(caches))
                .put("models", toJsonArray(models))
                .put("igfss", toJsonArray(igfss));
        }
    }

    /**
     * @param clusterId Cluster ID.
     * @return Cluster.
     */
    public Cluster loadCluster(UUID clusterId) {
        try (Transaction ignored = txStart()) {
            return clustersTbl.load(clusterId);
        }
    }

    /**
     * @param cluster Cluster DTO.
     * @return Short view of cluster DTO as JSON object.
     */
    protected JsonObject shortCluster(Cluster cluster) {
        UUID clusterId = cluster.id();

        int cachesCnt = cachesIdx.load(clusterId).size();
        int modelsCnt = modelsIdx.load(clusterId).size();
        int igfsCnt = igfssIdx.load(clusterId).size();

        return new JsonObject()
            .put("_id", cluster._id())
            .put("name", cluster.name())
            .put("discovery", cluster.discovery())
            .put("cachesCount", cachesCnt)
            .put("modelsCount", modelsCnt)
            .put("igfsCount", igfsCnt);
    }

    /**
     * @param userId User ID.
     * @return List of user clusters.
     */
    public JsonArray loadClusters(UUID userId) {
        try (Transaction ignored = txStart()) {
            TreeSet<UUID> clusterIds = clustersIdx.load(userId);

            Collection<Cluster> clusters = clustersTbl.loadAll(clusterIds);

            JsonArray shortList = new JsonArray();

            clusters.forEach(cluster -> shortList.add(shortCluster(cluster)));

            return shortList;
        }
    }

    /**
     * @param cacheId Cache ID.
     * @return Cache.
     */
    @Nullable public Cache loadCache(UUID cacheId) {
        try (Transaction ignored = txStart()) {
            return cachesTbl.load(cacheId);
        }
    }

    /**
     * @param clusterId Cluster ID.
     * @return Collection of cluster caches.
     */
    public Collection<? extends DataObject> loadCaches(UUID clusterId) {
        return loadList(clusterId, cachesIdx, cachesTbl);
    }

    /**
     * @param clusterId Cluster ID.
     * @return Collection of cluster models.
     */
    public Collection<? extends DataObject> loadModels(UUID clusterId) {
        return loadList(clusterId, modelsIdx, modelsTbl);
    }

    /**
     * @param clusterId Cluster ID.
     * @return Collection of cluster IGFSs.
     */
    public Collection<? extends DataObject> loadIgfss(UUID clusterId) {
        return loadList(clusterId, igfssIdx, igfssTbl);
    }

    /**
     * @param mdlId Model ID.
     */
    @Nullable public Model loadModel(UUID mdlId) {
        try (Transaction ignored = txStart()) {
             return modelsTbl.load(mdlId);
        }
    }

    /**
     * @param igfsId IGFS ID.
     * @return IGFS.
     */
    @Nullable public Igfs loadIgfs(UUID igfsId) {
        try (Transaction ignored = txStart()) {
            return igfssTbl.load(igfsId);
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
     * @return Saved cluster.
     */
    private Cluster saveCluster(UUID userId, JsonObject json) {
        ensureTx();

        JsonObject jsonCluster = json.getJsonObject("cluster");

        Cluster newCluster = Cluster.fromJson(jsonCluster);

        UUID clusterId = newCluster.id();

        Cluster oldCluster = clustersTbl.load(clusterId);

        if (oldCluster != null) {
            JsonObject oldClusterJson = new JsonObject(oldCluster.json());

            removedInCluster(cachesTbl, cachesIdx, clusterId, oldClusterJson, jsonCluster, "caches");
            removedInCluster(modelsTbl, modelsIdx, clusterId, oldClusterJson, jsonCluster, "models");
            removedInCluster(igfssTbl, igfssIdx, clusterId, oldClusterJson, jsonCluster, "igfss");
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

            oldCaches.forEach(oldCache -> {
                Cache newCache = caches.get(oldCache.id());

                if (newCache != null) {
                    JsonObject oldJson = new JsonObject(oldCache.json());
                    JsonObject newJson = new JsonObject(newCache.json());

                    newCache.json(oldJson.mergeIn(newJson).encode());
                }
            });
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
     * Save full cluster.
     *
     * @param userId User ID.
     * @param json JSON data.
     */
    public void saveAdvancedCluster(UUID userId, JsonObject json) {
        try (Transaction tx = txStart()) {
            Cluster cluster = saveCluster(userId, json);

            saveCaches(cluster, json, false);
            saveModels(cluster, json);
            saveIgfss(cluster, json);

            tx.commit();
        }
    }

    /**
     * Save basic cluster.
     *
     * @param userId User ID.
     * @param json JSON data.
     */
    public void saveBasicCluster(UUID userId, JsonObject json) {
        try (Transaction tx = txStart()) {
            Cluster cluster = saveCluster(userId, json);

            saveCaches(cluster, json, true);

            tx.commit();
        }
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
     * @param userId User ID.
     */
    public void deleteClusters(UUID userId, TreeSet<UUID> clusterIds) {
        try (Transaction tx = txStart()) {
            Collection<Cluster> clusters = clustersTbl.loadAll(clusterIds);

            clusters.forEach(cluster -> {
                UUID clusterId = cluster.id();

                clustersIdx.remove(userId, clusterId);

                removeClusterObjects(clusterId, cachesTbl, cachesIdx);
                removeClusterObjects(clusterId, modelsTbl, modelsIdx);
                removeClusterObjects(clusterId, igfssTbl, igfssIdx);
            });

            clustersTbl.deleteAll(clusterIds);

            tx.commit();
        }
    }
}
