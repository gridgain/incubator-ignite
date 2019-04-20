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

package org.apache.ignite.console.repositories;

import java.util.Collection;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Cache;
import org.apache.ignite.console.dto.Cluster;
import org.apache.ignite.console.dto.DataObject;
import org.apache.ignite.console.dto.Igfs;
import org.apache.ignite.console.dto.Model;
import org.apache.ignite.console.tx.TransactionManager;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.Transaction;
import org.springframework.stereotype.Repository;

import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.console.common.Utils.diff;
import static org.apache.ignite.console.common.Utils.idsFromJson;
import static org.apache.ignite.console.common.Utils.toJsonArray;
import static org.apache.ignite.console.json.JsonUtils.asJson;
import static org.apache.ignite.console.json.JsonUtils.fromJson;
import static org.apache.ignite.console.json.JsonUtils.toJson;

/**
 * Repository to work with configurations.
 */
@Repository
public class ConfigurationsRepository {
    /** */
    private final TransactionManager txMgr;

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
     * @param txMgr Transactions manager.
     */
    public ConfigurationsRepository(Ignite ignite, TransactionManager txMgr) {
        this.txMgr = txMgr;

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

    /**
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     * @return Configuration in JSON format.
     */
    public JsonObject loadConfiguration(UUID accId, UUID clusterId) {
        try (Transaction ignored = txMgr.txStart()) {
            Cluster cluster = clustersTbl.load(clusterId);

            if (cluster == null)
                throw new IllegalStateException("Cluster not found for ID: " + clusterId);

            checkOwner(accId, cluster);

            Collection<Cache> caches = cachesTbl.loadAll(cachesIdx.load(clusterId));
            Collection<Model> models = modelsTbl.loadAll(modelsIdx.load(clusterId));
            Collection<Igfs> igfss = igfssTbl.loadAll(igfssIdx.load(clusterId));

            return new JsonObject()
                .add("cluster", fromJson(cluster.json()))
                .add("caches", toJsonArray(caches))
                .add("models", toJsonArray(models))
                .add("igfss", toJsonArray(igfss));
        }
    }

    /**
     * @param cluster Cluster DTO.
     * @return Short view of cluster DTO as JSON object.
     */
    protected JsonObject shortCluster(Cluster cluster) {
        UUID clusterId = cluster.getId();

        int cachesCnt = cachesIdx.load(clusterId).size();
        int modelsCnt = modelsIdx.load(clusterId).size();
        int igfsCnt = igfssIdx.load(clusterId).size();

        return new JsonObject()
            .add("id", cluster.getId())
            .add("name", cluster.name())
            .add("discovery", cluster.discovery())
            .add("cachesCount", cachesCnt)
            .add("modelsCount", modelsCnt)
            .add("igfsCount", igfsCnt);
    }

    /**
     * @param accId Account ID.
     * @return List of user clusters.
     */
    public JsonArray loadClusters(UUID accId) {
        try (Transaction ignored = txMgr.txStart()) {
            TreeSet<UUID> clusterIds = clustersIdx.load(accId);

            Collection<Cluster> clusters = clustersTbl.loadAll(clusterIds);

            JsonArray shortList = new JsonArray();

            clusters.forEach(cluster -> shortList.add(shortCluster(cluster)));

            return shortList;
        }
    }

    /**
     *
     * @param accId Account ID.
     * @param id Object ID.
     * @param tbl Table with objects.
     * @param objName Object name.
     * @return DTO.
     */
    private <T extends DataObject> T loadObject(UUID accId, UUID id, Table<? extends DataObject> tbl, String objName) {
        T obj;

        try (Transaction ignored = txMgr.txStart()) {
            obj = (T)tbl.load(id);
        }

        if (obj == null)
            throw new IllegalStateException(objName + " not found for ID: " + id);

        checkOwner(accId, obj);

        return obj;
    }

    /**
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     * @return Cluster.
     */
    public Cluster loadCluster(UUID accId, UUID clusterId) {
        return loadObject(accId, clusterId, clustersTbl, "Cluster");
    }

    /**
     * @param accId Account ID.
     * @param cacheId Cache ID.
     * @return Cache.
     */
    public Cache loadCache(UUID accId, UUID cacheId) {
        return loadObject(accId, cacheId, cachesTbl, "Cache");
    }

    /**
     * @param accId Account ID.
     * @param mdlId Model ID.
     * @return Model.
     */
    public Model loadModel(UUID accId, UUID mdlId) {
        return loadObject(accId, mdlId, modelsTbl, "Model");
    }

    /**
     * @param accId Account ID.
     * @param igfsId IGFS ID.
     * @return IGFS.
     */
    public Igfs loadIgfs(UUID accId, UUID igfsId) {
        return loadObject(accId, igfsId, modelsTbl, "IGFS");
    }

    /**
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     * @return Collection of cluster caches.
     */
    public Collection<Cache> loadCaches(UUID accId, UUID clusterId) {
        return loadList(accId, clusterId, cachesIdx, cachesTbl);
    }

    /**
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     * @return Collection of cluster models.
     */
    public Collection<Model> loadModels(UUID accId, UUID clusterId) {
        return loadList(accId, clusterId, modelsIdx, modelsTbl);
    }

    /**
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     * @return Collection of cluster IGFSs.
     */
    public Collection<Igfs> loadIgfss(UUID accId, UUID clusterId) {
        return loadList(accId, clusterId, igfssIdx, igfssTbl);
    }

    /**
     * Handle objects that was deleted from cluster.
     *
     * @param accId Account ID.
     * @param tbl Table with DTOs.
     * @param idx Foreign key.
     * @param clusterId Cluster ID.
     * @param oldCluster Old cluster JSON.
     * @param newCluster New cluster JSON.
     * @param fld Field name that holds IDs to check for deletion.
     */
    private void removedInCluster(
        UUID accId,
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
            checkOwner(accId, tbl.loadAll(deletedIds));

            tbl.deleteAll(deletedIds);
            idx.removeAll(clusterId, deletedIds);
        }
    }

    /**
     * @param accId Account ID.
     * @param changedItems Items to save.
     * @return Saved cluster.
     */
    private Cluster saveCluster(UUID accId, JsonObject changedItems) {
        JsonObject jsonCluster = changedItems.getJsonObject("cluster");

        Cluster newCluster = Cluster.fromJson(jsonCluster);

        registerOwner(accId, newCluster);

        UUID clusterId = newCluster.getId();

        Cluster oldCluster = clustersTbl.load(clusterId);

        if (oldCluster != null) {
            JsonObject oldClusterJson = fromJson(oldCluster.json());

            removedInCluster(accId, cachesTbl, cachesIdx, clusterId, oldClusterJson, jsonCluster, "caches");
            removedInCluster(accId, modelsTbl, modelsIdx, clusterId, oldClusterJson, jsonCluster, "models");
            removedInCluster(accId, igfssTbl, igfssIdx, clusterId, oldClusterJson, jsonCluster, "igfss");
        }

        clustersIdx.add(accId, clusterId);

        clustersTbl.save(newCluster);

        return newCluster;
    }

    /**
     * @param cluster Cluster.
     * @param json JSON data.
     * @param basic {@code true} in case of saving basic cluster.
     */
    private void saveCaches(UUID accId, Cluster cluster, JsonObject json, boolean basic) {
        JsonArray jsonCaches = json.getJsonArray("caches");

        if (F.isEmpty(jsonCaches))
            return;

        Map<UUID, Cache> caches = jsonCaches
            .stream()
            .map(item -> Cache.fromJson(asJson(item)))
            .collect(toMap(Cache::getId, c -> c));

        caches.values().forEach(c -> registerOwner(accId, c));

        if (basic) {
            Collection<Cache> oldCaches = cachesTbl.loadAll(new TreeSet<>(caches.keySet()));

            oldCaches.forEach(oldCache -> {
                Cache newCache = caches.get(oldCache.getId());

                if (newCache != null) {
                    JsonObject oldJson = fromJson(oldCache.json());
                    JsonObject newJson = fromJson(newCache.json());

                    newCache.json(toJson(oldJson.mergeIn(newJson)));
                }
            });
        }

        cachesIdx.addAll(cluster.getId(), caches.keySet());

        cachesTbl.saveAll(caches);
    }

    /**
     * @param cluster Cluster.
     * @param json JSON data.
     */
    private void saveModels(UUID accId, Cluster cluster, JsonObject json) {
        JsonArray jsonModels = json.getJsonArray("models");

        if (F.isEmpty(jsonModels))
            return;

        Map<UUID, Model> mdls = jsonModels
            .stream()
            .map(item -> Model.fromJson(asJson(item)))
            .collect(toMap(Model::getId, m -> m));

        mdls.values().forEach(m -> registerOwner(accId, m));

        modelsIdx.addAll(cluster.getId(), mdls.keySet());

        modelsTbl.saveAll(mdls);
    }

    /**
     * @param cluster Cluster.
     * @param json JSON data.
     */
    private void saveIgfss(UUID accId, Cluster cluster, JsonObject json) {
        JsonArray jsonIgfss = json.getJsonArray("igfss");

        if (F.isEmpty(jsonIgfss))
            return;

        Map<UUID, Igfs> igfss = jsonIgfss
            .stream()
            .map(item -> Igfs.fromJson(asJson(item)))
            .collect(toMap(Igfs::getId, i -> i));

        igfss.values().forEach(igfs -> registerOwner(accId, igfs));

        igfssIdx.addAll(cluster.getId(), igfss.keySet());

        igfssTbl.saveAll(igfss);
    }

    /**
     * Save full cluster.
     *
     * @param accId Account ID.
     * @param json Configuration in JSON format.
     */
    public void saveAdvancedCluster(UUID accId, JsonObject json) {
        try (Transaction tx = txMgr.txStart()) {
            Cluster cluster = saveCluster(accId, json);

            saveCaches(accId, cluster, json, false);
            saveModels(accId, cluster, json);
            saveIgfss(accId, cluster, json);

            tx.commit();
        }
    }

    /**
     * Save basic cluster.
     *
     * @param accId Account ID.
     * @param json Configuration in JSON format.
     */
    public void saveBasicCluster(UUID accId, JsonObject json) {
        try (Transaction tx = txMgr.txStart()) {
            Cluster cluster = saveCluster(accId, json);

            saveCaches(accId, cluster, json, true);

            tx.commit();
        }
    }

    /**
     * Delete objects that relates to cluster.
     *
     * @param clusterId Cluster ID.
     * @param fkIdx Foreign key.
     * @param tbl Table with children.
     */
    private void deleteClusterObjects(UUID clusterId, OneToManyIndex fkIdx, Table<? extends DataObject> tbl) {
        TreeSet<UUID> ids = fkIdx.delete(clusterId);

        tbl.deleteAll(ids);
    }

    /**
     * Delete all objects that relates to cluster.
     *
     * @param clusterId Cluster ID.
     */
    protected void deleteAllClusterObjects(UUID clusterId) {
        deleteClusterObjects(clusterId, cachesIdx, cachesTbl);
        deleteClusterObjects(clusterId, modelsIdx, modelsTbl);
        deleteClusterObjects(clusterId, igfssIdx, igfssTbl);
    }

    /**
     * Delete clusters.
     *
     * @param accId Account ID.
     * @param clusterIds Cluster IDs to delete.
     * @return Number of deleted clusters.
     */
    public int deleteClusters(UUID accId, TreeSet<UUID> clusterIds) {
        try (Transaction tx = txMgr.txStart()) {
            checkOwner(accId, clustersTbl.loadAll(clusterIds));

            clusterIds.forEach(this::deleteAllClusterObjects);

            clustersTbl.deleteAll(clusterIds);
            clustersIdx.removeAll(accId, clusterIds);

            tx.commit();
        }

        return clusterIds.size();
    }

    /**
     * Delete all configurations for specified account.
     *
     * @param accId Account ID.
     */
    public void deleteByAccountId(UUID accId) {
        try (Transaction tx = txMgr.txStart()) {
            TreeSet<UUID> clusterIds = clustersIdx.delete(accId);

            clusterIds.forEach(this::deleteAllClusterObjects);

            clustersTbl.deleteAll(clusterIds);

            tx.commit();
        }
    }
}
