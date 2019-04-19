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
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.DataObject;
import org.apache.ignite.console.dto.RowsAffected;
import org.apache.ignite.console.repositories.ConfigurationsRepository;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.springframework.stereotype.Service;

import static org.apache.ignite.console.common.Utils.rowsAffected;

/**
 * Service to handle configurations.
 */
@Service
public class ConfigurationsService {
    /** Repository to work with configurations. */
    private final ConfigurationsRepository cfgsRepo;

    /**
     * @param cfgsRepo Configurations repository.
     */
    public ConfigurationsService(ConfigurationsRepository cfgsRepo) {
        this.cfgsRepo = cfgsRepo;
    }

    /**
     * Delete all notebook for specified account.
     *
     * @param accId Account ID.
     */
    void deleteByAccountId(UUID accId) {
        cfgsRepo.deleteByAccountId(accId);
    }

    /**
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     * @return Configuration.
     */
    public JsonObject loadConfiguration(UUID accId, UUID clusterId) {
        return cfgsRepo.loadConfiguration(accId, clusterId);
    }

    /**
     * @param accId Account ID.
     * @return List of clusters for specified account.
     */
    public JsonArray loadClusters(UUID accId) {
        return cfgsRepo.loadClusters(accId);
    }

    /**
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     * @return Cluster.
     */
    public String loadCluster(UUID accId, UUID clusterId) {
        return cfgsRepo.loadCluster(accId, clusterId).json();
    }

    /**
     * @param accId Account ID.
     * @param cacheId Cache ID.
     * @return Cache.
     */
    public String loadCache(UUID accId, UUID cacheId) {
        return cfgsRepo.loadCluster(accId, cacheId).json();
    }

    /**
     * @param accId Account ID.
     * @param mdlId Model ID.
     * @return Model.
     */
    public String loadModel(UUID accId, UUID mdlId) {
        return cfgsRepo.loadCluster(accId, mdlId).json();
    }

    /**
     * @param accId Account ID.
     * @param igfsId IGFS ID.
     * @return IGFS.
     */
    public String loadIgfs(UUID accId, UUID igfsId) {
        return cfgsRepo.loadCluster(accId, igfsId).json();
    }

    /**
     * Convert list of DTOs to short view.
     *
     * @param items List of DTOs to convert.
     * @return List of short objects.
     */
    private JsonArray toShortList(Collection<? extends DataObject> items) {
        JsonArray res = new JsonArray();

        items.forEach(item -> res.add(item.shortView()));

        return res;
    }

    /**
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     * @return Collection of cluster caches.
     */
    public JsonArray loadShortCaches(UUID accId, UUID clusterId) {
        return toShortList(cfgsRepo.loadCaches(accId, clusterId));
    }

    /**
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     * @return Collection of cluster models.
     */
    public JsonArray loadShortModels(UUID accId, UUID clusterId) {
        return toShortList(cfgsRepo.loadModels(accId, clusterId));
    }

    /**
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     * @return Collection of cluster IGFSs.
     */
    public JsonArray loadShortIgfss(UUID accId, UUID clusterId) {
        return toShortList(cfgsRepo.loadIgfss(accId, clusterId));
    }

    /**
     * Save full cluster.
     *
     * @param accId Account ID.
     * @param changedItems Items to save.
     * @return Affected rows JSON object.
     */
    public RowsAffected saveAdvancedCluster(UUID accId, JsonObject changedItems) {
        cfgsRepo.saveAdvancedCluster(accId, changedItems);

        return rowsAffected(1);
    }

    /**
     * Save basic cluster.
     *
     * @param accId Account ID.
     * @param changedItems Items to save.
     * @return Affected rows JSON object.
     */
    public RowsAffected saveBasicCluster(UUID accId, JsonObject changedItems) {
        cfgsRepo.saveBasicCluster(accId, changedItems);

        return rowsAffected(1);
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
     * @param accId Account ID.
     * @param clusterIds Clusters IDs to delete.
     * @return Affected rows JSON object.
     */
    public RowsAffected deleteClusters(UUID accId, TreeSet<UUID> clusterIds) {
        int rmvCnt = cfgsRepo.deleteClusters(accId, clusterIds);

        return rowsAffected(rmvCnt);
    }
}
