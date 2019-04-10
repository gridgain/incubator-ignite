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
import java.util.List;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.DataObject;
import org.apache.ignite.console.repositories.ConfigurationsRepository;
import org.apache.ignite.console.util.JsonObject;
import org.springframework.stereotype.Service;

import static org.apache.ignite.console.common.Utils.idsFromJson;

/**
 * Service to handle configurations.
 */
@Service
public class ConfigurationsService extends AbstractService {
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
     * @param params Parameters in JSON format.
     * @return Configuration.
     */
    private JsonObject loadConfiguration(JsonObject params) {
        UUID clusterId = params.getUuid("clusterId");

        return cfgsRepo.loadConfiguration(clusterId);
    }

    /**
     * @param params Parameters in JSON format.
     * @return List of user clusters.
     */
    private List<Object> loadClusters(JsonObject params) {
        UUID userId = getUserId(params);

        return cfgsRepo.loadClusters(userId);
    }

    /**
     * @param params Parameters in JSON format.
     * @return Cluster.
     */
    private JsonObject loadCluster(JsonObject params) {
        UUID clusterId = params.getUuid("clusterId");

        return new JsonObject(); // cfgsRepo.loadCluster(clusterId).json());
    }

    /**
     * @param params Parameters in JSON format.
     * @return Cache.
     */
    private JsonObject loadCache(JsonObject params) {
        UUID cacheId = params.getUuid("cacheId");

        return new JsonObject(); // cfgsRepo.loadCluster(cacheId).json());
    }

    /**
     * @param params Parameters in JSON format.
     * @return Model.
     */
    private JsonObject loadModel(JsonObject params) {
        UUID mdlId = params.getUuid("modelId");

        return new JsonObject(); // cfgsRepo.loadCluster(mdlId).json());
    }

    /**
     * @param params Parameters in JSON format.
     * @return IGFS.
     */
    private JsonObject loadIgfs(JsonObject params) {
        UUID igfsId = params.getUuid("igfsId");

        return new JsonObject(); // cfgsRepo.loadCluster(igfsId).json());
    }

    /**
     * Convert list of DTOs to short view.
     *
     * @param items List of DTOs to convert.
     * @return List of short objects.
     */
    private List<Object> toShortList(Collection<? extends DataObject> items) {
        return null; // new List<Object>(items.stream().map(DataObject::shortView).collect(Collectors.toList()));
    }

    /**
     * @param params Parameters in JSON format.
     * @return Collection of cluster caches.
     */
    private List<Object> loadShortCaches(JsonObject params) {
        UUID clusterId = params.getUuid("clusterId");

        return toShortList(cfgsRepo.loadCaches(clusterId));
    }

    /**
     * @param params Parameters in JSON format.
     * @return Collection of cluster models.
     */
    private List<Object> loadShortModels(JsonObject params) {
        UUID clusterId = params.getUuid("clusterId");

        return toShortList(cfgsRepo.loadModels(clusterId));
    }

    /**
     * @param params Parameters in JSON format.
     * @return Collection of cluster IGFSs.
     */
    private List<Object> loadShortIgfss(JsonObject params) {
        UUID clusterId = params.getUuid("clusterId");

        return toShortList(cfgsRepo.loadIgfss(clusterId));
    }

    /**
     * Save full cluster.
     *
     * @param params Parameters in JSON format.
     * @return Affected rows JSON object.
     */
    private JsonObject saveAdvancedCluster(JsonObject params) {
        UUID userId = getUserId(params);
        JsonObject json = getProperty(params, "cluster");

        cfgsRepo.saveAdvancedCluster(userId, json);

        return rowsAffected(1);
    }

    /**
     * Save basic cluster.
     *
     * @param params Parameters in JSON format.
     * @return Affected rows JSON object.
     */
    private JsonObject saveBasicCluster(JsonObject params) {
        UUID userId = getUserId(params);
        JsonObject json = getProperty(params, "cluster");

        cfgsRepo.saveBasicCluster(userId, json);

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
     * @param params Parameters in JSON format.
     * @return Affected rows JSON object.
     */
    private JsonObject deleteClusters(JsonObject params) {
        UUID userId = getUserId(params);
        TreeSet<UUID> clusterIds = idsFromJson(getProperty(params, "cluster"), "_id");

        int rmvCnt = cfgsRepo.deleteClusters(userId, clusterIds);

        return rowsAffected(rmvCnt);
    }
}
