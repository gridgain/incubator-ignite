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

import java.util.UUID;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.dto.RowsAffected;
import org.apache.ignite.console.services.ConfigurationsService;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.apache.ignite.console.common.Utils.idsFromJson;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

/**
 * Controller for configurations API.
 */
@RestController
@RequestMapping(path = "/api/v1/configuration")
public class ConfigurationsController {
    /** */
    private final ConfigurationsService cfgsSrvc;

    /**
     * @param cfgsSrvc Configurations service.
     */
    public ConfigurationsController(ConfigurationsService cfgsSrvc) {
        this.cfgsSrvc = cfgsSrvc;
    }

    /**
     * @param acc Account.
     * @param clusterId Cluster ID.
     */
    @GetMapping(path = "/{clusterId}")
    public ResponseEntity<JsonObject> loadConfiguration(
        @AuthenticationPrincipal Account acc,
        @PathVariable("clusterId") UUID clusterId
    ) {
        return ResponseEntity.ok(cfgsSrvc.loadConfiguration(acc.getId(), clusterId));
    }

    /**
     * @param acc Account.
     * @return Clusters short list.
     */
    @GetMapping(path = "/clusters")
    public ResponseEntity<JsonArray> loadClustersShortList(@AuthenticationPrincipal Account acc) {
        return ResponseEntity.ok(cfgsSrvc.loadClusters(acc.getId()));
    }

    /**
     * @param acc Account.
     * @param clusterId Cluster ID.
     * @return Cluster as JSON.
     */
    @GetMapping(path = "/clusters/{clusterId}")
    public ResponseEntity<String> loadCluster(
        @AuthenticationPrincipal Account acc,
        @PathVariable("clusterId") UUID clusterId
    ) {
        return ResponseEntity.ok(cfgsSrvc.loadCluster(acc.getId(), clusterId));
    }

    /**
     * Load cluster caches short list.
     *
     * @param acc Account.
     * @param clusterId Cluster ID.
     * @return Caches short list.
     */
    @GetMapping(path = "/clusters/{clusterId}/caches")
    public ResponseEntity<JsonArray> loadCachesShortList(
        @AuthenticationPrincipal Account acc,
        @PathVariable("clusterId") UUID clusterId
    ) {
        return ResponseEntity.ok(cfgsSrvc.loadShortCaches(acc.getId(), clusterId));
    }

    /**
     * Load cluster models short list.
     *
     * @param acc Account.
     * @param clusterId Cluster ID.
     * @return Models short list.
     */
    @GetMapping(path = "/clusters/{clusterId}/models")
    public ResponseEntity<JsonArray> loadModelsShortList(
        @AuthenticationPrincipal Account acc,
        @PathVariable("clusterId") UUID clusterId
    ) {
        return ResponseEntity.ok(cfgsSrvc.loadShortModels(acc.getId(), clusterId));
    }

    /**
     * Get cluster IGFSs short list.
     *
     * @param acc Account.
     * @param clusterId Cluster ID.
     * @return IGFSs short list.
     */
    @GetMapping(path = "/clusters/{clusterId}/igfss")
    public ResponseEntity<JsonArray> loadIgfssShortList(
        @AuthenticationPrincipal Account acc,
        @PathVariable("clusterId") UUID clusterId
    ) {
        return ResponseEntity.ok(cfgsSrvc.loadShortIgfss(acc.getId(), clusterId));
    }

    /**
     * @param acc Account.
     * @param cacheId Cache ID.
     */
    @GetMapping(path = "/caches/{cacheId}")
    public ResponseEntity<String> loadCache(
        @AuthenticationPrincipal Account acc,
        @PathVariable("cacheId") UUID cacheId
    ) {
        return ResponseEntity.ok(cfgsSrvc.loadCache(acc.getId(), cacheId));
    }

    /**
     * @param acc Account.
     * @param modelId Model ID.
     */
    @GetMapping(path = "/domains/{modelId}")
    public ResponseEntity<String> loadModel(
        @AuthenticationPrincipal Account acc,
        @PathVariable("modelId") UUID modelId
    ) {
        return ResponseEntity.ok(cfgsSrvc.loadModel(acc.getId(), modelId));
    }

    /**
     * @param igfsId IGFS ID.
     */
    @GetMapping(path = "/igfs/{igfsId}")
    public ResponseEntity<String> loadIgfs(
        @AuthenticationPrincipal Account acc,
        @PathVariable("igfsId") UUID igfsId
    ) {
        return ResponseEntity.ok(cfgsSrvc.loadIgfs(acc.getId(), igfsId));
    }

    /**
     * Save cluster.
     *
     * @param acc Account.
     * @param changedItems Items to save.
     * @return Number of affected rows.
     */
    @PutMapping(path = "/clusters", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<RowsAffected> saveAdvancedCluster(
        @AuthenticationPrincipal Account acc,
        @RequestBody JsonObject changedItems
    ) {
        return ResponseEntity.ok(cfgsSrvc.saveAdvancedCluster(acc.getId(), changedItems));
    }

    /**
     * Save basic clusters.
     *
     * @param acc Account.
     * @param changedItems Items to save.
     * @return Number of affected rows.
     */
    @PutMapping(path = "/clusters/basic", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<RowsAffected> saveBasicCluster(
        @AuthenticationPrincipal Account acc,
        @RequestBody JsonObject changedItems
    ) {
        return ResponseEntity.ok(cfgsSrvc.saveBasicCluster(acc.getId(), changedItems));
    }

    /**
     * Delete clusters.
     *
     * @param acc Account.
     * @param clusterIDs Cluster IDs for removal.
     * @return Number of affected rows.
     */
    @PostMapping(path = "/clusters/remove", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<RowsAffected> deleteClusters(
        @AuthenticationPrincipal Account acc,
        @RequestBody JsonObject clusterIDs
    ) {
        return ResponseEntity.ok(cfgsSrvc.deleteClusters(acc.getId(), idsFromJson(clusterIDs, "clusterIDs")));
    }
}
