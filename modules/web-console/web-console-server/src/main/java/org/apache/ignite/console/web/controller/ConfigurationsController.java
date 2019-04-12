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
     * @param clusterId Cluster ID.
     */
    @GetMapping(path = "/{clusterId}")
    private ResponseEntity<JsonObject> loadConfiguration(@PathVariable("clusterId") UUID clusterId) {
        return ResponseEntity.ok(cfgsSrvc.loadConfiguration(clusterId));
    }

    /**
     * @param user User.
     * @return Clusters short list.
     */
    @GetMapping(path = "/clusters")
    private ResponseEntity<JsonArray> loadClustersShortList(@AuthenticationPrincipal Account user) {
        return ResponseEntity.ok(cfgsSrvc.loadClusters(user.getId()));
    }

    /**
     * @param clusterId Cluster ID.
     */
    @GetMapping(path = "/clusters/{clusterId}")
    private ResponseEntity<String> loadCluster(@PathVariable("clusterId") UUID clusterId) {
        return ResponseEntity.ok(cfgsSrvc.loadCluster(clusterId));
    }

    /**
     * Load cluster caches short list.
     *
     * @param clusterId Cluster ID.
     */
    @GetMapping(path = "/clusters/{clusterId}/caches")
    private ResponseEntity<JsonArray> loadCachesShortList(@PathVariable("clusterId") UUID clusterId) {
        return ResponseEntity.ok(cfgsSrvc.loadShortCaches(clusterId));
    }

    /**
     * Load cluster models short list.
     *
     *  @param clusterId Cluster ID.
     */
    @GetMapping(path = "/clusters/{clusterId}/models")
    private ResponseEntity<JsonArray> loadModelsShortList(@PathVariable("clusterId") UUID clusterId) {
        return ResponseEntity.ok(cfgsSrvc.loadShortModels(clusterId));
    }

    /**
     * Get cluster IGFSs short list.
     *
     * @param clusterId Cluster ID.
     */
    @GetMapping(path = "/clusters/{clusterId}/igfss")
    private ResponseEntity<JsonArray> loadIgfssShortList(@PathVariable("clusterId") UUID clusterId) {
        return ResponseEntity.ok(cfgsSrvc.loadShortIgfss(clusterId));
    }

    /**
     * @param cacheId Cache ID.
     */
    @GetMapping(path = "/caches/{cacheId}")
    private ResponseEntity<String> loadCache(@PathVariable("cacheId") UUID cacheId) {
        return ResponseEntity.ok(cfgsSrvc.loadCache(cacheId));
    }

    /**
     * @param modelId Model ID.
     */
    @GetMapping(path = "/domains/{modelId}")
    private ResponseEntity<String> loadModel(@PathVariable("modelId") UUID modelId) {
        return ResponseEntity.ok(cfgsSrvc.loadModel(modelId));
    }

    /**
     * @param igfsId IGFS ID.
     */
    @GetMapping(path = "/igfs/{igfsId}")
    private ResponseEntity<String> loadIgfs(@PathVariable("igfsId") UUID igfsId) {
        return ResponseEntity.ok(cfgsSrvc.loadIgfs(igfsId));
    }

    /**
     * Save cluster.
     *
     * @param user User.
     * @param changedItems Items to save.
     */
    @PutMapping(path = "/clusters", consumes = APPLICATION_JSON_VALUE)
    private ResponseEntity<JsonObject> saveAdvancedCluster(
        @AuthenticationPrincipal Account user,
        @RequestBody JsonObject changedItems
    ) {
        return ResponseEntity.ok(cfgsSrvc.saveAdvancedCluster(user.getId(), changedItems));
    }

    /**
     * Save basic clusters.
     *
     * @param user User.
     * @param changedItems Items to save.
     */
    @PutMapping(path = "/clusters/basic", consumes = APPLICATION_JSON_VALUE)
    private ResponseEntity<JsonObject> saveBasicCluster(
        @AuthenticationPrincipal Account user,
        @RequestBody JsonObject changedItems
    ) {
        return ResponseEntity.ok(cfgsSrvc.saveBasicCluster(user.getId(), changedItems));
    }

    /**
     * Delete clusters.
     *
     * @param user User.
     * @param clusterIDs Cluster IDs for removal.
     */
    @PostMapping(path = "/clusters/remove", consumes = APPLICATION_JSON_VALUE)
    private ResponseEntity<JsonObject> deleteClusters(
        @AuthenticationPrincipal Account user,
        @RequestBody JsonObject clusterIDs
    ) {
        return ResponseEntity.ok(cfgsSrvc.deleteClusters(user.getId(), idsFromJson(clusterIDs, "clusterIDs")));
    }
}

