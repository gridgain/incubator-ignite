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

import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.services.AdminService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

/**
 * Controller for admin API.
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminController {
    /** Admin service. */
    private final AdminService adminSrvc;

    /**
     * @param adminSrvc Admin service.
     */
    @Autowired
    public AdminController(AdminService adminSrvc) {
        this.adminSrvc = adminSrvc;
    }

    /**
     * @param params Parameters.
     * @return List of accounts.
     */
    @PostMapping(path = "/list", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<JsonArray> loadAccounts(@RequestBody JsonObject params) {
        return ResponseEntity.ok(adminSrvc.list());
    }

    /**
     * @param params Parameters.
     */
    @PostMapping(path = "/toggle", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> toggle(@RequestBody JsonObject params) {
        adminSrvc.toggle(params.getUuid("id"), params.getBoolean("admin", false));

        return ResponseEntity.ok().build();
    }

    /**
     * @param params Parameters.
     */
    @PostMapping(path = "/remove", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> remove(@RequestBody JsonObject params) {
        adminSrvc.remove(params.getUuid("id"));

        return ResponseEntity.ok().build();
    }

    /**
     * @param params Parameters.
     */
    @PostMapping(path = "/become", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> become(@RequestBody JsonObject params) {
        adminSrvc.become(params.getUuid("id"));

        return ResponseEntity.ok().build();
    }
}
