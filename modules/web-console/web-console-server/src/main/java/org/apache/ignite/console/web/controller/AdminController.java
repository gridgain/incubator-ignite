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
import io.swagger.annotations.ApiOperation;
import javax.validation.Valid;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.services.AdminService;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.console.web.model.ToggleRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

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
    @ApiOperation(value = "Get a list of users.")
    @PostMapping(path = "/list", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<JsonArray> loadAccounts(@ApiIgnore @RequestBody JsonObject params) {
        return ResponseEntity.ok(adminSrvc.list());
    }

    /**
     * @param acc Account.
     * @param params Parameters.
     */
    @ApiOperation(value = "Toggle admin permissions.")
    @PostMapping(path = "/toggle", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> toggle(
        @AuthenticationPrincipal Account acc,
        @RequestBody ToggleRequest params
    ) {
        UUID accId = params.getId();
        boolean admin = params.isAdmin();

        if (acc.getId().equals(accId) && !admin)
            throw new IllegalStateException("Self revoke of administrator rights is prohibited");

        adminSrvc.toggle(accId, admin);

        return ResponseEntity.ok().build();
    }

    /**
     * @param params SignUp params.
     */
    @ApiOperation(value = "Register user.")
    @PutMapping(path = "/users")
    public ResponseEntity<Void> registerUser(@Valid @RequestBody SignUpRequest params) {
        adminSrvc.registerUser(params);

        return ResponseEntity.ok().build();
    }

    /**
     * @param accId Account ID.
     */
    @ApiOperation(value = "Delete user.")
    @DeleteMapping(path = "/users/{accountId}")
    public ResponseEntity<Void> delete(@PathVariable("accountId") UUID accId) {
        adminSrvc.delete(accId);

        return ResponseEntity.ok().build();
    }

    /**
     * @param params Parameters.
     */
    @ApiIgnore
    @PostMapping(path = "/become", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> become(@RequestBody JsonObject params) {
        adminSrvc.become(params.getUuid("id"));

        return ResponseEntity.ok().build();
    }
}
