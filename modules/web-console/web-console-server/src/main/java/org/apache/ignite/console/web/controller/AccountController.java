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

import javax.validation.Valid;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.services.AccountsService;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.console.web.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller with account API.
 */
@RestController
public class AccountController {
    /** Authentication manager. */
    private AuthenticationManager authMgr;

    /** Accounts service. */
    private AccountsService accountsSrvc;

    /**
     * @param authMgr Authentication manager.
     * @param accountsSrvc Accounts service.
     */
    @Autowired
    public AccountController(AuthenticationManager authMgr,  AccountsService accountsSrvc) {
        this.authMgr = authMgr;
        this.accountsSrvc = accountsSrvc;
    }

    /**
     * @param user User.
     */
    @GetMapping(path = "/api/v1/user")
    public ResponseEntity<User> user(@AuthenticationPrincipal UserDetails user) {
        Account acc = accountsSrvc.loadUserByUsername(user.getUsername());

        return ResponseEntity.ok(new User(
            acc.email(),
            acc.firstName(),
            acc.lastName(),
            acc.phone(),
            acc.company(),
            acc.country(),
            acc.token()
        ));
    }

    /**
     * @param params SignUp params.
     */
    @PostMapping(path = "/api/v1/signup")
    public ResponseEntity<Void> signup(@Valid @RequestBody SignUpRequest params) {
        Account account = accountsSrvc.register(params);

        if (account.isEnabled()) {
            Authentication authentication = authMgr.authenticate(
                new UsernamePasswordAuthenticationToken(params.getEmail(), params.getPassword()));

            SecurityContextHolder.getContext().setAuthentication(authentication);
        }

        return ResponseEntity.ok().build();
    }

    /** */
    @GetMapping(path = "/configuration/clusters")
    public String getClusters() {
        return "[{\"_id\":\"5c6813aeb1a0ab3e22b9c17c\",\"name\":\"Cluster\",\"discovery\":\"Multicast\",\"cachesCount\":1,\"modelsCount\":0,\"igfsCount\":1,\"gridgainInfo\":{\"gridgainEnabled\":true,\"rollingUpdatesEnabled\":false,\"securityEnabled\":false,\"dataReplicationReceiverEnabled\":false,\"dataReplicationSenderEnabled\":false,\"snapshotsEnabled\":true}}]";
    }

    /** */
    @PostMapping(path = "/activities/page")
    public String activitiesPage() {
        return "{}";
    }
}
