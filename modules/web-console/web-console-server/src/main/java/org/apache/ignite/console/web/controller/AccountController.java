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
import org.apache.ignite.console.web.model.UserDto;
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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** */
@RestController
@RequestMapping("/api/v1")
public class AccountController {
    /** Authentication manager. */
    private AuthenticationManager authenticationMgr;

    /** Accounts service. */
    private AccountsService accountsSrvc;

    /**
     * @param authenticationMgr Authentication manager.
     * @param accountsSrvc Accounts service.
     */
    @Autowired
    public AccountController(AuthenticationManager authenticationMgr,  AccountsService accountsSrvc) {
        this.authenticationMgr = authenticationMgr;
        this.accountsSrvc = accountsSrvc;
    }

    /**
     * @param user User.
     */
    @GetMapping(path = "/user")
    public ResponseEntity<UserDto> user(@AuthenticationPrincipal UserDetails user) {
        Account acc = accountsSrvc.loadUserByUsername(user.getUsername());

        return ResponseEntity.ok(new UserDto(
            acc.email(),
            acc.firstName(),
            acc.lastName(),
            acc.phone(),
            acc.company(),
            acc.country()
        ));
    }

    /**
     * @param user User.
     */
    @PostMapping(path = "/signup")
    public ResponseEntity<Void> register(@Valid @RequestBody UserDto user) {
        Account account = accountsSrvc.register(user);

        if (account.isEnabled()) {
            Authentication authentication = authenticationMgr.authenticate(
                new UsernamePasswordAuthenticationToken(user.getEmail(), user.getPassword()));

            SecurityContextHolder.getContext().setAuthentication(authentication);
        }

        return ResponseEntity.ok().build();
    }
}
