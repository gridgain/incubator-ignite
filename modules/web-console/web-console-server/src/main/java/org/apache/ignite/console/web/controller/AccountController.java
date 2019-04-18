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
import org.apache.ignite.console.web.model.ChangeUserRequest;
import org.apache.ignite.console.web.model.SignInRequest;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.console.web.model.UserResponse;
import org.springframework.http.HttpStatus;
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

import static org.apache.ignite.console.common.Utils.currentRequestOrigin;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

/**
 * Controller for accounts API.
 */
@RestController
public class AccountController {
    /** Authentication manager. */
    private final AuthenticationManager authMgr;

    /** Accounts service. */
    private final AccountsService accountsSrvc;

    /**
     * @param authMgr Authentication manager.
     * @param accountsSrvc Accounts service.
     */
    public AccountController(AuthenticationManager authMgr,  AccountsService accountsSrvc) {
        this.authMgr = authMgr;
        this.accountsSrvc = accountsSrvc;
    }

    /**
     * @param user User.
     */
    @GetMapping(path = "/api/v1/user")
    private ResponseEntity<UserResponse> user(@AuthenticationPrincipal UserDetails user) {
        Account acc = accountsSrvc.loadUserByUsername(user.getUsername());

        return ResponseEntity.ok(new UserResponse(
            acc.email(),
            acc.firstName(),
            acc.lastName(),
            acc.phone(),
            acc.company(),
            acc.country(),
            acc.token(),
            acc.admin()
        ));
    }

    /**
     * @param params SignUp params.
     */
    @PostMapping(path = "/api/v1/signup")
    private ResponseEntity<Void> signup(@Valid @RequestBody SignUpRequest params) {
        Account account = accountsSrvc.register(params);

        if (account.isEnabled()) {
            Authentication authentication = authMgr.authenticate(
                new UsernamePasswordAuthenticationToken(params.getEmail(), params.getPassword()));

            SecurityContextHolder.getContext().setAuthentication(authentication);
        }

        return ResponseEntity.ok().build();
    }

    /**
     * @param acc Current user.
     * @param changes Changes to apply to user.
     * @return {@linkplain HttpStatus#OK OK} on success.
     */
    @PostMapping(path = "/api/v1/profile/save", consumes = APPLICATION_JSON_VALUE)
    private ResponseEntity<Void> save(@AuthenticationPrincipal Account acc, @Valid @RequestBody ChangeUserRequest changes) {
        accountsSrvc.save(acc.getId(), changes);

        return ResponseEntity.ok().build();
    }

    /**
     * @param req Forgot password request.
     * @return {@linkplain HttpStatus#OK OK} on success.
     */
    @PostMapping(path = "/api/v1/password/forgot", consumes = APPLICATION_JSON_VALUE)
    private ResponseEntity<Void> forgotPassword(@RequestBody SignInRequest req) {
        accountsSrvc.forgotPassword(currentRequestOrigin(), req.getEmail());

        return ResponseEntity.ok().build();
    }

    /**
     *
     * @return {@linkplain HttpStatus#OK OK} on success.
     */
    @PostMapping(path = "/api/v1/password/reset")
    private ResponseEntity<Void> resetPassword() {
        return ResponseEntity.ok().build();
    }

    /**
     *
     * @return {@linkplain HttpStatus#OK OK} on success.
     */
    @PostMapping(path = "/api/v1/password/validate/token")
    private ResponseEntity<Void> validatePasswordToken() {
        return ResponseEntity.ok().build();
    }

    /**
     *
     * @return {@linkplain HttpStatus#OK OK} on success.
     */
    @PostMapping(path = "/api/v1/activation/resend")
    private ResponseEntity<Void> resendSignupConfirmation() {
        return ResponseEntity.ok().build();
    }
}
