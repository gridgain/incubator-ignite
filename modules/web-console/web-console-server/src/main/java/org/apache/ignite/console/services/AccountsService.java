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

import java.util.List;
import java.util.UUID;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.console.web.model.ChangeUserRequest;
import org.apache.ignite.console.web.model.SignUpRequest;
import org.apache.ignite.console.web.socket.WebSocketManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.transactions.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

/**
 * Service to handle accounts.
 */
@Service
public class AccountsService implements UserDetailsService {
    /** Repository to work with accounts. */
    private final AccountsRepository accountsRepo;

    /** */
    private final WebSocketManager wsm;

    /** Password encoder. */
    private final PasswordEncoder encoder;

    /**
     * @param accountsRepo Accounts repository.
     * @param wsm Websocket manager.
     */
    @Autowired
    public AccountsService(AccountsRepository accountsRepo, WebSocketManager wsm) {
        this.accountsRepo = accountsRepo;
        this.wsm = wsm;

        this.encoder = encoder();
    }

    /** {@inheritDoc} */
    @Override public Account loadUserByUsername(String email) throws UsernameNotFoundException {
        return accountsRepo.getByEmail(email);
    }

    /**
     * @param params SignUp params.
     * @return Registered account.
     */
    public Account register(SignUpRequest params) {
        Account account = new Account(
            params.getEmail(),
            encoder.encode(params.getPassword()),
            params.getFirstName(),
            params.getLastName(),
            params.getPhone(),
            params.getCompany(),
            params.getCountry()
        );

        return accountsRepo.create(account);
    }

    /**
     * Delete account by ID.
     *
     * @return All registered accounts.
     */
    public List<Account> list() {
        return accountsRepo.list();
    }

    /**
     * Delete account by ID.
     *
     * @param accId Account ID.
     * @return Number of removed accounts.
     */
    public int delete(UUID accId) {
        return accountsRepo.delete(accId);
    }

    /**
     * Update admin flag..
     *
     * @param accId Account ID.
     * @param adminFlag New value for admin flag.
     */
    public void toggle(UUID accId, boolean adminFlag) {
        try (Transaction tx = accountsRepo.txStart()) {
            Account account = accountsRepo.getById(accId);

            if (account.admin() != adminFlag) {
                account.admin(adminFlag);

                accountsRepo.save(account);

                tx.commit();
            }
        }
    }

    /**
     * Save user.
     *
     * @param accId User ID.
     * @param changes Changes to apply to user.
     */
    public void save(UUID accId, ChangeUserRequest changes) {
        try (Transaction tx = accountsRepo.txStart()) {
            Account acc = accountsRepo.getById(accId);

            String pwd = changes.getPassword();

            if (!F.isEmpty(pwd))
                acc.setPassword(encoder.encode(pwd));

            String oldTok = acc.token();
            String newTok = changes.getToken();

            if (!oldTok.equals(newTok)) {
                wsm.revokeToken(oldTok);

                acc.token(newTok);
            }

            String oldEmail = acc.email();
            String newEmail = changes.getEmail();

            if (!oldEmail.equals(newEmail)) {
                Account accByEmail = accountsRepo.getByEmail(oldEmail);

                if (acc.getId().equals(accByEmail.getId()))
                    acc.email(changes.getEmail());
                else
                    throw new IllegalStateException("User with this email already registered");
            }

            acc.firstName(changes.getFirstName());
            acc.lastName(changes.getLastName());
            acc.phone(changes.getPhone());
            acc.country(changes.getCountry());
            acc.company(changes.getCompany());

            accountsRepo.save(acc);

            tx.commit();
        }
    }

    /**
     * @return Service for encoding user passwords.
     */
    @Bean
    public PasswordEncoder encoder() {
        // Pbkdf2PasswordEncoder is compatible with passport.js, but BCryptPasswordEncoder is recommended by Spring.
        // We can return to Pbkdf2PasswordEncoder if we decided to import old users.
        //  Pbkdf2PasswordEncoder encoder = new Pbkdf2PasswordEncoder("", 25000, HASH_WIDTH); // HASH_WIDTH = 512
        //
        //  encoder.setAlgorithm(PBKDF2WithHmacSHA256);
        //  encoder.setEncodeHashAsBase64(true);

        return new BCryptPasswordEncoder();
    }
}
