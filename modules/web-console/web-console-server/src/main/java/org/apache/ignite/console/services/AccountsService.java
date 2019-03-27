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
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.common.Addresses;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.console.web.model.UserDto;
import org.apache.ignite.transactions.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.crypto.password.Pbkdf2PasswordEncoder;
import org.springframework.stereotype.Service;

import static org.springframework.security.crypto.password.Pbkdf2PasswordEncoder.SecretKeyFactoryAlgorithm.PBKDF2WithHmacSHA256;

/**
 * Service to handle accounts.
 */
@Service
public class AccountsService extends AbstractService implements UserDetailsService {
    /**  */
    private static final int HASH_WIDTH = 512;

    /** Password encoder. */
    private PasswordEncoder encoder;

    /** Repository to work with accounts. */
    private AccountsRepository accountsRepo;

    /**
     * @param ignite Ignite.
     */
    @Autowired
    public AccountsService(Ignite ignite, AccountsRepository accountsRepo) {
        super(ignite);

        this.encoder = encoder();
        this.accountsRepo = accountsRepo;
    }

    /** {@inheritDoc} */
    @Override public AccountsService install(Vertx vertx) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public Account loadUserByUsername(String username) throws UsernameNotFoundException {
        return accountsRepo.getByEmail(username);
    }

    /**
     * @param user User.
     * @return Registered account.
     */
    public Account register(UserDto user) {
        Account account = new Account(
            user.getEmail(),
            encoder.encode(user.getPassword()),
            user.getFirstName(),
            user.getLastName(),
            user.getPhone(),
            user.getCompany(),
            user.getCountry()
        );

        return accountsRepo.create(account);
    }

    /**
     * Delete account by ID.
     *
     * @return All registered accounts.
     */
    List<Account> list() {
        return accountsRepo.list();
    }

    /**
     * Delete account by ID.
     *
     * @param accId Account ID.
     * @return Number of removed accounts.
     */
    int delete(UUID accId) {
        return accountsRepo.delete(accId);
    }

    /**
     * Update account permission.
     *
     * @param accId Account ID.
     * @param adminFlag New value for admin flag.
     */
    void updatePermission(UUID accId, boolean adminFlag) {
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
     * @return Service for encoding user passwords.
     */
    @Bean
    public PasswordEncoder encoder() {
        Pbkdf2PasswordEncoder encoder = new Pbkdf2PasswordEncoder("", 25000, HASH_WIDTH);

        encoder.setAlgorithm(PBKDF2WithHmacSHA256);
        encoder.setEncodeHashAsBase64(true);

        return encoder;
    }
}
