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

import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.common.Addresses;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.console.common.Utils.boolParam;
import static org.apache.ignite.console.common.Utils.uuidParam;

/**
 * Service to handle accounts.
 */
public class AccountsService extends AbstractService {
    /** Repository to work with accounts. */
    private final AccountsRepository accountsRepo;

    /**
     * @param ignite Ignite.
     */
    public AccountsService(Ignite ignite) {
        super(ignite);

        this.accountsRepo = new AccountsRepository(ignite);
    }

    /** {@inheritDoc} */
    @Override public AccountsService install(Vertx vertx) {
        addConsumer(vertx, Addresses.ACCOUNT_GET_BY_ID, this::getById);
        addConsumer(vertx, Addresses.ACCOUNT_GET_BY_EMAIL, this::getByEmail);
        addConsumer(vertx, Addresses.ACCOUNT_REGISTER, this::register);

        return this;
    }

    /**
     * Get account by ID.
     *
     * @param params Parameters in JSON format.
     * @return Public fields of account as JSON.
     */
    private JsonObject getById(JsonObject params) {
        UUID accId = uuidParam(params, "_id");

        return accountsRepo.getById(accId).publicView();
    }

    /**
     * Get account by email.
     *
     * @param params Parameters in JSON format.
     * @return Account as JSON.
     */
    private JsonObject getByEmail(JsonObject params) {
        String email = params.getString("email");

        return accountsRepo.getByEmail(email).toJson();
    }

    /**
     * @param params Parameters in JSON format.
     * @return Affected rows JSON object.
     */
    private JsonObject register(JsonObject params) {
        Account account = new Account(
            UUID.randomUUID(),
            params.getString("email"),
            params.getString("firstName"),
            params.getString("lastName"),
            params.getString("company"),
            params.getString("country"),
            params.getString("industry"),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            ZonedDateTime.now().toString(),
            "",
            "",
            "",
            params.getString("salt"),
            params.getString("hash"),
            false,
            false,
            false
        );

        accountsRepo.save(account);

        return rowsAffected(1);
    }

    /**
     * Delete account by ID.
     * @return All registered accounts.
     */
    List<Account> list() {
        return accountsRepo.list();
    }

    /**
     * Delete account by ID.
     *
     * @param accId Account ID.
     */
    void delete(UUID accId) {
        accountsRepo.delete(accId);
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
}
