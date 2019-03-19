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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.common.Addresses;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.console.common.Utils.boolParam;
import static org.apache.ignite.console.common.Utils.uuidParam;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Service to handle administrator actions.
 */
public class AdminService extends AbstractService {
    /** */
    private final AccountsService accountsSvc;

    /** */
    private final ConfigurationsService cfgsSvc;

    /** */
    private final NotebooksService notebooksSvc;

    /**
     * @param ignite Ignite.
     * @param accountsSvc Service to work with accounts.
     * @param cfgsSvc Service to work with configurations.
     * @param notebooksSvc Service to work with notebooks.
     */
    public AdminService(
        Ignite ignite,
        AccountsService accountsSvc,
        ConfigurationsService cfgsSvc,
        NotebooksService notebooksSvc
    ) {
        super(ignite);

        this.accountsSvc = accountsSvc;
        this.cfgsSvc = cfgsSvc;
        this.notebooksSvc = notebooksSvc;
    }

    /** {@inheritDoc} */
    @Override public AdminService install(Vertx vertx) {
        addConsumer(vertx, Addresses.ADMIN_LOAD_ACCOUNTS, this::list);
        addConsumer(vertx, Addresses.ADMIN_DELETE_ACCOUNT, this::delete);
        addConsumer(vertx, Addresses.ADMIN_CHANGE_ADMIN_STATUS, this::toggle);

        return this;
    }

    /**
     * @param params Parameters in JSON format.
     */
    @SuppressWarnings("unused")
    private JsonArray list(JsonObject params) {
        List<Account> accounts = accountsSvc.list();

        JsonArray res = new JsonArray();

        accounts.forEach(account ->
            res.add(new JsonObject()
                .put("_id", account._id())
                .put("firstName", account.firstName())
                .put("lastName", account.lastName())
                .put("admin", account.admin())
                .put("email", account.email())
                .put("company", account.company())
                .put("country", account.country())
                .put("lastLogin", account.lastLogin())
                .put("lastActivity", account.lastActivity())
                .put("activated", account.activated())
                .put("counters", new JsonObject()
                    .put("clusters", 0)
                    .put("caches", 0)
                    .put("models", 0)
                    .put("igfs", 0))
            )
        );

        return res;
    }

    /**
     * Remove account.
     *
     * @param accId Account Id.
     * @return Affected rows JSON object.
     */
    private JsonObject delete(String accId) {
        UUID id = UUID.fromString(accId);

        int rows;

        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cfgsSvc.deleteByAccountId(id);
            notebooksSvc.deleteByAccountId(id);

            rows = accountsSvc.delete(id);

            tx.commit();
        }

        return rowsAffected(rows);
    }

    /**
     * @param params Parameters in JSON format.
     * @return Affected rows JSON object.
     */
    private JsonObject toggle(JsonObject params) {
        accountsSvc.updatePermission(uuidParam(params, "accountId"), boolParam(params, "admin", false));

        return rowsAffected(1);
    }
}
