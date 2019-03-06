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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.common.Addresses;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.console.repositories.AccountsRepository;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.console.common.Utils.boolParam;
import static org.apache.ignite.console.common.Utils.uuidParam;

/**
 * Service to handle administrator actions.
 */
public class AdminService extends AbstractService {
    /** */
    private final AccountsRepository accountsRepo;

    /**
     * @param ignite Ignite.
     */
    public AdminService(Ignite ignite) {
        super(ignite);

        accountsRepo = new AccountsRepository(ignite);
    }

    /** {@inheritDoc} */
    @Override protected void initEventBus() {
        addConsumer(Addresses.ADMIN_LOAD_ACCOUNTS, this::list);
        addConsumer(Addresses.ADMIN_DELETE_ACCOUNT, this::delete);
        addConsumer(Addresses.ADMIN_CHANGE_ADMIN_STATUS, this::toggle);
    }

    /**
     * @param params Parameters in JSON format.
     */
    @SuppressWarnings("unused")
    private JsonArray list(JsonObject params) {
        List<Account> accounts = accountsRepo.list();

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
     * @param params Parameters in JSON format.
     * @return Affected rows JSON object.
     */
    private JsonObject delete(JsonObject params) {
        int rmvCnt;

        UUID accId = uuidParam(params, "_id");

        try (Transaction tx = accountsRepo.txStart()) {
            rmvCnt = accountsRepo.delete(accId);

//            if (rmvCnt > 0) {
//                // TODO WC-935 Delete all dependent entities.
//            }

            tx.commit();
        }

        return rowsAffected(rmvCnt);
    }

    /**
     * @param params Parameters in JSON format.
     * @return Affected rows JSON object.
     */
    private JsonObject toggle(JsonObject params) {
        UUID accId = uuidParam(params, "userId");
        boolean adminFlag = boolParam(params, "adminFlag", false);

        try (Transaction tx = accountsRepo.txStart()) {
            Account account = accountsRepo.getById(accId);

            account.admin(adminFlag);

            accountsRepo.save(account);

            tx.commit();
        }

        return rowsAffected(1);
    }
}
