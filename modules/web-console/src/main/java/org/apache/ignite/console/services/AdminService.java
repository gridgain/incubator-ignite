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
import javax.cache.Cache;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.console.common.Addresses;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.console.common.Utils.boolParam;
import static org.apache.ignite.console.common.Utils.uuidParam;

/**
 * Service to handle administaror actions.
 */
public class AdminService extends AbstractService {
    /** */
    private final Table<Account> accountsTbl;

    /**
     * @param ignite Ignite.
     */
    public AdminService(Ignite ignite) {
        super(ignite);

        accountsTbl = new Table<Account>(ignite, "accounts")
            .addUniqueIndex(Account::email, (acc) -> "Account with email '" + acc.email() + "' already registered");
    }

    /** {@inheritDoc} */
    @Override protected void initialize() {
        accountsTbl.cache();
    }

    /** {@inheritDoc} */
    @Override public void start() {
        addConsumer(Addresses.ADMIN_LOAD_ACCOUNTS, this::list);
        addConsumer(Addresses.ADMIN_DELETE_ACCOUNT, this::delete);
        addConsumer(Addresses.ADMIN_CHANGE_ADMIN_STATUS, this::toggle);
    }

    /**
     * @param params Parameters in JSON format.
     */
    private JsonArray list(JsonObject params) {
        IgniteCache<UUID, Account> cache = accountsTbl.cache();

        List<Cache.Entry<UUID, Account>> users = cache.query(new ScanQuery<UUID, Account>()).getAll();

        JsonArray res = new JsonArray();

        users.forEach(entry -> {
            Object v = entry.getValue();

            if (v instanceof Account) {
                Account user = (Account)v;

                res.add(new JsonObject()
                    .put("_id", user._id())
                    .put("firstName", user.firstName())
                    .put("lastName", user.lastName())
                    .put("admin", user.admin())
                    .put("email", user.email())
                    .put("company", user.company())
                    .put("country", user.country())
                    .put("lastLogin", user.lastLogin())
                    .put("lastActivity", user.lastActivity())
                    .put("activated", user.activated())
                    .put("counters", new JsonObject()
                        .put("clusters", 0)
                        .put("caches", 0)
                        .put("models", 0)
                        .put("igfs", 0))
                );
            }
        });

        return res;
    }

    /**
     * Remove account.
     *
     * @param params Parameters in JSON format.
     * @return JSON with affected rows .
     */
    private JsonObject delete(JsonObject params) {
        int rmvCnt = 0;

        UUID accId = uuidParam(params, "_id");

        try (Transaction tx = txStart()) {
            Account acc = accountsTbl.delete(accId);

            if (acc != null)
                rmvCnt = 1;

            tx.commit();
        }

        return rowsAffected(rmvCnt);
    }

    /**
     * @param params Parameters in JSON format.
     * @return JSON result.
     */
    private JsonObject toggle(JsonObject params) {
        UUID userId = uuidParam(params, "userId");
        boolean adminFlag = boolParam(params, "adminFlag");

        try (Transaction tx = txStart()) {
            Account acc = accountsTbl.load(userId);

            if (acc == null)
                throw new IllegalStateException("Account not found for id: " + userId);

            acc.admin(adminFlag);

            accountsTbl.save(acc);

            tx.commit();
        }

        return rowsAffected(1);
    }
}
