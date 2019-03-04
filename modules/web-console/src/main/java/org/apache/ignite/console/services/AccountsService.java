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
import static org.apache.ignite.console.common.Utils.emptyJson;
import static org.apache.ignite.console.common.Utils.uuidParam;

/**
 * Service to handle accounts.
 */
public class AccountsService extends AbstractService {
    /** Special key to check that first user should be granted admin rights. */
    private static final UUID FIRST_USER_MARKER_KEY = UUID.fromString("039d28e2-133d-4eae-ae2b-29d6db6d4974");

    /** */
    private final Table<Account> accountsTbl;

    /**
     * @param ignite Ignite.
     */
    public AccountsService(Ignite ignite) {
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
        addConsumer(Addresses.ACCOUNT_GET_BY_ID, this::getById);
        addConsumer(Addresses.ACCOUNT_GET_BY_EMAIL, this::getByEmail);
        addConsumer(Addresses.ACCOUNT_REGISTER, this::register);
        addConsumer(Addresses.ACCOUNT_LIST, this::list);
        addConsumer(Addresses.ACCOUNT_SAVE, this::save);
        addConsumer(Addresses.ACCOUNT_DELETE, this::delete);
        addConsumer(Addresses.ACCOUNT_TOGGLE, this::toggle);
    }

    /**
     * Get account by ID.
     *
     * @param params Parameters in JSON format.
     * @return Account as JSON.
     */
    private JsonObject getById(JsonObject params) {
        UUID accId = uuidParam(params, "_id");

        try(Transaction ignored = txStart()) {
            Account acc = accountsTbl.load(accId);

            if (acc == null)
                throw new IllegalStateException("Account not found with ID: " + accId);

            return acc.principal();
        }
    }

    /**
     * Get account by email.
     *
     * @param params Parameters in JSON format.
     * @return Account as JSON.
     */
    private JsonObject getByEmail(JsonObject params) {
        String email = params.getString("email");

        try(Transaction ignored = txStart()) {
            Account acc = accountsTbl.getByIndex(email);

            if (acc == null)
                throw new IllegalStateException("Account not found with email: " + email);

            return acc.toJson();
        }
    }

    /**
     * @param params Parameters in JSON format.
     */
    private JsonObject register(JsonObject params) {
        boolean admin = shouldBeAdmin();

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
            admin,
            false,
            false
        );

        accountsTbl.save(account);

        return emptyJson();
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
     * Save account.
     *
     * @param params Parameters in JSON format.
     * @return Saved account.
     */
    private JsonObject save(JsonObject params) {
        Account account = Account.fromJson(params);

        try (Transaction tx = txStart()) {
            accountsTbl.save(account);

            tx.commit();
        }

        return emptyJson();
    }

    /**
     * @return {@code true} if first user should be granted with admin permissions.
     */
    @SuppressWarnings("unchecked")
    private boolean shouldBeAdmin() {
        boolean admin;

        try(Transaction tx = txStart()) {
            IgniteCache cache = accountsTbl.cache();

            Object firstUserMarker = cache.get(FIRST_USER_MARKER_KEY);

            admin = firstUserMarker == null;

            if (admin)
                cache.put(FIRST_USER_MARKER_KEY, FIRST_USER_MARKER_KEY);

            tx.commit();
        }

        return admin;
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

        return emptyJson();
    }
}
