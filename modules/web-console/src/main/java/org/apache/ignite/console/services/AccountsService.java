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
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.console.common.Addresses;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.transactions.Transaction;

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;

/**
 * Service to handle notebooks.
 */
public class AccountsService extends AbstractService {
    /** Special key to check that first user should be granted admin rights. */
    private static final UUID FIRST_USER_MARKER_KEY = UUID.fromString("75744f56-59c4-4228-b07a-fabf4babc059");

    /** */
    private final Table<Account> accountsTbl;

    /**
     * @param ignite Ignite.
     */
    public AccountsService(Ignite ignite) {
        super(ignite);

        accountsTbl = new Table<Account>(ignite, "accounts")
            .addUniqueIndex(Account::email, (acc) -> "Account with email: " + acc.email() + " already registered");
    }

    /** {@inheritDoc} */
    @Override protected void initialize() {
        accountsTbl.cache();
    }


    /** {@inheritDoc} */
    @Override public void start() {
        EventBus eventBus = vertx.eventBus();

        eventBus.consumer(Addresses.ACCOUNT_GET_BY_ID, this::getById1);
        eventBus.consumer(Addresses.ACCOUNT_GET_BY_EMAIL, this::getByEmail1);
        eventBus.consumer(Addresses.ACCOUNT_LIST, this::list1);
        eventBus.consumer(Addresses.ACCOUNT_SAVE, this::save1);
        eventBus.consumer(Addresses.ACCOUNT_DELETE, this::delete1);
        eventBus.consumer(Addresses.ACCOUNT_TOGGLE, this::toggle1);
    }

    /**
     * @param msg Message.
     */
    private void getById1(Message<JsonObject> msg) {
        msg.fail(HTTP_INTERNAL_ERROR, "Not implemented yet");
    }

    /**
     * @param msg Message.
     */
    private void getByEmail1(Message<JsonObject> msg) {
        msg.fail(HTTP_INTERNAL_ERROR, "Not implemented yet");
    }

    /**
     * @param msg Message.
     */
    private void list1(Message<JsonObject> msg) {
        msg.fail(HTTP_INTERNAL_ERROR, "Not implemented yet");
    }

    /**
     * @param msg Message.
     */
    private void save1(Message<JsonObject> msg) {
        msg.fail(HTTP_INTERNAL_ERROR, "Not implemented yet");
    }

    /**
     * @param msg Message.
     */
    private void delete1(Message<JsonObject> msg) {
        msg.fail(HTTP_INTERNAL_ERROR, "Not implemented yet");
    }

    /**
     * @param msg Message.
     */
    private void toggle1(Message<JsonObject> msg) {
        msg.fail(HTTP_INTERNAL_ERROR, "Not implemented yet");
    }

    /**
     * Get account by ID.
     *
     * @param accId Account ID.
     */
    public Account getById(UUID accId) {
        Account acc;

        try(Transaction ignored = txStart()) {
            acc = accountsTbl.load(accId);
        }

        return acc;
    }

    /**
     * Get account by email.
     *
     * @param email Account email.
     */
    public Account getByEmail(String email) {
        Account acc;

        try(Transaction ignored = txStart()) {
            acc = accountsTbl.getByIndex(email);
        }

        return acc;
    }

    /**
     * @return List of all users.
     */
    public JsonArray list() {
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
                    .put("activitiesTotal", 0)
                    .put("activitiesDetail", 0)
                );
            }
        });

        return res;
    }

    /**
     * Save notebook.
     *
     * @param account Account to save.
     * @return Saved account.
     */
    public Account save(Account account) {
        try (Transaction tx = txStart()) {
            accountsTbl.save(account);

            tx.commit();
        }

        return account;
    }

    /**
     * @return {@code true} if first user should be granted with admin permissions.
     */
    @SuppressWarnings("unchecked")
    public boolean shouldBeAdmin() {
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
     * @param accId Account ID.
     */
    public int delete(UUID accId) {
        int rmvCnt = 0;

        try (Transaction tx = txStart()) {
            Account acc = accountsTbl.delete(accId);

            if (acc != null)
                rmvCnt = 1;

            tx.commit();
        }

        return rmvCnt;
    }

    /**
     * @param userId User ID.
     * @param adminFlag Administration flag.
     */
    public void toggle(UUID userId, boolean adminFlag) {
        try (Transaction tx = txStart()) {
            Account acc = accountsTbl.load(userId);

            if (acc == null)
                throw new IllegalStateException("Account not found for id: " + userId);

            acc.admin(adminFlag);

            accountsTbl.save(acc);

            tx.commit();
        }
    }
}
