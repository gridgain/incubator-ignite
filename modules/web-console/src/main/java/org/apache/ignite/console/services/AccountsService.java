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

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

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
    public boolean shouldBeAdmin() {
        /*
        try(Transaction tx = txStart()) {
            IgniteCache cache = accountsTbl.cache();

            Object firstUserMarker = cache.get(FIRST_USER_MARKER_KEY);

            admin = firstUserMarker == null;

            if (admin)
                cache.put(FIRST_USER_MARKER_KEY, FIRST_USER_MARKER_KEY);

            tx.commit();
        }

         */

        return false;
    }

    /**
     * Remove account.
     *
     * @param accId Avvount ID.
     */
    public int delete(UUID accId) {
        int rmvCnt = 0;

        try (Transaction tx = txStart()) {
//            Account acc = accountsTbl.delete(notebookId);
//
//            if (acc != null)
//                rmvCnt = 1;

            tx.commit();
        }

        return rmvCnt;
    }
}
