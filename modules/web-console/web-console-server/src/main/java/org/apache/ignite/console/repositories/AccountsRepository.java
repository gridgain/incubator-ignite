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

package org.apache.ignite.console.repositories;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.transactions.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Repository;

/**
 * Repository to work with accounts.
 */
@Repository
public class AccountsRepository extends AbstractRepository {
    /** Special key to check that first user should be granted admin rights. */
    private static final UUID FIRST_USER_MARKER_KEY = UUID.fromString("039d28e2-133d-4eae-ae2b-29d6db6d4974");

    /** Accounts collection. */
    private final Table<Account> accountsTbl;

    /**
     * @param ignite Ignite.
     */
    @Autowired
    public AccountsRepository(Ignite ignite) {
        super(ignite);

        accountsTbl = new Table<Account>(ignite, "accounts")
            .addUniqueIndex(Account::getUsername, (acc) -> "Account with email '" + acc.getUsername() + "' already registered");
    }

    /** {@inheritDoc} */
    @Override protected void initDatabase() {
        accountsTbl.cache();
    }

    /**
     * Get account by ID.
     *
     * @param accId Account ID.
     * @return Account.
     */
    public Account getById(UUID accId) {
        try (Transaction ignored = txStart()) {
            Account account = accountsTbl.load(accId);

            if (account == null)
                throw new IllegalStateException("Account not found with ID: " + accId);

            return account;
        }
    }

    /**
     * Get account by email.
     *
     * @param email Parameters in JSON format.
     * @return Account.
     */
    public Account getByEmail(String email) {
        try (Transaction ignored = txStart()) {
            Account account = accountsTbl.getByIndex(email);

            if (account == null)
                throw new UsernameNotFoundException(email);

            return account;
        }
        catch (IgniteException e) {
            throw new UsernameNotFoundException(email, e);
        }
    }

    /**
     * Save account.
     *
     * @param account Account to save.
     */
    @SuppressWarnings("unchecked")
    public Account create(Account account) {
        try (Transaction tx = txStart()) {
            IgniteCache cache = accountsTbl.cache();

            if (accountsTbl.getByIndex(account.getUsername()) != null)
                throw new IgniteException("Account with email already exists: " + account.getUsername());

            Object firstUserMarker = cache.getAndPutIfAbsent(FIRST_USER_MARKER_KEY, account.id());

            account.admin(firstUserMarker == null);

            save(account);

            tx.commit();

            return account;
        }
    }

    /**
     * Save account.
     *
     * @param account Account to save.
     */
    public void save(Account account) {
        try (Transaction tx = txStart()) {
            accountsTbl.save(account);

            tx.commit();
        }
    }

    /**
     * Delete account.
     *
     * @param accId Account ID.
     * @return Number of removed accounts.
     */
    public int delete(UUID accId) {
        int rmvCnt = 0;

        try (Transaction tx = txStart()) {
            Account account = accountsTbl.delete(accId);

            if (account != null)
                rmvCnt = 1;

            tx.commit();
        }

        return rmvCnt;
    }

    /**
     * @return List of accounts.
     */
    public List<Account> list() {
        IgniteCache<UUID, Account> cache = accountsTbl.cache();

        return cache
            .query(new ScanQuery<UUID, Object>())
            .getAll()
            .stream()
            .map(Cache.Entry::getValue)
            .filter(item -> item instanceof Account)
            .map(item -> (Account)item)
            .collect(Collectors.toList());
    }
}
