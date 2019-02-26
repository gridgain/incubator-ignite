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

package org.apache.ignite.console.auth;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.ZonedDateTime;
import java.util.UUID;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.PRNG;
import io.vertx.ext.auth.User;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Authentication with storing user information in Ignite.
 */
public class IgniteAuth implements AuthProvider {
    /** */
    private static final int ITERATIONS = 25000;

    /** */
    private static final int KEY_LEN = 512 * 8;

    /** */
    private static final String E_INVALID_CREDENTIALS = "Invalid email or password";

    /** Special key to check that first user should be granted admin rights. */
    private static final UUID FIRST_USER_MARKER_KEY = UUID.fromString("75744f56-59c4-4228-b07a-fabf4babc059");

    /** */
    private final Table<Account> accountsTbl;

    /** */
    private Ignite ignite;
    
    /** */
    private final PRNG rnd;

    /**
     * @param ignite Ignite.
     * @param vertx Vertx.
     */
    public IgniteAuth(Ignite ignite, Vertx vertx) {
        this.ignite = ignite;
        
        rnd = new PRNG(vertx);

        accountsTbl = new Table<Account>(ignite, "accounts")
            .addUniqueIndex(Account::email, (acc) -> "Account with email: " + acc.email() + " already registered");
    }

    /**
     * @return Salt for hashing.
     */
    private String generateSalt() {
        byte[] salt = new byte[32];

        rnd.nextBytes(salt);

        return U.byteArray2HexString(salt);
    }
    
    /**
     * Compute password hash.
     *
     * @param pwd Password to hash.
     * @param salt Salt to use.
     * @return Computed hash.
     * @throws GeneralSecurityException If failed to compute hash.
     */
    private String computeHash(String pwd, String salt) throws Exception {
        // TODO IGNITE-5617: How about to re-hash on first successful compare?
        PBEKeySpec spec = new PBEKeySpec(
            pwd.toCharArray(),
            salt.getBytes(StandardCharsets.UTF_8), // For compatibility with hashing data imported from NodeJS.
            ITERATIONS,
            KEY_LEN);

        SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");

        byte[] hash = skf.generateSecret(spec).getEncoded();

        return U.byteArray2HexString(hash);
    }

    /**
     * @param accId Account id.
     */
    Account account(UUID accId) throws IgniteAuthenticationException {
        if (accId == null)
            throw new IgniteAuthenticationException("Missing account identity");

        IgniteCache<UUID, Account> cache = accountsTbl.cache();

        Account account = cache.get(accId);

        if (account == null)
            throw new IgniteAuthenticationException("Failed to find account with identity: " + accId);

        return account;
    }

    /** {@inheritDoc} */
    @Override public void authenticate(JsonObject data, Handler<AsyncResult<User>> asyncResHnd) throws IgniteException {
        try {
            String email = data.getString("email");
            String pwd = data.getString("password");

            if (F.isEmpty(email) || F.isEmpty(pwd))
                throw new IgniteAuthenticationException(E_INVALID_CREDENTIALS);

            Account account;

            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                account = accountsTbl.getByIndex(email);
            }

            if (account == null)
                throw new IgniteAuthenticationException("Failed to find registered account");

            String hash = computeHash(data.getString("password"), account.salt());

            if (!account.hash().equals(hash))
                throw new IgniteAuthenticationException(E_INVALID_CREDENTIALS);

            asyncResHnd.handle(Future.succeededFuture(new ContextAccount(account)));
        }
        catch (Throwable e) {
            ignite.log().error("Failed to authenticate account", e);

            asyncResHnd.handle(Future.failedFuture(e));
        }
    }

    /**
     * @param body Sign up info.
     */
    @SuppressWarnings("unchecked")
    public Account registerAccount(JsonObject body) throws Exception {
        String salt = generateSalt();
        String hash = computeHash(body.getString("password"), salt);

        boolean admin;

        try(Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            IgniteCache cache = accountsTbl.cache();

            Object firstUserMarker = cache.get(FIRST_USER_MARKER_KEY);

            admin = firstUserMarker == null;

            if (admin)
                cache.put(FIRST_USER_MARKER_KEY, FIRST_USER_MARKER_KEY);

            tx.commit();
        }

        Account account = new Account(
            UUID.randomUUID(),
            body.getString("email"),
            body.getString("firstName"),
            body.getString("lastName"),
            body.getString("company"),
            body.getString("country"),
            body.getString("industry"),
            body.getBoolean("admin", admin),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            ZonedDateTime.now().toString(),
            "",
            "",
            "",
            false,
            salt,
            hash
        );





        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            accountsTbl.save(account);

            tx.commit();

            return account;
        }
    }
}
