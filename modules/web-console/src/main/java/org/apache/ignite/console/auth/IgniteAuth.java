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
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.security.auth.login.AccountException;
import javax.security.auth.login.CredentialException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.PRNG;
import io.vertx.ext.auth.User;
import org.apache.commons.codec.binary.Hex;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.console.db.CacheHolder;
import org.apache.ignite.console.dto.Account;
import org.apache.ignite.internal.util.typedef.F;

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

    /** */
    private final CacheHolder<String, Account> accounts;

    /** */
    private final PRNG rnd;

    /**
     * @param ignite Ignite.
     * @param vertx Vertx.
     */
    public IgniteAuth(Ignite ignite, Vertx vertx) {
        accounts = new CacheHolder<>(ignite, "wc_accounts");
        rnd = new PRNG(vertx);
    }

    /**
     * @return Salt for hashing.
     */
    private String salt() {
        byte[] salt = new byte[32];

        rnd.nextBytes(salt);

        return Hex.encodeHexString(salt);
    }
    /**
     * Compute password hash.
     *
     * @param pwd Password to hash.
     * @param salt Salt to use.
     * @return Computed hash.
     * @throws GeneralSecurityException If failed to compute hash.
     */
    private String computeHash(String pwd, String salt) throws GeneralSecurityException {
        // TODO IGNITE-5617: How about re-hash on first successful compare.
        PBEKeySpec spec = new PBEKeySpec(
            pwd.toCharArray(),
            salt.getBytes(StandardCharsets.UTF_8), // For compatibility with hashing data imported from NodeJS.
            ITERATIONS,
            KEY_LEN);

        SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");

        byte[] hash = skf.generateSecret(spec).getEncoded();

        return Hex.encodeHexString(hash);
    }

    /** {@inheritDoc} */
    @Override public void authenticate(JsonObject authInfo, Handler<AsyncResult<User>> asyncResHnd) {
        try {
            checkCredentials(authInfo);

            // TODO IGNITE-5617 vertx.executeBlocking(); Auth is a possibly long operation

            Account account = authInfo.getBoolean("signup", false)
                ? signUp(authInfo)
                : signIn(authInfo);

            asyncResHnd.handle(Future.succeededFuture(new IgniteUser(account.principal())));
        }
        catch (Throwable e) {
            asyncResHnd.handle(Future.failedFuture(e));
        }
    }

    /**
     * @param authInfo JSON object with authentication info.
     * @throws CredentialException If email or password not found.
     */
    private void checkCredentials(JsonObject authInfo) throws CredentialException {
        String email = authInfo.getString("email");
        String pwd = authInfo.getString("password");

        if (F.isEmpty(email) || F.isEmpty(pwd))
            throw new CredentialException(E_INVALID_CREDENTIALS);
    }

    /**
     * TODO IGNITE-5617 Do this method in transaction?
     * Sign up.
     *
     * @param authInfo JSON with authentication info.
     * @return Account.
     * @throws GeneralSecurityException If failed to authenticate.
     */
    private Account signUp(JsonObject authInfo) throws GeneralSecurityException {
        IgniteCache<String, Account> cache = accounts.prepare();

        String email = authInfo.getString("email");

        Account account = cache.get(email);

        if (account != null)
            throw new AccountException("Account already exists");

        String salt = salt();
        String hash = computeHash(authInfo.getString("password"), salt);

        account = new Account(
            UUID.randomUUID(),
            authInfo.getString("email"),
            authInfo.getString("firstName"),
            authInfo.getString("lastName"),
            authInfo.getString("company"),
            authInfo.getString("country"),
            authInfo.getString("industry"),
            authInfo.getBoolean("admin", false),
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

        cache.put(email, account);

//        Space space = new Space(
//            UUID.randomUUID(),
//            "Personal space",
//            account.id(),
//            false
//        );
//
//        ignite.cache("wc_spaces").put(space.id(), space);

        return account;
    }

    /**
     * @param authInfo JSON with authentication info.
     * @return Account.
     * @throws GeneralSecurityException If failed to authenticate.
     */
    private Account signIn(JsonObject authInfo) throws GeneralSecurityException {
        IgniteCache<String, Account> cache = accounts.prepare();

        String email = authInfo.getString("email");

        Account account = cache.get(email);

        if (account == null)
            throw new CredentialException(E_INVALID_CREDENTIALS);

        String hash = computeHash(authInfo.getString("password"), account.salt());

        if (!hash.equals(account.hash()))
            throw new CredentialException(E_INVALID_CREDENTIALS);

        return account;
    }
}
