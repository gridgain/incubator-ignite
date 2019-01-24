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

package org.apache.ignite.web.console.auth.impl;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.ZonedDateTime;
import java.util.UUID;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.naming.AuthenticationException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.PRNG;
import io.vertx.ext.auth.User;
import org.apache.commons.codec.binary.Hex;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.web.console.auth.IgniteAuth;
import org.apache.ignite.web.console.dto.Account;

/**
 * Authentication with storing user information in Ignite.
 */
public class IgniteAuthImpl implements IgniteAuth {
    /** */
    private static final int ITERATIONS = 25000;

    /** */
    private static final int KEY_LEN = 512 * 8;

    /** */
    private final Ignite ignite;

    /** */
    private final PRNG prnd;

    /**
     * @param vertx Vertex.
     * @param ignite Ignite.
     */
    public IgniteAuthImpl(Vertx vertx, Ignite ignite) {
        this.ignite = ignite;
        this.prnd = new PRNG(vertx);
    }

    /**
     * @return
     */
    private String salt() {
        byte[] salt = new byte[32];

        prnd.nextBytes(salt);

        return Hex.encodeHexString(salt);
    }

    /**
     * TODO javadocs
     *
     * @param pwd
     * @param salt
     * @return
     * @throws GeneralSecurityException
     */
    private String computeHash(String pwd, String salt) throws GeneralSecurityException {
        PBEKeySpec spec = new PBEKeySpec(
            pwd.toCharArray(),
            salt.getBytes(StandardCharsets.UTF_8), // For compatibility with hasing on previous implementation on NodeJS.
            ITERATIONS,
            KEY_LEN);

        SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");

        byte[] hash = skf.generateSecret(spec).getEncoded();

        return Hex.encodeHexString(hash);
    }

    /**
     *
     * @param json
     * @param key
     * @return
     */
    private String checkMandatoryField(JsonObject json, String key) throws AuthenticationException {
        String val = json.getString(key);

        if (F.isEmpty(val))
            throw new AuthenticationException("Mandatoty field missing: " + key);

        return key;
    }

    /**
     *
     * @param authInfo
     * @throws AuthenticationException
     */
    private void checkMandatoryFields(JsonObject authInfo) throws AuthenticationException {
        checkMandatoryField(authInfo, "email");
        checkMandatoryField(authInfo, "password");
    }

    /**
     *
     * @param authInfo
     * @return Account.
     * @throws Exception
     */
    private Account signUp(JsonObject authInfo) throws Exception {
        checkMandatoryFields(authInfo);

        IgniteCache<String, Account> cache = ignite.cache("accounts");

        String email = authInfo.getString("email");

        Account account = cache.get(email);

        if (account != null)
            throw new AuthenticationException("Account already exists");

        account = new Account();

        account._id = "5b9b5ad477670d001936692a"; // FIX!!!
        account.email = authInfo.getString("email");
        account.firstName = authInfo.getString("firstName");
        account.lastName = authInfo.getString("lastName");
        account.company = authInfo.getString("company");
        account.country = authInfo.getString("country");
        account.industry = authInfo.getString("industry");
        account.admin = true; // authInfo.getBoolean("admin");
        account.token = UUID.randomUUID().toString(); // ???
        account.resetPasswordToken = UUID.randomUUID().toString(); // ???
        account.registered = ZonedDateTime.now().toString();
        account.lastLogin = "";
        account.lastActivity = "";
        account.lastEvent = "";
        account.demoCreated = false;
        account.salt = salt();
        account.hash = computeHash(authInfo.getString("password"), account.salt);

        cache.put(email, account);

        return account;
    }

    /**
     *
     * @param authInfo
     * @return Account.
     * @throws Exception
     */
    private Account signIn(JsonObject authInfo) throws Exception {
        checkMandatoryFields(authInfo);

        String email = authInfo.getString("email");

        IgniteCache<String, Account> cache = ignite.cache("accounts");

        Account account = cache.get(email);

        if (account == null)
            throw new AuthenticationException("Invalid email or password");

        String hash = computeHash(account.salt, authInfo.getString("password"));

        if (!hash.equals(account.hash))
            throw new AuthenticationException("Invalid email or password");

        return account;
    }

    /** {@inheritDoc} */
    @Override public void authenticate(JsonObject authInfo, Handler<AsyncResult<User>> asyncResHnd) {
        try {
            Account account = (authInfo.getBoolean("signup"))
                ? signUp(authInfo)
                : signIn(authInfo);

            asyncResHnd.handle(Future.succeededFuture(new IgniteUser(account.json())));
        }
        catch (Throwable e) {
            asyncResHnd.handle(Future.failedFuture(e));
        }
    }
}
