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
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.naming.AuthenticationException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.auth.PRNG;
import io.vertx.ext.auth.User;
import org.apache.commons.codec.binary.Hex;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.console.common.Addresses;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.console.common.Consts;
import org.apache.ignite.console.dto.Account;

/**
 * Authentication with storing user information in Ignite.
 */
public class IgniteAuth implements AuthProvider {
    /** */
    private static final int ITERATIONS = 25000;

    /** */
    private static final int KEY_LEN = 512 * 8;

    /** */
    private final Vertx vertx;

    /**
     * @param vertx Vertx.
     * @return Authenticator.
     */
    public static IgniteAuth create(Vertx vertx) {
        return new IgniteAuth(vertx);
    }

    /**
     * @param vertx Vertex.
     */
    public IgniteAuth(Vertx vertx) {
        this.vertx = vertx;
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
            checkMandatoryFields(authInfo);

            String addr = authInfo.getBoolean("signup", false) ? Addresses.IGNITE_SIGN_UP : Addresses.IGNITE_SIGN_IN;

            vertx.eventBus().<JsonObject>send(addr, authInfo, asyncRes -> {
                if (asyncRes.succeeded())
                    asyncResHnd.handle(Future.succeededFuture(new IgniteUser(asyncRes.result().body())));
                else
                    asyncResHnd.handle(Future.failedFuture(asyncRes.cause()));
            });
        }
        catch (Throwable e) {
            asyncResHnd.handle(Future.failedFuture(e));
        }
    }
}
