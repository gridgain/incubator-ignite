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

import java.security.NoSuchAlgorithmException;
import javax.crypto.SecretKeyFactory;
import javax.xml.bind.DatatypeConverter;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import org.apache.ignite.Ignite;
import org.apache.ignite.web.console.auth.IgniteAuth;

/**
 * Authentication with storing user information in Ignite.
 */
public class IgniteAuthImpl implements IgniteAuth {
    /** */
    private final Ignite ignite;

    /**
     * @param vertx Vertex.
     * @param ignite Ignite.
     */
    public IgniteAuthImpl(Vertx vertx, Ignite ignite) {
        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override public void authenticate(JsonObject authInfo, Handler<AsyncResult<User>> asyncResHnd) {
        try {
            SecretKeyFactory f = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");

            byte[] salt = DatatypeConverter.parseHexBinary("a6fe2057bf1e2286c3ff4184fb44d28b068c0308017bfcea7b7a65a871a049b6");
            byte[] hash = DatatypeConverter.parseHexBinary("754b34b06796a900870d1f18f1bb3b702b1e61c9b85a8f0512d35bc9d57e0bdcc09fe195609f8bf804354d0b759c7ad7d9b2edd8ff2f0d106bd5a954f311f88da8d7274c82643034daac1ef1908e728ef2ea423ab027ecf933989dcb8676db7b268f52632ec9eb6bdaae7063cec3c91593347db17e1916704ae161aa6811e61eab24829ed0b4ee47ccd54eaa1d37d3e964d7014490f85629a9eb945473ff11f238a8a77b4e43750187ac3f03f97a34d074afaf7301115232448ebfe17ccedc8152213d2219797de458c9cf47ffc09cce3821a619244e219f82999b52fe4efa121832d42092442cfa1331783c0c097a29a92a5d07896fdcb0be998c6e44b8d654493506ca3d164e761f10a57696111e740e131129404cefd4cfe4363cedf15c0a7fd4f433de6f157ce6f14ff754b3a0a0a09b9fb565d72f29441076e1c93f1a334f453ca580688d955baaba73a797f2f582c7cb5e1addeda9af273e3bf0348aba44c1c2ddaf356a565bfd1b90521ee31eb0f51c256876b33c5ec6cfcb22a2fae2f2fc1dc2128e0e856fa1389c30fcf38b60b21b31a9504f00e84a089eaf9b999aec73df6699e9b4980d4cde17d9fb30d5246591690d81489221de9e30e6c83de454491a5119103a99d97ec59aa0da3d19e69d0a12782ff630e2935792ddb97c7b07e1d1c87180043c8c09ee7842c77a607565e7bfde402f9e0bb919f0869fd938");
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        asyncResHnd.handle(Future.succeededFuture(new IgniteUser("ignite")));
    }
}
