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

package org.apache.ignite.client.system;

import org.apache.ignite.*;
import org.apache.ignite.client.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.ssl.*;
import org.junit.*;

import java.util.function.*;

import static org.junit.Assert.*;

/**
 * Thin client security test.
 */
public class SecurityTest {
    /** Test SSL/TLS encryption. */
    @Test
    public void testEncryption() throws Exception {
        // Server-side security configuration
        IgniteConfiguration srvCfg = Config.getServerConfiguration();

        SslContextFactory sslCfg = new SslContextFactory();

        Function<String, String> rsrcPath = rsrc -> SecurityTest.class.getResource(rsrc).getPath();

        sslCfg.setKeyStoreFilePath(rsrcPath.apply("/server.jks"));
        sslCfg.setKeyStorePassword("123456".toCharArray());
        sslCfg.setTrustStoreFilePath(rsrcPath.apply("/trust.jks"));
        sslCfg.setTrustStorePassword("123456".toCharArray());

        srvCfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
            .setSslEnabled(true)
            .setSslClientAuth(true)
        );

        srvCfg.setSslContextFactory(sslCfg);

        // Client-side security configuration
        IgniteClientConfiguration clientCfg = new IgniteClientConfiguration(Config.HOST);

        try (Ignite ignored = Ignition.start(srvCfg)) {
            boolean failed;

            try (IgniteClient client = IgniteClient.start(clientCfg)) {
                client.<Integer, String>cache(Config.DEFAULT_CACHE_NAME).put(1, "1");

                failed = false;
            }
            catch (Exception ex) {
                failed = true;
            }

            assertTrue("Client connection without SSL must fail", failed);

            clientCfg
                .setSslMode(SslMode.REQUIRE)
                .setSslClientCertificateKeyStorePath(rsrcPath.apply("/client.jks"))
                .setSslClientCertificateKeyStoreType("JKS")
                .setSslClientCertificateKeyStorePassword("123456")
                .setSslTrustCertificateKeyStorePath(rsrcPath.apply("/trust.jks"))
                .setSslTrustCertificateKeyStoreType("JKS")
                .setSslTrustCertificateKeyStorePassword("123456")
                .setSslKeyAlgorithm("SunX509")
                .setSslTrustAll(false)
                .setSslProtocol("TLS");

            try (IgniteClient client = IgniteClient.start(clientCfg)) {
                client.<Integer, String>cache(Config.DEFAULT_CACHE_NAME).put(1, "1");
            }
        }
    }

    /** Test authentication. */
    @Test
    public void testAuthentication() {
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration())) {
            Function<Credentials, Exception> authenticate = cred -> {
                IgniteClientConfiguration clientCfg = new IgniteClientConfiguration(Config.HOST)
                    .setCredentialsProvider(() -> cred);

                try (IgniteClient client = IgniteClient.start(clientCfg)) {
                    client.getOrCreateCache("testAuthentication");
                }
                catch (Exception e) {
                    return e;
                }

                return null;
            };

            assertTrue(
                "Authentication with invalid credentials succeeded",
                authenticate.apply(new Credentials("bad-user", "bad-password")) instanceof org.apache.ignite.client.IgniteAuthenticationException
            );

            Credentials validCred = new Credentials("user", "password");

            // TODO: addUser(validCred)
            // (the feature is not yet implemented on the server side. Another team will complete this as part of the
            // server side development).
            assertNull(
                "Authentication with valid credentials failed",
                authenticate.apply(validCred)
            );
        }
    }
}
