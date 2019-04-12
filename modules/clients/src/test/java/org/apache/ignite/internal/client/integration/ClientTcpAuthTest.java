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

package org.apache.ignite.internal.client.integration;

import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientData;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientPredicate;
import org.apache.ignite.internal.client.GridClientProtocol;
import org.apache.ignite.internal.client.ssl.GridSslContextFactory;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.spi.discovery.tcp.TestAuthPluginProvider;
import org.apache.ignite.spi.discovery.tcp.TestAuthProcessor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

/**
 * Tests the Authorization in client-server communication.
 */
public class ClientTcpAuthTest extends ClientAbstractSelfTest {

    @Override
    protected GridClientConfiguration clientConfiguration() throws GridClientException {
        GridClientConfiguration cliCfg = super.clientConfiguration();
        cliCfg.setSecurityCredentialsProvider(
                new SecurityCredentialsBasicProvider(
                        new SecurityCredentials("user", "password")));
        return cliCfg;
    }

    @Test
    public void testAuthorization() throws Exception{
        GridClient client = client();

        GridClientData data = client.data("cache");
        data.put("key", "val");
        assertEquals("val", data.get("key"));

        GridClientCompute compute = client.compute().projection(new GridClientPredicate<GridClientNode>() {
            @Override public boolean apply(GridClientNode e) {
                return true;
            }
        });

        Integer result = compute.execute(getTaskName(), Collections.singletonList("taskArg"));
        assertNotNull(result);
        assertEquals(7, result.intValue());
    }

    /** {@inheritDoc} */
    @Override protected GridClientProtocol protocol() {
        return GridClientProtocol.TCP;
    }

    /** {@inheritDoc} */
    @Override protected String serverAddress() {
        return HOST + ":" + BINARY_PORT;
    }

    /** {@inheritDoc} */
    @Override protected boolean useSsl() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected GridSslContextFactory sslContextFactory() {
        return null;
    }

    @BeforeClass
    public static void enableAuth(){
        TestAuthPluginProvider.enabled = true;
        TestAuthProcessor.enabled = true;
    }

    @AfterClass
    public static void disableAuth(){
        TestAuthPluginProvider.enabled = false;
        TestAuthProcessor.enabled = false;
    }

}