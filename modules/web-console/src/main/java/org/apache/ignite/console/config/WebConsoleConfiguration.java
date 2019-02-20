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

package org.apache.ignite.console.config;

/**
 * Configuration of Web Console.
 */
public class WebConsoleConfiguration {
    /** Path to key store. */
    private String keyStore;

    /** Optional password for key store. */
    private String keyStorePwd;

    /** Path to trust store. */
    private String trustStore;

    /** Optional password for trust store. */
    private String trustStorePwd;

    /** Optional comma-separated list of SSL cipher suites. */
    private String cipherSuites;

    /** Authentication required flag. */
    private boolean clientAuth;

    /** Path to static content. */
    private String webRoot;

    /** Default port. */
    private int port = 3000;

    /**
     * @return Path to key store.
     */
    public String getKeyStore() {
        return keyStore;
    }

    /**
     * @param keyStore Path to key store.
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setKeyStore(String keyStore) {
        this.keyStore = keyStore;

        return this;
    }

    /**
     * @return Key store password.
     */
    public String getKeyStorePassword() {
        return keyStorePwd;
    }

    /**
     * @param keyStorePwd Key store password.
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setKeyStorePassword(String keyStorePwd) {
        this.keyStorePwd = keyStorePwd;

        return this;
    }

    /**
     * @return Path to trust store.
     */
    public String getTrustStore() {
        return trustStore;
    }

    /**
     * @param trustStore Path to trust store.
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setTrustStore(String trustStore) {
        this.trustStore = trustStore;

        return this;
    }

    /**
     * @return Trust store password.
     */
    public String getTrustStorePassword() {
        return trustStorePwd;
    }

    /**
     * @param trustStorePwd Trust store password.
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setTrustStorePassword(String trustStorePwd) {
        this.trustStorePwd = trustStorePwd;

        return this;
    }

    /**
     * @return SSL cipher suites.
     */
    public String getCipherSuites() {
        return cipherSuites;
    }

    /**
     * @param cipherSuites SSL cipher suites.
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setCipherSuites(String cipherSuites) {
        this.cipherSuites = cipherSuites;

        return this;
    }

    /**
     * @return {@code true} if authentication required.
     */
    public boolean isClientAuth() {
        return clientAuth;
    }

    /**
     * @param clientAuth Authentication required flag.
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setClientAuth(boolean clientAuth) {
        this.clientAuth = clientAuth;

        return this;
    }

    /**
     * @return Path to static content
     */
    public String getWebRoot() {
        return webRoot;
    }

    /**
     * @param webRoot Path to static content.
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setWebRoot(String webRoot) {
        this.webRoot = webRoot;

        return this;
    }

    /**
     * @return Port to listen.
     */
    public int getPort() {
        return port;
    }

    /**
     * @param port Port to listen.
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setPort(int port) {
        this.port = port;

        return this;
    }
}
