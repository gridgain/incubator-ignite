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

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Configuration of Web Console.
 */
public class WebConsoleConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int DFLT_PORT = 3000;

    /** Path to static content. */
    private String webRoot;

    /** Web Console port. */
    private int port = DFLT_PORT;

    /** Folder with Web Agent binaries. */
    private String agentFolderName;

    /** Web Agent file name. */
    private String agentFileName;

    /** */
    private AccountConfiguration accountCfg;

    /** */
    private MailConfiguration mailCfg;

    /** */
    private SslConfiguration sslCfg;

    /**
     * Empty constructor.
     */
    public WebConsoleConfiguration() {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param cc Configuration to copy.
     */
    public WebConsoleConfiguration(WebConsoleConfiguration cc) {
       webRoot = cc.getWebRoot();
       port = cc.getPort();
       agentFolderName = cc.getAgentFolderName();
       agentFileName = cc.getAgentFileName();
       accountCfg = cc.getAccountConfiguration();
       mailCfg = cc.getMailConfiguration();
       sslCfg = cc.getSslConfiguration();
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

    /**
     * @return Folder with Web Agent binaries.
     */
    public String getAgentFolderName() {
        return agentFolderName;
    }

    /**
     *
     * @param agentFolderName Folder with Web Agent binaries.
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setAgentFolderName(String agentFolderName) {
        this.agentFolderName = agentFolderName;

        return this;
    }

    /**
     * @return Web Agent file name.
     */
    public String getAgentFileName() {
        return agentFileName;
    }

    /**
     * @param agentFileName Web Agent file name.
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setAgentFileName(String agentFileName) {
        this.agentFileName = agentFileName;

        return this;
    }

    /**
     * @return Account configuration.
     */
    public AccountConfiguration getAccountConfiguration() {
        return accountCfg;
    }

    /**
     * @param activationCfg Account configuration.
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setActivationConfiguration(AccountConfiguration activationCfg) {
        this.accountCfg = activationCfg;

        return this;
    }

    /**
     * @return Mail configuration.
     */
    public MailConfiguration getMailConfiguration() {
        return mailCfg;
    }

    /**
     * @param mailCfg Mail configuration.
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setMailConfiguration(MailConfiguration mailCfg) {
        this.mailCfg = mailCfg;

        return this;
    }

    /**
     * @return SSL configuration.
     */
    public SslConfiguration getSslConfiguration() {
        return sslCfg;
    }

    /**
     * @param sslCfg SSL configuration.
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setSslConfiguration(SslConfiguration sslCfg) {
        this.sslCfg = sslCfg;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WebConsoleConfiguration.class, this);
    }
}
