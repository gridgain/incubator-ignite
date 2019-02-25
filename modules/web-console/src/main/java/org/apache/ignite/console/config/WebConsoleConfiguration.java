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

    /** Path to static content. */
    private String webRoot;

    /** Default port. */
    private int port = 3000;

    /** */
    private boolean disableSignup;

    /** */
    private ActivationConfiguration activationCfg;

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
       disableSignup = cc.isDisableSignup();
       activationCfg = cc.getActivationConfiguration();
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WebConsoleConfiguration.class, this);
    }

    /**
     * @return
     */
    public boolean isDisableSignup() {
        return disableSignup;
    }

    /**
     * @param disableSignup
     * @return
     */
    public WebConsoleConfiguration  setDisableSignup(boolean disableSignup) {
        this.disableSignup = disableSignup;

        return this;
    }

    /**
     * @return
     */
    public ActivationConfiguration getActivationConfiguration() {
        return activationCfg;
    }

    /**
     * @param activationCfg
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setActivationConfiguration(ActivationConfiguration activationCfg) {
        this.activationCfg = activationCfg;

        return this;
    }

    /**
     * @return
     */
    public MailConfiguration getMailConfiguration() {
        return mailCfg;
    }

    /**
     * @param mailCfg
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setMailConfiguration(MailConfiguration mailCfg) {
        this.mailCfg = mailCfg;

        return this;
    }

    /**
     * @return
     */
    public SslConfiguration getSslConfiguration() {
        return sslCfg;
    }

    /**
     * @param sslCfg
     * @return {@code this} for chaining.
     */
    public WebConsoleConfiguration setSslConfiguration(SslConfiguration sslCfg) {
        this.sslCfg = sslCfg;

        return this;
    }
}
