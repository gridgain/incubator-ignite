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

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Mail configuration.
 */
public class MailConfiguration {
    /** Text to show in mail header. */
    private String greeting;

    /** Text to show in mail footer. */
    private String sign;

    /** Address mail will be send from. */
    private String from;

    /** Mail server host. */
    private String host;

    /** User name that will be used to authenticate on mail server. */
    private String user;

    /** Password that will be used to authenticate on mail server. */
    private String pwd;

    /**
     * Empty constructor.
     */
    public MailConfiguration() {
        // No-op.
    }

    /**
     * Copy constructor.
     * 
     * @param cc Configuration to copy.
     */
    public MailConfiguration(MailConfiguration cc) {
        greeting = cc.getGreeting();
        sign = cc.getSign();
        from = cc.getFrom();
        host = cc.getHost();
        user = cc.getUser();
        pwd = cc.getPassword();
    }

    /**
     * @return Text to show in mail header.
     */
    public String getGreeting() {
        return greeting;
    }

    /**
     * @param greeting Text to show in mail header.
     * @return {@code this} for chaining.
     */
    public MailConfiguration setGreeting(String greeting) {
        this.greeting = greeting;
        
        return this;
    }

    /**
     * @return Text to show in mail footer.
     */
    public String getSign() {
        return sign;
    }

    /**
     * @param sign Text to show in mail footer.
     * @return {@code this} for chaining.
     */
    public MailConfiguration setSign(String sign) {
        this.sign = sign;

        return this;
    }

    /**
     * @return Address mail will be send from.
     */
    public String getFrom() {
        return from;
    }

    /**
     * @param from Address mail will be send from.
     * @return {@code this} for chaining.
     */
    public MailConfiguration setFrom(String from) {
        this.from = from;

        return this;
    }

    /**
     * @return Mail server host.
     */
    public String getHost() {
        return host;
    }

    /**
     * @param host Mail server host.
     * @return {@code this} for chaining.
     */
    public MailConfiguration setHost(String host) {
        this.host = host;

        return this;
    }

    /**
     * @return User name that will be used to authenticate on mail server.
     */
    public String getUser() {
        return user;
    }

    /**
     * @param user User name that will be used to authenticate on mail server.
     * @return {@code this} for chaining.
     */
    public MailConfiguration setUser(String user) {
        this.user = user;

        return this;
    }

    /**
     * @return Password that will be used to authenticate on mail server.
     */
    public String getPassword() {
        return pwd;
    }

    /**
     * @param pwd Password that will be used to authenticate on mail server.
     * @return {@code this} for chaining.
     */
    public MailConfiguration setPassword(String pwd) {
        this.pwd = pwd;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MailConfiguration.class, this);
    }
}
