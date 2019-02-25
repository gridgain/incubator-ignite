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
 * ACtivation configuration.
 */
public class ActivationConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** By default activation link will be awailable for 24 hours. */
    private static final long DFLT_TIMEOUT = 24 * 60 * 60 * 1000;

    /** By default activation send email throttle is 3 minutes. */
    private static final long DFLT_SEND_TIMEOUT = 3 * 60 * 1000;

    /** Whether account should be activated by e-mail confirmation. */
    private boolean enabled;

    /** Activation link life time. */
    private long timeout = DFLT_TIMEOUT;

    /** Activation send email throttle. */
    private long sndTimeout = DFLT_SEND_TIMEOUT;

    /**
     * Empty constructor.
     */
    public ActivationConfiguration() {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param cc Configuration to copy.
     */
    public ActivationConfiguration(ActivationConfiguration cc) {
        enabled = cc.isEnabled();
        timeout = cc.getTimeout();
        sndTimeout = cc.getSendTimeout();
    }

    /**
     * @return {@code true} if new accounts should be activated via e-mail confirmation.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @param enabled Accounts activation required flag.
     * @return {@code this} for chaining.
     */
    public ActivationConfiguration setEnabled(boolean enabled) {
        this.enabled = enabled;

        return this;
    }

    /**
     * @return Activation link life time.
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     *
     * @param timeout Activation link life time.
     * @return {@code this} for chaining.
     */
    public ActivationConfiguration setTimeout(long timeout) {
        this.timeout = timeout;

        return this;
    }

    /**
     * @return Activation send email throttle.
     */
    public long getSendTimeout() {
        return sndTimeout;
    }

    /**
     * @param sndTimeout Activation send email throttle.
     * @return {@code this} for chaining.
     */
    public ActivationConfiguration setSendTimeout(long sndTimeout) {
        this.sndTimeout = sndTimeout;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ActivationConfiguration.class, this);
    }
}
