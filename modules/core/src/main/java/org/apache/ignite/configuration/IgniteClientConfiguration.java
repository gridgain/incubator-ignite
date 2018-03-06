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

package org.apache.ignite.configuration;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.IgniteConfigurationException;
import org.apache.ignite.internal.TcpAddressConfigLines;
import org.apache.ignite.internal.thinclient.TcpIgniteClient;

/**
 * {@link TcpIgniteClient} configuration.
 */
public final class IgniteClientConfiguration implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = -427423815608464519L;

    /** @serial Server addresses. */
    private final TcpAddressConfigLines addrs = new TcpAddressConfigLines();

    /** @serial Tcp no delay. */
    private boolean tcpNoDelay = true;

    /** @serial Timeout. 0 means infinite. */
    private int timeout = 0;

    /** @serial Send buffer size. 0 means system default. */
    private int sndBufSize = 32 * 1024;

    /** @serial Receive buffer size. 0 means system default. */
    private int rcvBufSize = 32 * 1024;

    /** @serial Configuration for Ignite binary objects. */
    private BinaryConfiguration binaryCfg;

    /** @serial Ssl mode. */
    private SslMode sslMode = SslMode.DISABLED;

    /** @serial Ssl client certificate key store path. */
    private String sslClientCertKeyStorePath;

    /** @serial Ssl client certificate key store password. */
    private String sslClientCertKeyStorePwd;

    /** @serial Ssl trust certificate key store path. */
    private String sslTrustCertKeyStorePath;

    /** @serial Ssl trust certificate key store password. */
    private String sslTrustCertKeyStorePwd;

    /** @serial Ssl client certificate key store type. */
    private String sslClientCertKeyStoreType;

    /** @serial Ssl trust certificate key store type. */
    private String sslTrustCertKeyStoreType;

    /** @serial Ssl key algorithm. */
    private String sslKeyAlgorithm;

    /** @serial Flag indicating if certificate validation errors should be ignored. */
    private boolean sslTrustAll;

    /** @serial Ssl protocol. */
    private SslProtocol sslProto = SslProtocol.TLS;

    /** @serial Ssl context factory. */
    private Factory<SSLContext> sslCtxFactory;

    /** @serial User name. */
    private String userName;

    /** @serial User password. */
    private String userPwd;

    /**
     * @return Host addresses.
     */
    public Collection<String> getAddresses() {
        return addrs.getAddressLines();
    }

    /**
     * @return Host addresses.
     */
    public Collection<InetSocketAddress> getParsedAddresses() {
        return addrs.getAddresses();
    }

    /**
     * @param addrs Host addresses.
     */
    public IgniteClientConfiguration setAddresses(String... addrs) {
        if (addrs == null || addrs.length == 0)
            throw new IllegalArgumentException("addrs must be specified");

        try {
            this.addrs.set(Arrays.asList(addrs));
        }
        catch (IgniteConfigurationException e) {
            throw new IllegalArgumentException("addrs", e);
        }

        return this;
    }

    /**
     * @return Whether Nagle's algorithm is enabled.
     */
    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * @param tcpNoDelay whether Nagle's algorithm is enabled.
     */
    public IgniteClientConfiguration tcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;

        return this;
    }

    /**
     * @return Send/receive timeout in milliseconds.
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * @param timeout Send/receive timeout in milliseconds.
     */
    public IgniteClientConfiguration setTimeout(int timeout) {
        this.timeout = timeout;

        return this;
    }

    /**
     * @return Send buffer size.
     */
    public int getSendBufferSize() {
        return sndBufSize;
    }

    /**
     * @param sndBufSize Send buffer size.
     */
    public IgniteClientConfiguration setSendBufferSize(int sndBufSize) {
        this.sndBufSize = sndBufSize;

        return this;
    }

    /**
     * @return Send buffer size.
     */
    public int getReceiveBufferSize() {
        return rcvBufSize;
    }

    /**
     * @param rcvBufSize Send buffer size.
     */
    public IgniteClientConfiguration setReceiveBufferSize(int rcvBufSize) {
        this.rcvBufSize = rcvBufSize;

        return this;
    }

    /**
     * @return Configuration for Ignite Binary objects.
     */
    public BinaryConfiguration getBinaryConfiguration() {
        return binaryCfg;
    }

    /**
     * @param binaryCfg Configuration for Ignite Binary objects.
     */
    public IgniteClientConfiguration setBinaryConfiguration(BinaryConfiguration binaryCfg) {
        this.binaryCfg = binaryCfg;

        return this;
    }

    /**
     * @return SSL mode.
     */
    public SslMode getSslMode() {
        return sslMode;
    }

    /**
     * @param sslMode SSL mode.
     */
    public IgniteClientConfiguration setSslMode(SslMode sslMode) {
        this.sslMode = sslMode;

        return this;
    }

    /**
     * @return Ssl client certificate key store path.
     */
    public String getSslClientCertificateKeyStorePath() {
        return sslClientCertKeyStorePath;
    }

    /**
     * @param newVal Ssl client certificate key store path.
     */
    public IgniteClientConfiguration setSslClientCertificateKeyStorePath(String newVal) {
        sslClientCertKeyStorePath = newVal;

        return this;
    }

    /**
     * @return Ssl client certificate key store password.
     */
    public String getSslClientCertificateKeyStorePassword() {
        return sslClientCertKeyStorePwd;
    }

    /**
     * @param newVal Ssl client certificate key store password.
     */
    public IgniteClientConfiguration setSslClientCertificateKeyStorePassword(String newVal) {
        sslClientCertKeyStorePwd = newVal;

        return this;
    }

    /**
     * @return Ssl client certificate key store type.
     */
    public String getSslClientCertificateKeyStoreType() {
        return sslClientCertKeyStoreType;
    }

    /**
     * @param newVal Ssl client certificate key store type.
     */
    public IgniteClientConfiguration setSslClientCertificateKeyStoreType(String newVal) {
        sslClientCertKeyStoreType = newVal;

        return this;
    }

    /**
     * @return Ssl trust certificate key store path.
     */
    public String getSslTrustCertificateKeyStorePath() {
        return sslTrustCertKeyStorePath;
    }

    /**
     * @param newVal Ssl trust certificate key store path.
     */
    public IgniteClientConfiguration setSslTrustCertificateKeyStorePath(String newVal) {
        sslTrustCertKeyStorePath = newVal;

        return this;
    }

    /**
     * @return Ssl trust certificate key store password.
     */
    public String getSslTrustCertificateKeyStorePassword() {
        return sslTrustCertKeyStorePwd;
    }

    /**
     * @param newVal Ssl trust certificate key store password.
     */
    public IgniteClientConfiguration setSslTrustCertificateKeyStorePassword(String newVal) {
        sslTrustCertKeyStorePwd = newVal;

        return this;
    }

    /**
     * @return Ssl trust certificate key store type.
     */
    public String getSslTrustCertificateKeyStoreType() {
        return sslTrustCertKeyStoreType;
    }

    /**
     * @param newVal Ssl trust certificate key store type.
     */
    public IgniteClientConfiguration setSslTrustCertificateKeyStoreType(String newVal) {
        sslTrustCertKeyStoreType = newVal;

        return this;
    }

    /**
     * @return Ssl key algorithm.
     */
    public String getSslKeyAlgorithm() {
        return sslKeyAlgorithm;
    }

    /**
     * @param newVal Ssl key algorithm.
     */
    public IgniteClientConfiguration setSslKeyAlgorithm(String newVal) {
        sslKeyAlgorithm = newVal;

        return this;
    }

    /**
     * @return Flag indicating if certificate validation errors should be ignored.
     */
    public boolean isSslTrustAll() {
        return sslTrustAll;
    }

    /**
     * @param newVal Flag indicating if certificate validation errors should be ignored.
     */
    public IgniteClientConfiguration setSslTrustAll(boolean newVal) {
        sslTrustAll = newVal;

        return this;
    }

    /**
     * @return Ssl protocol.
     */
    public SslProtocol getSslProtocol() {
        return sslProto;
    }

    /**
     * @param newVal Ssl protocol.
     */
    public IgniteClientConfiguration setSslProtocol(SslProtocol newVal) {
        sslProto = newVal;

        return this;
    }

    /**
     * @return User name.
     */
    public String getUserName() {
        return userName;
    }

    /**
     * @param newVal User name.
     */
    public IgniteClientConfiguration setUserName(String newVal) {
        userName = newVal;

        return this;
    }

    /**
     * @return User password.
     */
    public String getUserPassword() {
        return userPwd;
    }

    /**
     * @param newVal User password.
     */
    public IgniteClientConfiguration setUserPassword(String newVal) {
        userPwd = newVal;

        return this;
    }

    /**
     * @return SSL Context Factory.
     */
    public Factory<SSLContext> getSslContextFactory() {
        return sslCtxFactory;
    }

    /**
     * @param newVal SSL Context Factory.
     */
    public IgniteClientConfiguration setSslContextFactory(Factory<SSLContext> newVal) {
        this.sslCtxFactory = newVal;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (!(obj instanceof IgniteClientConfiguration))
            return false;

        IgniteClientConfiguration other = (IgniteClientConfiguration)obj;

        return Objects.equals(addrs, other.addrs) &&
            tcpNoDelay == other.tcpNoDelay &&
            timeout == other.timeout &&
            sndBufSize == other.sndBufSize &&
            rcvBufSize == other.rcvBufSize;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(addrs, tcpNoDelay, timeout, sndBufSize, rcvBufSize);
    }
}
