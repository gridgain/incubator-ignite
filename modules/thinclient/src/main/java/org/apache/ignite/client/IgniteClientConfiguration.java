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

package org.apache.ignite.client;

import java.net.InetSocketAddress;
import java.util.stream.*;

import org.apache.ignite.configuration.*;

import java.util.*;

/**
 * {@link TcpIgniteClient} configuration.
 */
public final class IgniteClientConfiguration {
    /** Default host. */
    private static final String DFLT_HOST = "127.0.0.1";

    /** Default port. */
    private static final int DFLT_PORT = 10800;

    /** Server addresses. */
    private List<InetSocketAddress> addrs = new ArrayList<>();

    /** Tcp no delay. */
    private boolean tcpNoDelay = false;

    /** Timeout. 0 means infinite. */
    private int timeout = 0;

    /** Send buffer size. 0 means system default. */
    private int sndBufSize = 0;

    /** Receive buffer size. 0 means system default. */
    private int rcvBufSize = 0;

    /** Configuration for Ignite binary objects. */
    private BinaryConfiguration binaryCfg;

    /** Ssl mode. */
    private SslMode sslMode = SslMode.DISABLE;

    /** Ssl client certificate key store path. */
    private String sslClientCertKeyStorePath;

    /** Ssl client certificate key store password. */
    private String sslClientCertKeyStorePwd;

    /** Ssl trust certificate key store path. */
    private String sslTrustCertKeyStorePath;

    /** Ssl trust certificate key store password. */
    private String sslTrustCertKeyStorePwd;

    /** Ssl client certificate key store type. */
    private String sslClientCertKeyStoreType;

    /** Ssl trust certificate key store type. */
    private String sslTrustCertKeyStoreType;

    /** Ssl key algorithm. */
    private String sslKeyAlgorithm;

    /** Flag indicating if certificate validation errors should be ignored. */
    private boolean sslTrustAll;

    /** Ssl protocol. */
    private String sslProto;

    /** Credential provider. */
    private CredentialsProvider credProvider;

    /**
     * Constructor to connect to single cluster node. No fault tolerance: the connection is lost if the host goes down.
     *
     * @param host name or IP address of an Ignite server node to connect to.
     */
    public IgniteClientConfiguration(String host) {
        setHost(host);
    }

    /**
     * Constructor to provide fault tolerance: the client connects to the first node in the list and automatically
     * re-connects to the next node in the list if the current host goes down.
     *
     * @param addrs Server addresses formatted as follows:
     * <ul>
     *     <li>IP address (e.g. 127.0.0.1, 9.9.9.9, etc);</li>
     *     <li>IP address and port (e.g. 127.0.0.1:47500, 9.9.9.9:47501, etc);</li>
     *     <li>IP address and port range (e.g. 127.0.0.1:47500..47510, 9.9.9.9:47501..47504, etc);</li>
     *     <li>Hostname (e.g. host1.com, host2, etc);</li>
     *     <li>Hostname and port (e.g. host1.com:47500, host2:47502, etc).</li>
     *     <li>Hostname and port range (e.g. host1.com:47500..47510, host2:47502..47508, etc).</li>
     * </ul>
     * <p>
     * If port is 0 or not provided then default port will be used (depends on
     * discovery SPI configuration).
     * <p>
     * If port range is provided (e.g. host:port1..port2) the following should be considered:
     * <ul>
     *     <li>{@code port1 < port2} should be {@code true};</li>
     *     <li>Both {@code port1} and {@code port2} should be greater than {@code 0}.</li>
     * </ul>
     */
    public IgniteClientConfiguration(Collection<String> addrs) {
        setAddresses(addrs);
    }

    /**
     * @return Name or IP address of an Ignite server node to connect to.
     */
    public String getHost() {
        return addrs.size() > 0 ? addrs.get(0).getHostName() : DFLT_HOST;
    }

    /**
     * @param host name or IP address of an Ignite server node to connect to.
     */
    public IgniteClientConfiguration setHost(String host) {
        if (host == null || host.length() == 0)
            throw new IllegalArgumentException("host must be specified.");

        if (addrs.size() == 1) {
            InetSocketAddress adr = addrs.get(0);

            addrs.set(0, new InetSocketAddress(host, adr.getPort()));
        }
        else
            addrs = Collections.singletonList(new InetSocketAddress(host, DFLT_PORT));

        return this;
    }

    /**
     * @return Ignite server port to connect to. Port 10800 is used by default.
     */
    public int getPort() {
        return addrs.size() == 0 ? DFLT_PORT : addrs.get(0).getPort();
    }

    /**
     * @param port Ignite server port to connect to. Port 10800 is used by default.
     */
    public IgniteClientConfiguration setPort(int port) {
        if (addrs.size() == 1) {
            InetSocketAddress adr = addrs.get(0);

            addrs.set(0, new InetSocketAddress(adr.getHostName(), port));
        }
        else
            addrs = Collections.singletonList(new InetSocketAddress(DFLT_HOST, port));

        return this;
    }

    /**
     * @return Host addresses.
     */
    public List<InetSocketAddress> getAddresses() {
        return addrs;
    }

    /**
     * @param addrs Host addresses formatted as described in {@link #IgniteClientConfiguration(Collection)}.
     */
    public IgniteClientConfiguration setAddresses(Collection<String> addrs) {
        if (addrs == null || addrs.size() == 0)
            throw new IllegalArgumentException("addrs must be specified");

        this.addrs = addrs.stream()
            .map(IgniteClientConfiguration::parseAddressString)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

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
    public String getSslProtocol() {
        return sslProto;
    }

    /**
     * @param newVal Ssl protocol.
     */
    public IgniteClientConfiguration setSslProtocol(String newVal) {
        sslProto = newVal;

        return this;
    }

    /**
     * @return Credentials provider.
     */
    public CredentialsProvider getCredentialsProvider() {
        return credProvider;
    }

    /**
     * @param newVal Credentials provider.
     */
    public IgniteClientConfiguration setCredentialsProvider(CredentialsProvider newVal) {
        this.credProvider = newVal;

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


    /**
     * Parse address string formatted as described in {@link #IgniteClientConfiguration(Collection)}.
     */
    private static Collection<InetSocketAddress> parseAddressString(String str) {
        str = str.trim();

        String errMsg = "Failed to parse provided address: " + str;

        int colonCnt = str.length() - str.replace(":", "").length();

        if (colonCnt > 1) {
            // IPv6 address (literal IPv6 addresses are enclosed in square brackets, for example
            // https://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443).
            if (str.startsWith("[")) {
                str = str.substring(1);

                if (str.contains("]:"))
                    return parseAddressString(str, "\\]\\:", errMsg);
                else if (str.endsWith("]"))
                    str = str.substring(0, str.length() - 1);
                else
                    throw new IllegalArgumentException(errMsg);
            }
        }
        else {
            // IPv4 address.
            if (str.endsWith(":"))
                str = str.substring(0, str.length() - 1);
            else if (str.indexOf(':') >= 0)
                return parseAddressString(str, "\\:", errMsg);
        }

        // Provided address does not contain port (will use default one).
        return Collections.singleton(new InetSocketAddress(str, DFLT_PORT));
    }

    /**
     * Parse address string formatted as described in {@link #IgniteClientConfiguration(Collection)}.
     *
     * @param ipStr Address string
     * @param regexDelim Port regex delimiter.
     * @param errMsg Error message.
     * @return Address list.
     */
    private static Collection<InetSocketAddress> parseAddressString(String ipStr, String regexDelim, String errMsg) {
        String[] tokens = ipStr.split(regexDelim);

        if (tokens.length == 2) {
            String addrStr = tokens[0];
            String portStr = tokens[1];

            if (portStr.contains("..")) {
                    int port1 = Integer.parseInt(portStr.substring(0, portStr.indexOf("..")));
                    int port2 = Integer.parseInt(portStr.substring(portStr.indexOf("..") + 2, portStr.length()));

                    if (port2 < port1 || port1 == port2 || port1 <= 0 || port2 <= 0)
                        throw new IllegalArgumentException(errMsg);

                    Collection<InetSocketAddress> res = new ArrayList<>(port2 - port1);

                    // Upper bound included.
                    for (int i = port1; i <= port2; i++)
                        res.add(new InetSocketAddress(addrStr, i));

                    return res;
            }
            else {
                    int port = Integer.parseInt(portStr);

                    return Collections.singleton(new InetSocketAddress(addrStr, port));
            }
        }
        else
            throw new IllegalArgumentException(errMsg);
    }
}
