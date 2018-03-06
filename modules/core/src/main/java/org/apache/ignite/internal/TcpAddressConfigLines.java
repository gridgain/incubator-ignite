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

package org.apache.ignite.internal;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;

/**
 * A utility to parse TCP addresses provided in configuration of various Ignite entities like IP Finder.
 * <p>
 * Addresses may be represented as follows:
 * <ul>
 * <li>IP address (e.g. 127.0.0.1, 9.9.9.9, etc);</li>
 * <li>IP address and port (e.g. 127.0.0.1:47500, 9.9.9.9:47501, etc);</li>
 * <li>IP address and port range (e.g. 127.0.0.1:47500..47510, 9.9.9.9:47501..47504, etc);</li>
 * <li>Hostname (e.g. host1.com, host2, etc);</li>
 * <li>Hostname and port (e.g. host1.com:47500, host2:47502, etc).</li>
 * <li>Hostname and port range (e.g. host1.com:47500..47510, host2:47502..47508, etc).</li>
 * </ul>
 * <p>
 * If port is 0 or not provided then default port will be used (depends on
 * discovery SPI configuration).
 * <p>
 * If port range is provided (e.g. host:port1..port2) the following should be considered:
 * <ul>
 * <li>{@code port1 < port2} should be {@code true};</li>
 * <li>Both {@code port1} and {@code port2} should be greater than {@code 0}.</li>
 * </ul>
 */
public final class TcpAddressConfigLines implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = -1808074946281029711L;

    /** @serial Lines of formatted addresses. */
    private Collection<String> addrLines = new LinkedHashSet<>();

    /** Parsed addresses. */
    private transient LinkedHashSet<InetSocketAddress> addrs = new LinkedHashSet<>();

    /**
     * Constructor.
     */
    public TcpAddressConfigLines() {
    }

    /**
     * @param addrLines Address lines.
     */
    public TcpAddressConfigLines set(Collection<String> addrLines) throws IgniteConfigurationException {
        this.addrLines = addrLines == null ? new LinkedHashSet<>() : new LinkedHashSet<>(addrLines);

        for (String ipStr : this.addrLines)
            this.addrs.addAll(address(ipStr));

        return this;
    }

    /**
     * @param addr Single address line.
     */
    public TcpAddressConfigLines set(String addr) throws IgniteConfigurationException {
        return addr != null && addr.length() > 0 ? set(Collections.singleton(addr)) : this;
    }

    /**
     * Sets single address line.
     */
    public TcpAddressConfigLines set(String host, int port) throws IgniteConfigurationException {
        if (host == null || host.length() == 0)
            throw new IllegalArgumentException("host");

        return set(String.format("%s:%s", host, port));
    }

    /**
     * @return Addresses parsed as {@link InetSocketAddress}.
     */
    public Collection<InetSocketAddress> getAddresses() {
        return addrs;
    }

    /**
     * @return Formatted address lines.
     */
    public Collection<String> getAddressLines() {
        return addrLines;
    }

    /**
     * @return {code true} if there are no addresses configured.
     */
    public boolean isEmpty() {
        return addrs.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (!(obj instanceof TcpAddressConfigLines))
            return false;

        TcpAddressConfigLines other = (TcpAddressConfigLines)obj;

        return Objects.equals(addrLines, other.addrLines);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(addrLines);
    }

    /**
     * Creates address from string.
     *
     * @param ipStr Address string.
     * @return Socket addresses (may contain 1 or more addresses if provided string
     * includes port range).
     * @throws IgniteConfigurationException If failed.
     */
    private static Collection<InetSocketAddress> address(String ipStr) throws IgniteConfigurationException {
        ipStr = ipStr.trim();

        String errMsg = "Failed to parse provided address: " + ipStr;

        int colonCnt = ipStr.length() - ipStr.replace(":", "").length();

        if (colonCnt > 1) {
            // IPv6 address (literal IPv6 addresses are enclosed in square brackets, for example
            // https://[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443).
            if (ipStr.startsWith("[")) {
                ipStr = ipStr.substring(1);

                if (ipStr.contains("]:"))
                    return addresses(ipStr, "\\]\\:", errMsg);
                else if (ipStr.endsWith("]"))
                    ipStr = ipStr.substring(0, ipStr.length() - 1);
                else
                    throw new IgniteConfigurationException(errMsg);
            }
        }
        else {
            // IPv4 address.
            if (ipStr.endsWith(":"))
                ipStr = ipStr.substring(0, ipStr.length() - 1);
            else if (ipStr.indexOf(':') >= 0)
                return addresses(ipStr, "\\:", errMsg);
        }

        // Provided address does not contain port (will use default one).
        return Collections.singleton(new InetSocketAddress(ipStr, 0));
    }

    /**
     * Creates address from string with port information.
     *
     * @param ipStr Address string
     * @param regexDelim Port regex delimiter.
     * @param errMsg Error message.
     * @return Socket addresses (may contain 1 or more addresses if provided string
     * includes port range).
     * @throws IgniteConfigurationException If failed.
     */
    private static Collection<InetSocketAddress> addresses(String ipStr, String regexDelim, String errMsg)
        throws IgniteConfigurationException {
        String[] tokens = ipStr.split(regexDelim);

        if (tokens.length == 2) {
            String addrStr = tokens[0];
            String portStr = tokens[1];

            if (portStr.contains("..")) {
                try {
                    int port1 = Integer.parseInt(portStr.substring(0, portStr.indexOf("..")));
                    int port2 = Integer.parseInt(portStr.substring(portStr.indexOf("..") + 2, portStr.length()));

                    if (port2 < port1 || port1 == port2 || port1 <= 0 || port2 <= 0)
                        throw new IgniteConfigurationException(errMsg);

                    Collection<InetSocketAddress> res = new ArrayList<>(port2 - port1);

                    // Upper bound included.
                    for (int i = port1; i <= port2; i++)
                        res.add(new InetSocketAddress(addrStr, i));

                    return res;
                }
                catch (IllegalArgumentException e) {
                    throw new IgniteConfigurationException(errMsg, e);
                }
            }
            else {
                try {
                    int port = Integer.parseInt(portStr);

                    return Collections.singleton(new InetSocketAddress(addrStr, port));
                }
                catch (IllegalArgumentException e) {
                    throw new IgniteConfigurationException(errMsg, e);
                }
            }
        }
        else
            throw new IgniteConfigurationException(errMsg);
    }

    /**
     * Deserialization part of {@link Serializable}.
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        if (addrLines == null)
            addrLines = new LinkedHashSet<>();

        addrs = new LinkedHashSet<>();

        for (String ipStr : addrLines) {
            try {
                addrs.addAll(address(ipStr));
            }
            catch (IgniteConfigurationException e) {
                throw new IOException(e);
            }
        }
    }

    /**
     * Serialization part of {@link Serializable}.
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
    }
}
