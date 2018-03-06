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

package org.apache.ignite.internal.thinclient;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.configuration.IgniteClientConfiguration;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.thinclient.ClientException;
import org.apache.ignite.thinclient.IgniteUnavailableException;

/**
 * Adds failover abd multi-threading support to {@link ClientChannel} communication.
 */
final class ReliableChannel implements AutoCloseable {
    /** Raw channel. */
    private final Function<ClientChannelConfiguration, Result<ClientChannel>> chFactory;

    /** Service lock. */
    private final Lock svcLock = new ReentrantLock();

    /** Primary server. */
    private InetSocketAddress primary;

    /** Backup servers. */
    private final Deque<InetSocketAddress> backups = new LinkedList<>();

    /** Channel. */
    private ClientChannel ch = null;

    /** Ignite config. */
    private final IgniteClientConfiguration clientCfg;

    /**
     * Constructor.
     */
    ReliableChannel(
        Function<ClientChannelConfiguration, Result<ClientChannel>> chFactory,
        IgniteClientConfiguration clientCfg
    ) {
        if (chFactory == null)
            throw new NullPointerException("chFactory");

        if (clientCfg == null)
            throw new NullPointerException("clientCfg");

        this.chFactory = chFactory;
        this.clientCfg = clientCfg;

        Collection<InetSocketAddress> addrs = clientCfg.getParsedAddresses();

        Iterator<InetSocketAddress> addrIt = addrs.iterator();

        primary = addrIt.next(); // we already verified there is at least one address

        while (addrIt.hasNext())
            this.backups.add(addrIt.next());
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        if (ch != null) {
            ch.close();

            ch = null;
        }
    }

    /**
     * Send request and handle response. The method is synchronous and single-threaded.
     */
    public <T> T service(
        ClientOperation op,
        Consumer<BinaryOutputStream> payloadWriter,
        Function<BinaryInputStream, T> payloadReader
    ) throws ClientException {
        IgniteUnavailableException failure = null;

        T res = null;

        int totalSrvs = 1 + backups.size();

        svcLock.lock();
        try {
            for (int i = 0; i < totalSrvs; i++) {
                try {
                    if (failure != null)
                        changeServer();

                    if (ch == null)
                        ch = chFactory.apply(new ClientChannelConfiguration(clientCfg).setAddresses(primary)).get();

                    long id = ch.send(op, payloadWriter);

                    res = ch.receive(op, id, payloadReader);

                    failure = null;

                    break;
                }
                catch (IgniteUnavailableException e) {
                    if (failure == null)
                        failure = e;
                    else
                        failure.addSuppressed(e);
                }
            }
        }
        finally {
            svcLock.unlock();
        }

        if (failure != null)
            throw failure;

        return res;
    }

    /**
     * Send request without payload and handle response.
     */
    public <T> T service(ClientOperation op, Function<BinaryInputStream, T> payloadReader)
        throws ClientException {
        return service(op, null, payloadReader);
    }

    /**
     * Send request and handle response without payload.
     */
    public void request(ClientOperation op, Consumer<BinaryOutputStream> payloadWriter) throws ClientException {
        service(op, payloadWriter, null);
    }

    /** */
    private void changeServer() {
        if (backups.size() > 0) {
            backups.addLast(primary);

            primary = backups.removeFirst();

            try {
                ch.close();
            }
            catch (Exception ignored) {
            }

            ch = null;
        }
    }
}
