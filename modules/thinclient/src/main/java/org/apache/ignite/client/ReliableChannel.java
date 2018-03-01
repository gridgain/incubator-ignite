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
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;

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

        List<InetSocketAddress> addrs = clientCfg.getAddresses();

        primary = addrs.get(0);

        for (int i = 1; i < addrs.size(); i++)
            this.backups.add(addrs.get(i));
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
    ) throws IgniteClientException {
        IgniteUnavailableException failure = null;

        T res = null;

        int totalSrvs = 1 + backups.size();

        svcLock.lock();
        try {
            for (int i = 0; i < totalSrvs; i++) {
                try {
                    if (failure != null)
                        changeServer();

                    if (ch == null) {
                        ch = chFactory.apply(
                            new ClientChannelConfiguration(clientCfg)
                                .setPort(primary.getPort())
                                .setHost(primary.getHostName())
                        ).get();
                    }

                    long id = ch.send(op, payloadWriter);

                    res = ch.receive(op, id, payloadReader);

                    failure = null;

                    break;
                }
                catch (IgniteUnavailableException e) {
                    failure = e;
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
        throws IgniteClientException {
        return service(op, null, payloadReader);
    }

    /**
     * Send request and handle response without payload.
     */
    public void request(ClientOperation op, Consumer<BinaryOutputStream> payloadWriter) throws IgniteClientException {
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
