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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridTcpNioCommunicationClient;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 */
public class IgniteConnectionConcurrentReserveAndRemoveTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private volatile boolean failure;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.setCommunicationSpi(new DelegatingTcpCommunicationSpi().
            setIdleConnectionTimeout(Integer.MAX_VALUE).
            setSelectorsCount(8));

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(50 * 1024 * 1024));

        c.setDataStorageConfiguration(memCfg);

        c.setClientMode(igniteInstanceName.startsWith("client"));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        U.IGNITE_TEST_FEATURES_ENABLED = true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        U.IGNITE_TEST_FEATURES_ENABLED = false;
    }

    /** */
    private static final class TestClosure implements IgniteCallable<Integer> {
        /** Serial version uid. */
        private static final long serialVersionUid = 0L;

        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            return 1;
        }
    }

    public void testReservationHang() throws Exception {
        IgniteEx svr = startGrid(0);

        Ignite c1 = startGrid("client1");

        assertTrue(c1.configuration().isClientMode());

        Ignite c2 = startGrid("client2");

        assertTrue(c2.configuration().isClientMode());

        AtomicInteger cnt = new AtomicInteger();

        cnt.getAndAdd(c1.compute(c1.cluster().forNodeId(c2.cluster().localNode().id())).call(new TestClosure()));

        TestRecordingCommunicationSpi spi1 = TestRecordingCommunicationSpi.spi(c1);

        ConcurrentMap<UUID, GridCommunicationClient[]> clientsMap = U.field(spi1, "clients");

        GridCommunicationClient[] arr = clientsMap.get(c2.cluster().localNode().id());

        GridTcpNioCommunicationClient client = null;

        for (GridCommunicationClient c : arr) {
            client = (GridTcpNioCommunicationClient)c;

            if(client != null) {
                assertTrue(client.session().outRecoveryDescriptor().reserved());

                assertFalse(client.session().outRecoveryDescriptor().connected());
            }
        }

        assertNotNull(client);

        failure = true;

        try {
            cnt.getAndAdd(c1.compute(c1.cluster().forNodeId(c2.cluster().localNode().id())).call(new TestClosure()));

            fail();
        }
        catch (IgniteException e) {
            // Expected.
        }

        failure = false;

        // Should hang without fix.
        cnt.getAndAdd(c1.compute(c1.cluster().forNodeId(c2.cluster().localNode().id())).call(new TestClosure()));

        assertEquals(2, cnt.get());
    }

    /**
     *
     */
    class BrokenCommunicationClient extends GridTcpNioCommunicationClient implements GridCommunicationClient {
        /** Delegate. */
        private final GridTcpNioCommunicationClient delegate;

        /** Remote node which receives messages. */
        private final ClusterNode remoteNode;

        /**
         * @param locNode Local node.
         * @param remoteNode Remote node.
         * @param delegate Delegate.
         */
        BrokenCommunicationClient(ClusterNode locNode, ClusterNode remoteNode, GridTcpNioCommunicationClient delegate,
            IgniteLogger log) {
            super(delegate.connectionIndex(), delegate.session(), log);
            this.delegate = delegate;
            this.remoteNode = remoteNode;
        }

        /** {@inheritDoc} */
        @Override public GridNioSession session() {
            return delegate.session();
        }

        /** {@inheritDoc} */
        @Override public void doHandshake(IgniteInClosure2X<InputStream, OutputStream> handshakeC) throws IgniteCheckedException {
            delegate.doHandshake(handshakeC);
        }

        /** {@inheritDoc} */
        @Override public boolean close() {
            return delegate.close();
        }

        /** {@inheritDoc} */
        @Override public void forceClose() {
            if (!failure)
                delegate.forceClose();
        }

        /** {@inheritDoc} */
        @Override public boolean closed() {
            return delegate.closed();
        }

        /** {@inheritDoc} */
        @Override public boolean reserve() {
            return delegate.reserve();
        }

        /** {@inheritDoc} */
        @Override public void release() {
            delegate.release();
        }

        /** {@inheritDoc} */
        @Override public long getIdleTime() {
            return delegate.getIdleTime();
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ByteBuffer data) throws IgniteCheckedException {
            delegate.sendMessage(data);
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(byte[] data, int len) throws IgniteCheckedException {
            delegate.sendMessage(data, len);
        }

        /** {@inheritDoc} */
        @Override public boolean sendMessage(@Nullable UUID nodeId, Message msg, @Nullable IgniteInClosure<IgniteException> c) throws IgniteCheckedException {
            if (failure)
                throw new IgniteCheckedException(new IOException("Cannot send message"));

            return delegate.sendMessage(nodeId, msg, c);
        }

        /** {@inheritDoc} */
        @Override public boolean async() {
            return delegate.async();
        }

        /** {@inheritDoc} */
        @Override public int connectionIndex() {
            return delegate.connectionIndex();
        }
    }

    /**
     *
     */
    class DelegatingTcpCommunicationSpi extends TestRecordingCommunicationSpi {
        /** {@inheritDoc} */
        @Override protected GridCommunicationClient createTcpClient(ClusterNode node, int connIdx) throws IgniteCheckedException {
            GridCommunicationClient client = super.createTcpClient(node, connIdx);

            if (client == null)
                return null;

            try {
                return new BrokenCommunicationClient(getSpiContext().localNode(), node, (GridTcpNioCommunicationClient)client, log);
            }
            catch (Exception e) {
                throw new Error();
            }
        }
    }
}
