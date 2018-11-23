package org.apache.ignite.spi.discovery.tcp;

import junit.framework.TestCase;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoverySpiState;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryStatistics;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class RingMessageWorkerTest extends GridCommonAbstractTest {

    private IgniteLogger mockLogger = mock(IgniteLogger.class);

    private DiscoverySpiListener mockSpiListener = mock(DiscoverySpiListener.class);

    public void test1() throws Exception {
        TcpDiscoverySpi mockSpi = getTcpDiscoverySpi();

        IgniteEx mockIgniteEx = mock(IgniteEx.class);

        GridKernalContext mockKernalContext =  mock(GridKernalContext.class);

        doReturn(mockKernalContext).when(mockIgniteEx).context();

        doReturn(mockLogger).when(mockIgniteEx).log();

        doReturn(mockIgniteEx).when(mockSpi).ignite();

        ServerImpl serverImpl = new ServerImpl(mockSpi);

        serverImpl.spiState = TcpDiscoverySpiState.CONNECTED;

        serverImpl.locNode = new TcpDiscoveryNode(
                UUID.randomUUID(),
                Collections.singletonList("127.0.0.1"),
                Collections.singletonList("127.0.0.1"),
                45000,
                mock(DiscoveryMetricsProvider.class),
                new IgniteProductVersion((byte)1, (byte)1, (byte)1, 1L, new byte[20]),
                "a");
        serverImpl.locNode.internalOrder(1L);

        serverImpl.spi.lsnr = mockSpiListener;

        TcpDiscoveryNode node2 = new TcpDiscoveryNode(
                UUID.randomUUID(),
                Collections.singletonList("127.0.0.1"),
                Collections.singletonList("127.0.0.1"),
                45000,
                mock(DiscoveryMetricsProvider.class),
                new IgniteProductVersion((byte) 1, (byte) 1, (byte) 1, 1L, new byte[20]),
                "a");
        node2.internalOrder(2L);

        serverImpl.ring.add(serverImpl.locNode);
        serverImpl.ring.add(node2);

        Whitebox.setInternalState(mockSpi, "locNode", serverImpl.locNode);


        RingMessageWorker ringMessageWorker = new RingMessageWorker(serverImpl, mockIgniteEx.log());

        IgniteThread th = new IgniteThread(ringMessageWorker);

        th.start();

        UUID nodeId1 = UUID.randomUUID();

        CustomMessageWrapper msg = new CustomMessageWrapper(new TestCustomDiscoveryMessage(1));

        TcpDiscoveryCustomEventMessage eventMsg = new TcpDiscoveryCustomEventMessage(nodeId1, msg, new byte[]{});

        //verify msg
        eventMsg.verify(serverImpl.getLocalNodeId());

        eventMsg.senderNodeId(UUID.randomUUID());

        ringMessageWorker.addMessage(eventMsg);

        th.join();
    }

    @NotNull
    private TcpDiscoverySpi getTcpDiscoverySpi() {
        TcpDiscoverySpi mockSpi = mock(TcpDiscoverySpi.class);

        Whitebox.setInternalState(mockSpi, "log", mockLogger);
        Whitebox.setInternalState(mockSpi, "stats", new TcpDiscoveryStatistics());

        doReturn(true).when(mockSpi).failureDetectionTimeoutEnabled();
        doReturn(10L).when(mockSpi).failureDetectionTimeout();

        doReturn(true).when(mockSpi).ensured(any());

        return mockSpi;
    }

}