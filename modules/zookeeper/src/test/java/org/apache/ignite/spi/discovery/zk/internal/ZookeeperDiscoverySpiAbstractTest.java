package org.apache.ignite.spi.discovery.zk.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.test.TestingCluster;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.discovery.DiscoveryLocalJoinData;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpiAbstractTestSuite;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.zookeeper.ZkTestClientCnxnSocketNIO;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.spi.discovery.zk.internal.ZookeeperDiscoveryImpl.IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD;
import static org.apache.zookeeper.ZooKeeper.ZOOKEEPER_CLIENT_CNXN_SOCKET;

/**
 * An abstract class to develop tests using Zookeeper Discovery SPI.
 */
public abstract class ZookeeperDiscoverySpiAbstractTest extends GridCommonAbstractTest {
    /** Zookeeper root path for Discovery. */
    protected static final String IGNITE_ZK_ROOT = ZookeeperDiscoverySpi.DFLT_ROOT_PATH;

    /** Zookeeper servers count. */
    protected static final int ZK_SRVS = 3;

    /** Zookeeper cluster. */
    protected static TestingCluster zkCluster;

    /** Discovery events received by each node. */
    protected static ConcurrentHashMap<UUID, Map<Long, DiscoveryEvent>> evts = new ConcurrentHashMap<>();

    /** Unexpected error during test invocation. */
    protected static volatile Throwable err;

    /** Builder for Zookeeper Discovery SPI. */
    protected ZookeeperDiscoverySpiBuilder zkSpiBuilder = new ZookeeperDiscoverySpiBuilder();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(ZookeeperDiscoveryImpl.IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_TIMEOUT, "1000");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopZkCluster();

        System.clearProperty(ZookeeperDiscoveryImpl.IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_TIMEOUT);
    }

    /**
     *
     */
    private void clearAckEveryEventSystemProperty() {
        System.setProperty(IGNITE_ZOOKEEPER_DISCOVERY_SPI_ACK_THRESHOLD, "1");
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (zkCluster == null) {
            zkCluster = ZookeeperDiscoverySpiAbstractTestSuite.createTestingCluster(ZK_SRVS);

            zkCluster.start();

            zkSpiBuilder
        }

        reset();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        clearAckEveryEventSystemProperty();

        try {
            if (err != null)
                log.error("", err);

            assertNull("Unexpected error, see log for details", err);

            checkEventsConsistency();

            checkInternalStructuresCleanup();

            //TODO uncomment when https://issues.apache.org/jira/browse/IGNITE-8193 is fixed
//            checkZkNodesCleanup();
        }
        finally {
            reset();

            stopAllGrids();
        }
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        assert zkCluster != null : "Zookeeper cluster should be initialized";

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(zkSpiBuilder.build(igniteInstanceName, zkCluster));

        Map<IgnitePredicate<? extends Event>, int[]> lsnrs = new HashMap<>();

        lsnrs.put(new DiscoveryEventsCollector(),
            new int[] {EVT_NODE_JOINED, EVT_NODE_FAILED, EVT_NODE_LEFT});

        cfg.setLocalEventListeners(lsnrs);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    protected void checkInternalStructuresCleanup() throws Exception {
        for (Ignite node : G.allGrids()) {
            final AtomicReference<?> res = GridTestUtils.getFieldValue(spi(node), "impl", "commErrProcFut");

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return res.get() == null;
                }
            }, 30_000);

            assertNull(res.get());
        }
    }

    /**
     *
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    protected void checkEventsConsistency() {
        for (Map.Entry<UUID, Map<Long, DiscoveryEvent>> nodeEvtEntry : evts.entrySet()) {
            UUID nodeId = nodeEvtEntry.getKey();
            Map<Long, DiscoveryEvent> nodeEvts = nodeEvtEntry.getValue();

            for (Map.Entry<UUID, Map<Long, DiscoveryEvent>> nodeEvtEntry0 : evts.entrySet()) {
                if (!nodeId.equals(nodeEvtEntry0.getKey())) {
                    Map<Long, DiscoveryEvent> nodeEvts0 = nodeEvtEntry0.getValue();

                    synchronized (nodeEvts) {
                        synchronized (nodeEvts0) {
                            checkEventsConsistency(nodeEvts, nodeEvts0);
                        }
                    }
                }
            }
        }
    }

    /**
     * @param evts1 Received events.
     * @param evts2 Received events.
     */
    private void checkEventsConsistency(Map<Long, DiscoveryEvent> evts1, Map<Long, DiscoveryEvent> evts2) {
        for (Map.Entry<Long, DiscoveryEvent> e1 : evts1.entrySet()) {
            DiscoveryEvent evt1 = e1.getValue();
            DiscoveryEvent evt2 = evts2.get(e1.getKey());

            if (evt2 != null) {
                assertEquals(evt1.topologyVersion(), evt2.topologyVersion());
                assertEquals(evt1.eventNode(), evt2.eventNode());
                assertEquals(evt1.topologyNodes(), evt2.topologyNodes());
            }
        }
    }

    /**
     *
     */
    protected void reset() {
        System.clearProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET);

        ZkTestClientCnxnSocketNIO.reset();

        System.clearProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET);

        err = null;

        evts.clear();

        try {
            cleanPersistenceDir();
        }
        catch (Exception e) {
            error("Failed to delete DB files: " + e, e);
        }
    }

    /**
     *
     */
    protected void stopZkCluster() {
        if (zkCluster != null) {
            try {
                zkCluster.close();
            }
            catch (Exception e) {
                U.error(log, "Failed to stop Zookeeper client: " + e, e);
            }

            zkCluster = null;
        }
    }

    /**
     * @param node Node.
     * @return Node's discovery SPI.
     */
    protected static ZookeeperDiscoverySpi spi(Ignite node) {
        return (ZookeeperDiscoverySpi)node.configuration().getDiscoverySpi();
    }

    private static class DiscoveryEventsCollector implements IgnitePredicate<Event> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
        @Override public boolean apply(Event evt) {
            try {
                DiscoveryEvent discoveryEvt = (DiscoveryEvent)evt;

                UUID locId = ((IgniteKernal)ignite).context().localNodeId();

                Map<Long, DiscoveryEvent> nodeEvts = evts.get(locId);

                if (nodeEvts == null) {
                    Object old = evts.put(locId, nodeEvts = new TreeMap<>());

                    assertNull(old);

                    // If the current node has failed, the local join will never happened.
                    if (evt.type() != EVT_NODE_FAILED ||
                        discoveryEvt.eventNode().consistentId().equals(ignite.configuration().getConsistentId())) {
                        synchronized (nodeEvts) {
                            DiscoveryLocalJoinData locJoin = ((IgniteEx)ignite).context().discovery().localJoin();

                            nodeEvts.put(locJoin.event().topologyVersion(), locJoin.event());
                        }
                    }
                }

                synchronized (nodeEvts) {
                    DiscoveryEvent old = nodeEvts.put(discoveryEvt.topologyVersion(), discoveryEvt);

                    assertNull(old);
                }
            }
            catch (Throwable e) {
                err = e;
            }

            return true;
        }
    }
}
