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

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.failure.TestUnresponsiveExchangeTcpCommunicationSpi;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class PartitionsExchangeUnresponsiveNodeTimeoutTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestUnresponsiveExchangeTcpCommunicationSpi());
        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setExchangeHardTimeout(10_000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (Ignite ig : G.allGrids()) {
            ((TestUnresponsiveExchangeTcpCommunicationSpi)ig.configuration().getCommunicationSpi()).writeExchangeMessageSendDelay(0);
            ((TestUnresponsiveExchangeTcpCommunicationSpi)ig.configuration().getCommunicationSpi()).writeCheckMessageSendOverrideTopologyVersion(-1);
        }

        List<Ignite> nodes = G.allGrids();

        while (!nodes.isEmpty()) {
            for (Ignite node : nodes)
                stopGrid(node.name(), true, true);

            nodes = G.allGrids();
        }

        super.afterTest();
    }

    /**
     * Tests situation when non-coordinator node becomes unresponsive. This node should be kicked from cluster.
     *
     * @throws Exception if failed.
     */
    public void testNonCoordinatorUnresponsive() throws Exception {
        final Ignite ig = startGrids(4);

        ig.cluster().active(true);

        final ClusterNode failNode = ignite(1).cluster().localNode();

        ((TestUnresponsiveExchangeTcpCommunicationSpi) ignite(1).configuration().getCommunicationSpi()).writeExchangeMessageSendDelay(-1);

        GridTestUtils.runAsync(() -> startGrid(4));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                Collection<ClusterNode> nodes = ig.cluster().nodes();

                return nodes.size() == 4 && !nodes.contains(failNode);
            }
        }, 60_000));
    }

    /**
     * Tests situation when coordinator node becomes unresponsive to exchange messages. Other nodes should leave cluster.
     *
     * @throws Exception if failed.
     */
    public void testCoordinatorUnresponsive() throws Exception {
        final Ignite ig = startGrids(4);

        ig.cluster().active(true);

        ((TestUnresponsiveExchangeTcpCommunicationSpi)ignite(0).configuration().getCommunicationSpi()).writeExchangeMessageReceiveDelay(-1);

        GridTestUtils.runAsync(() -> startGrid(4));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                return ig.cluster().nodes().size() == 1;
            }
        }, 3 * 60_000));
    }

    /**
     * Tests situation when coordinator node becomes unresponsive to exchange and version check messages.
     * Other nodes should leave cluster.
     *
     * @throws Exception if failed.
     */
    public void testCoordinatorUnresponsiveWithCheckMessagesBlocked() throws Exception {
        final Ignite ig = startGrids(4);

        ig.cluster().active(true);

        ((TestUnresponsiveExchangeTcpCommunicationSpi)ig.configuration().getCommunicationSpi()).writeExchangeMessageSendDelay(-1);
        ((TestUnresponsiveExchangeTcpCommunicationSpi)ig.configuration().getCommunicationSpi()).writeCheckMessageSendDelay(-1);

        GridTestUtils.runAsync(() -> startGrid(4));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                return ig.cluster().nodes().size() == 1;
            }
        }, 3 * 60_000));
    }

    /**
     * Tests situation when coordinator node becomes unresponsive to exchange messages, but continues to reply to
     * version check messages with outdated version. Coordinator should be kicked from cluster.
     *
     * @throws Exception if failed.
     */
    public void testCoordinatorUnresponsiveWithOutdatedCheckMessages() throws Exception {
        startGrids(4);

        final Ignite ig = grid(1);

        ig.cluster().active(true);

        UUID failNode = ignite(0).cluster().localNode().id();

        ((TestUnresponsiveExchangeTcpCommunicationSpi)ignite(0).configuration().getCommunicationSpi()).writeExchangeMessageSendDelay(-1);
        ((TestUnresponsiveExchangeTcpCommunicationSpi)ignite(0).configuration().getCommunicationSpi()).writeCheckMessageSendOverrideTopologyVersion(1);

        GridTestUtils.runAsync(() -> startGrid(4));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                Collection<ClusterNode> nodes = ig.cluster().nodes();

                return nodes.size() == 4 && !nodes.contains(failNode);
            }
        }, 60_000));
    }


}
