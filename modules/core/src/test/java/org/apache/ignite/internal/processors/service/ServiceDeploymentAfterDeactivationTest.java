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

package org.apache.ignite.internal.processors.service;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class ServiceDeploymentAfterDeactivationTest extends GridCommonAbstractTest {
    /** */
    private static final String SERVICE_NAME = "test-service";

    /** */
    private static final IgnitePredicate<ClusterNode> CLIENT_FILTER = new IgnitePredicate<ClusterNode>() {
        @Override public boolean apply(ClusterNode node) {
            return node.isClient();
        }
    };

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    private ServiceConfiguration getServiceCfg() {
        ServiceConfiguration srvcCfg = new ServiceConfiguration();
        srvcCfg.setName(SERVICE_NAME);
        srvcCfg.setMaxPerNodeCount(1);
        srvcCfg.setService(new DummyService());
        srvcCfg.setNodeFilter(F.alwaysTrue());
        return srvcCfg;
    }

    /** */
    protected void activate() {
        grid(0).cluster().active(true);
    }

    /** */
    protected void deactivate() {
        grid(0).cluster().active(false);
    }

    /**
     * @param idx Index.
     * @return Started Grid.
     * @throws Exception if failed.
     */
    protected IgniteEx startClient(int idx) throws Exception {
        return startGrid(getConfiguration(getTestIgniteInstanceName(idx)).setClientMode(true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployFromClientAfterDeactivation() throws Exception {
        startGrid(0);

        Ignite client = startClient(1);

        deactivate();
        activate();

        deployService(client, F.alwaysTrue());

        checkServiceDeployed(grid(0), client);
    }

    /**
     * @throws Exception If failed.
     */
    public void testServiceAfterJoinToDeactivated() throws Exception {
        startGrid(0);

        deactivate();

        Ignite client = startClient(1);

        activate();

        deployService(client, F.alwaysTrue());
        checkServiceDeployed(grid(0), client);
    }

    /**
     * @throws Exception If failed.
     */
    public void testServiceAfterJoinToActivated() throws Exception {
        startGrid(0);

        deactivate();
        activate();

        Ignite client = startClient(1);

        deployService(client, CLIENT_FILTER);
        checkServiceDeployed(client);
    }

    /**
     * @param ignite Grid instance.
     * @param nodeFilter Node filter.
     */
    private void deployService(Ignite ignite, IgnitePredicate<ClusterNode> nodeFilter) {
        IgniteFuture<Void> depFut = ignite.services().deployAsync(getServiceCfg());

        depFut.get(10, TimeUnit.SECONDS);
    }

    /**
     * Checks that service is deployed on specified instances.
     *
     * @param nodes Grid nodes.
     * @throws Exception If failed.
     */
    private void checkServiceDeployed(Ignite... nodes) throws Exception {
        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (Ignite node : nodes) {
                    if (node.services().service(SERVICE_NAME) == null)
                        return false;
                }

                return true;
            }
        }, 10000));
    }

}
