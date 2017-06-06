/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.database.standbycluster.join;

import java.util.Map;
import org.apache.ignite.cache.database.standbycluster.AbstractNodeJoinTemplate;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.junit.Assert;

/**
 *
 */
public class JoinActiveNodeToActiveCluster extends AbstractNodeJoinTemplate {
    /**
     *
     */
    @Override public JoinNodeTestPlanBuilder withOutConfigurationTemplate() throws Exception {
        JoinNodeTestPlanBuilder b = builder();

        b.clusterConfiguration(
            cfg(name(0)).setActiveOnStart(true),
            cfg(name(1)).setActiveOnStart(true),
            cfg(name(2)).setActiveOnStart(true)
        ).afterClusterStarted(
            b.checkCacheOnlySystem()
        ).nodeConfiguration(
            cfg(name(3)).setActiveOnStart(true)
        ).afterNodeJoin(
            b.checkCacheOnlySystem()
        ).stateAfterJoin(
            true
        ).afterDeActivate(
            b.checkCacheEmpty()
        ).setEnd(
            b.checkCacheOnlySystem()
        );

        return b;
    }

    /**
     *
     */
    @Override public JoinNodeTestPlanBuilder staticCacheConfigurationOnJoinTemplate() throws Exception {
        JoinNodeTestPlanBuilder b = builder();

        b.clusterConfiguration(
            cfg(name(0)).setActiveOnStart(true),
            cfg(name(1)).setActiveOnStart(true),
            cfg(name(2)).setActiveOnStart(true)
        ).afterClusterStarted(
            b.checkCacheOnlySystem()
        ).nodeConfiguration(
            cfg(name(3))
                .setActiveOnStart(true)
                .setCacheConfiguration(allCacheConfigurations())
        ).afterNodeJoin(
            b.checkCacheNotEmpty()
        ).stateAfterJoin(
            true
        ).afterDeActivate(
            b.checkCacheEmpty()
        ).setEnd(
            b.checkCacheNotEmpty()
        );

        return b;
    }

    /**
     *
     */
    @Override public JoinNodeTestPlanBuilder staticCacheConfigurationInClusterTemplate() throws Exception {
        JoinNodeTestPlanBuilder b = builder();

        b.clusterConfiguration(
            cfg(name(0))
                .setActiveOnStart(true)
                .setCacheConfiguration(allCacheConfigurations()),
            cfg(name(1)).setActiveOnStart(true),
            cfg(name(2)).setActiveOnStart(true)
        ).afterClusterStarted(
            b.checkCacheNotEmpty()
        ).nodeConfiguration(
            cfg(name(3))
                .setActiveOnStart(true)
        ).afterNodeJoin(
            b.checkCacheNotEmpty()
        ).stateAfterJoin(
            true
        ).afterDeActivate(
            b.checkCacheEmpty()
        ).setEnd(
            b.checkCacheNotEmpty()
        );

        return b;
    }

    /**
     *
     */
    @Override public JoinNodeTestPlanBuilder staticCacheConfigurationSameOnBothTemplate() throws Exception {
        JoinNodeTestPlanBuilder b = builder();

        b.clusterConfiguration(
            cfg(name(0))
                .setActiveOnStart(true)
                .setCacheConfiguration(allCacheConfigurations()),
            cfg(name(1)).setActiveOnStart(true),
            cfg(name(2)).setActiveOnStart(true)
        ).afterClusterStarted(
            b.checkCacheNotEmpty()
        ).nodeConfiguration(
            cfg(name(3))
                .setActiveOnStart(true)
                .setCacheConfiguration(allCacheConfigurations())
        ).afterNodeJoin(
            b.checkCacheNotEmpty()
        ).stateAfterJoin(
            true
        ).afterDeActivate(
            b.checkCacheEmpty()
        ).setEnd(
            b.checkCacheNotEmpty()
        );

        return b;
    }

    /**
     *
     */
    @Override public JoinNodeTestPlanBuilder staticCacheConfigurationDifferentOnBothTemplate() throws Exception {
        JoinNodeTestPlanBuilder b = builder();

        b.clusterConfiguration(
            cfg(name(0))
                .setActiveOnStart(true)
                .setCacheConfiguration(atomicCfg()),
            cfg(name(1)).setActiveOnStart(true),
            cfg(name(2)).setActiveOnStart(true)
        ).afterClusterStarted(
            new Runnable() {
                @Override public void run() {
                    for (int i = 0; i < 3; i++) {
                        IgniteEx ig = grid(name(i));

                        Map<String, DynamicCacheDescriptor> desc = cacheDescriptors(ig);

                        Assert.assertEquals(3, desc.size());

                        Assert.assertNotNull(ig.context().cache().cache(cache1));
                        Assert.assertNull(ig.context().cache().cache(cache2));

                        Map<String, GridCacheAdapter> caches = caches(ig);

                        Assert.assertEquals(3, caches.size());
                    }
                }
            }
        ).nodeConfiguration(
            cfg(name(3))
                .setActiveOnStart(true)
                .setCacheConfiguration(transactionCfg())
        ).afterNodeJoin(
            b.checkCacheNotEmpty()
        ).stateAfterJoin(
            true
        ).afterDeActivate(
            b.checkCacheEmpty()
        ).setEnd(
            b.checkCacheNotEmpty()
        );

        return b;
    }

    // Server node join.

    /** {@inheritDoc} */
    @Override public void testJoinWithOutConfiguration() throws Exception {
        withOutConfigurationTemplate().execute();
    }

    /**
     *
     */
    @Override public void testStaticCacheConfigurationOnJoin() throws Exception {
        staticCacheConfigurationOnJoinTemplate().execute();
    }

    /**
     *
     */
    @Override public void testStaticCacheConfigurationInCluster() throws Exception {
        staticCacheConfigurationInClusterTemplate().execute();
    }

    /**
     *
     */
    @Override public void testStaticCacheConfigurationSameOnBoth() throws Exception {
        staticCacheConfigurationSameOnBothTemplate().execute();
    }

    /**
     *
     */
    @Override public void testStaticCacheConfigurationDifferentOnBoth() throws Exception {
        staticCacheConfigurationDifferentOnBothTemplate().execute();
    }

    // Client node join.

    /**
     *
     */
    @Override public void testJoinClientWithOutConfiguration() throws Exception {
        withOutConfigurationTemplate().nodeConfiguration(setClient).execute();
    }

    /**
     *
     */
    @Override public void testJoinClientStaticCacheConfigurationOnJoin() throws Exception {
        staticCacheConfigurationOnJoinTemplate().nodeConfiguration(setClient).execute();
    }

    /**
     *
     */
    @Override public void testJoinClientStaticCacheConfigurationInCluster() throws Exception {
        staticCacheConfigurationInClusterTemplate()
            .nodeConfiguration(setClient)
            .afterNodeJoin(
                new Runnable() {
                    @Override public void run() {
                        for (int i = 0; i < 3; i++) {
                            IgniteEx ig = grid(name(i));

                            Map<String, DynamicCacheDescriptor> desc = cacheDescriptors(ig);

                            Assert.assertEquals(4, desc.size());

                            if (!ig.context().discovery().localNode().isClient()) {
                                Assert.assertNotNull(ig.context().cache().cache(cache1));
                                Assert.assertNotNull(ig.context().cache().cache(cache2));
                            }
                            else {
                                Assert.assertNull(ig.context().cache().cache(cache1));
                                Assert.assertNull(ig.context().cache().cache(cache2));
                            }

                            Map<String, GridCacheAdapter> caches = caches(ig);

                            Assert.assertEquals(4, caches.size());
                        }
                    }
                }).execute();
    }

    /**
     *
     */
    @Override public void testJoinClientStaticCacheConfigurationSameOnBoth() throws Exception {
        staticCacheConfigurationSameOnBothTemplate().nodeConfiguration(setClient).execute();
    }

    /**
     *
     */
    @Override public void testJoinClientStaticCacheConfigurationDifferentOnBoth() throws Exception {
        staticCacheConfigurationDifferentOnBothTemplate()
            .nodeConfiguration(setClient)
            .afterNodeJoin(new Runnable() {
                @Override public void run() {
                    for (int i = 0; i < 4; i++) {
                        IgniteEx ig = grid(name(i));

                        Map<String, DynamicCacheDescriptor> desc = cacheDescriptors(ig);

                        Assert.assertEquals(4, desc.size());

                        if (!ig.context().discovery().localNode().isClient())
                            Assert.assertNotNull(ig.context().cache().cache(cache1));

                        Assert.assertNotNull(ig.context().cache().cache(cache2));

                        Map<String, GridCacheAdapter> caches = caches(ig);

                        if (!ig.context().discovery().localNode().isClient())
                            Assert.assertEquals(4, caches.size());
                        else
                            Assert.assertEquals(3, caches.size());
                    }
                }
            }).execute();
    }
}
