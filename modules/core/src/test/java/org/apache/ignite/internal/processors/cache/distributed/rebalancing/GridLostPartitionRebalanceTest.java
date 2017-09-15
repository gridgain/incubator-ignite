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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST;

/**
 *
 */
public class GridLostPartitionRebalanceTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Latch. */
    private static CountDownLatch latch;

    /** Count. */
    private static AtomicInteger cnt;

    /** Failed flag. */
    private static boolean failed;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setNodeFilter(NODE_FILTER);

        cfg.setCacheConfiguration(ccfg);

        cfg.setIncludeEventTypes(EVT_CACHE_REBALANCE_PART_DATA_LOST);

        Map<IgnitePredicate<? extends Event>, int[]> listeners = new HashMap<>();

        listeners.put(new Listener(), new int[]{EVT_CACHE_REBALANCE_PART_DATA_LOST});

        cfg.setLocalEventListeners(listeners);

        cfg.setClientMode(gridName.contains("client"));

        final Map<String, Object> attrs = new HashMap<>();

        attrs.put("node.name", gridName);

        cfg.setUserAttributes(attrs);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** Filter. */
    private static final IgnitePredicate NODE_FILTER = new IgnitePredicate<ClusterNode>() {
        /** */
        private static final long serialVersionUID = 0L;

        @Override public boolean apply(ClusterNode node) {
            return !"basenode".equals(node.attribute("node.name"));
        }
    };

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        latch = new CountDownLatch(4);
        cnt = new AtomicInteger(0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartDataLostEvent() throws Exception {
        List<Ignite> srvrs = new ArrayList<>();

        // Client router. It always up, so client is guaranteed to get
        // event.
        srvrs.add(startGrid("basenode"));

        Ignite client = startGrid("client");

        srvrs.add(startGrid("server-1"));
        srvrs.add(startGrid("server-2"));
        srvrs.add(startGrid("server-3"));

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = client.cache(CACHE_NAME);

        for (int i = 0; i < 10_000; i++)
            cache.put(i, i);

        // Stop node with 0 partition.
        ClusterNode node = client.affinity(CACHE_NAME).mapPartitionToNode(0);

        for (Ignite srv : srvrs) {
            if (node.equals(srv.cluster().localNode())) {
                srv.close();

                System.out.println(">> Stopped " + srv.name());

                break;
            }
        }

        // Check that all nodes (and clients) got notified.
        assert latch.await(15, TimeUnit.SECONDS) : latch.getCount();

        // Check that exchange was not finished when event received.
        assertFalse("Exchange was finished when event received.", failed);

        U.sleep(4_000);

        assertEquals("Fired more events than expected",4, cnt.get());
    }

    /**
     *
     */
    private static class Listener implements IgnitePredicate<CacheRebalancingEvent> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Ignite. */
        @SuppressWarnings("unused")
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public boolean apply(CacheRebalancingEvent evt) {
            int part = evt.partition();

            // AtomicBoolean because new owner will produce two events.
            if (part == 0 && CACHE_NAME.equals(evt.cacheName())) {
                System.out.println(">> Received event for 0 partition. [node=" + ignite.name() + ", evt=" + evt
                    + ", thread=" + Thread.currentThread().getName() + ']');

                latch.countDown();

                cnt.incrementAndGet();

                if (exchangeCompleted(ignite))
                    failed = true;
            }

            return true;
        }
    }

    /**
     * @param ignite Ignite.
     * @return {@code True} if exchange finished.
     */
    private static boolean exchangeCompleted(Ignite ignite) {
        GridKernalContext ctx = ((IgniteKernal)ignite).context();

        List<GridDhtPartitionsExchangeFuture> futs = ctx.cache().context().exchange().exchangeFutures();

        for (GridDhtPartitionsExchangeFuture fut : futs) {
            if (!fut.isDone())
                return false;
        }

        return true;
    }
}
