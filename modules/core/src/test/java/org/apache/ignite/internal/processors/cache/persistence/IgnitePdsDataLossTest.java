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
package org.apache.ignite.internal.processors.cache.persistence;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCacheTopologySplitAbstractTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_FAILURE_DETECTION_TIMEOUT;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Data may be lost in a split-brain scenario
 *
 * @author Alexandr Kuramshin <ein.nsk.ru@gmail.com>
 */
public class IgnitePdsDataLossTest extends IgniteCacheTopologySplitAbstractTest {

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /**  */
    private static final String CACHE_GROUP = "cacheGroup";

    /**  */
    private static final String[] CACHE_NAMES = {"cache0", "cache1"};

    /**  */
    private static final long SHORT_TX_TIMEOUT = DFLT_FAILURE_DETECTION_TIMEOUT - 5000L;

    /**  */
    private static final long LONG_TX_TIMEOUT = DFLT_FAILURE_DETECTION_TIMEOUT + 5000L;

    /**  */
    private Collection<TestRecordingCommunicationSpi> comms = new ArrayList<>();

    /** {@inheritDoc} */
    public String getTestIgniteInstanceName() {
        return "ignite";
    }

    /** {@inheritDoc} */
    @Override public int getTestIgniteInstanceIndex(String testIgniteInstanceName) {
        int idx = super.getTestIgniteInstanceIndex(testIgniteInstanceName);

        if (idx > 1)
            throw new IllegalArgumentException("Unexpected grid index. May be 0 or 1");

        return idx;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = (TcpDiscoverySpi)cfg.getDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        int idx = getTestIgniteInstanceIndex(igniteInstanceName);

        disco.setLocalPort(TcpDiscoverySpi.DFLT_PORT + idx);

        TestRecordingCommunicationSpi comm = (TestRecordingCommunicationSpi)cfg.getCommunicationSpi();

        comms.add(comm);

        comm.setLocalPort(TcpCommunicationSpi.DFLT_PORT + idx);

        cfg.setConsistentId(idx);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
            )
        );

        CacheConfiguration[] ccfgs = getCacheConfigurations();

        cfg.setCacheConfiguration(ccfgs);

        return cfg;
    }

    /**  */
    private CacheConfiguration[] getCacheConfigurations() {
        CacheConfiguration[] cfgs = new CacheConfiguration[CACHE_NAMES.length];

        for (int i = 0; i < CACHE_NAMES.length; ++i) {
            CacheConfiguration c = new CacheConfiguration()
                .setName(CACHE_NAMES[i])
                .setGroupName(CACHE_GROUP)
                .setAffinity(new RendezvousAffinityFunction(false, 4))
                .setBackups(1)
                .setTopologyValidator(new NotSingleNode())
                .setAtomicityMode(TRANSACTIONAL)
                .setWriteSynchronizationMode(FULL_SYNC);

            cfgs[i] = c;
        }

        return cfgs;
    }

    /** {@inheritDoc} */
    @Override protected boolean isBlocked(int locPort, int rmtPort) {
        return locPort % 100 != rmtPort % 100;
    }

    /** {@inheritDoc} */
    @Override protected int segment(ClusterNode node) {
        String igniteInstanceName = node.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME);

        return getTestIgniteInstanceIndex(igniteInstanceName);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        U.resolveWorkDirectory(U.defaultWorkDirectory(), "binary_meta", true);

        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", true);

        U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        comms.clear();

        stopAllGrids();
    }

    /**  */
    public void test() throws Exception {
        Ignite ignite0 = startGrid(0);

        Ignite ignite1 = startGrid(1);

        ignite0.cluster().active(true);

        // Find primary keys and create tasks will do puts

        Collection<Callable<Void>> tasks = new ArrayList<>();

        for (final Ignite ignite : G.allGrids()) {
            boolean autocommit = true;

            for (String cacheName : CACHE_NAMES) {
                final IgniteCache<Object, Object> cache = ignite.cache(cacheName);

                List<Integer> priKeys = primaryKeys(cache, 3);

                for (final Integer priKey : priKeys) {
                    autocommit ^= true;

                    final long timeout = autocommit ? SHORT_TX_TIMEOUT : LONG_TX_TIMEOUT;

                    tasks.add(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            if (log.isInfoEnabled())
                                log.info(">>> put: ignite=" + ignite.name() + ", cache=" + cacheName +
                                    ", key=" + priKey + ", timeout=" + timeout);

                            Transaction tx = ignite.transactions().txStart(OPTIMISTIC, SERIALIZABLE);
                            try {
                                tx.timeout(timeout);

                                cache.put(priKey, ignite.name() + "_" + priKey);

                                tx.commit();
                            }
                            finally {
                                tx.close();
                            }
                            return null;
                        }
                    });
                }
            }
        }

        // Simulate a split at the middle of commit
        // Some transaction will be successfully mapped by not prepared
        // Other will be prepared but not finished

        if (log.isInfoEnabled())
            log.info(">>> Simulate commit loss");

        for (TestRecordingCommunicationSpi comm : comms) {
            comm.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message message) {
                    if (message instanceof GridDhtTxPrepareRequest) {
                        GridDhtTxPrepareRequest req = (GridDhtTxPrepareRequest)message;

                        return req.timeout() > SHORT_TX_TIMEOUT;
                    }
                    else if (message instanceof GridDhtTxPrepareResponse)
                        return true;

                    return false;
                }
            });
        }

        // Do transactional puts
        // All puts will be initiated on a primary node with unique keys

        ExecutorService exec = Executors.newFixedThreadPool(tasks.size());

        List<Future<Void>> futs = new ArrayList<>();

        for (Callable<Void> task : tasks) {
            futs.add(exec.submit(task));
        }

        Thread.sleep(DFLT_FAILURE_DETECTION_TIMEOUT);

        // Simulate a split for the discovery and communication
        // Wait for node failures finished

        splitAndWait();

        // Waiting for transactions finished

        exec.shutdown();

        while (!exec.awaitTermination(1000, TimeUnit.MILLISECONDS))
            ;

        for (Future<?> fut : futs)
            try {
                fut.get();
            }
            catch (ExecutionException e) {
                if (!(e.getCause() instanceof TransactionTimeoutException))
                    throw e;
            }

        // Convince of data differs on nodes

        assertEquals("Data on nodes must NOT match", tasks.size(), diffCaches(ignite0, ignite1));

        // Simulate network restored and restart the second node

        unsplit();

        stopGrid(1);

        ignite1 = startGrid(1);

        awaitPartitionMapExchange();

        for (Ignite ignite : G.allGrids()) {
            for (String cacheName : CACHE_NAMES) {
                ignite.cache(cacheName).rebalance().get();
            }
        }

        // Convince of data async

        assertEquals("Data on nodes must match", 0, diffCaches(ignite0, ignite1));

        // Convince of data loss

        for (CachePeekMode peekMode : new CachePeekMode[] {CachePeekMode.PRIMARY, CachePeekMode.BACKUP}) {
            int totalSize = 0;

            for (Ignite ignite : G.allGrids()) {
                for (String cacheName : CACHE_NAMES) {
                    totalSize += ignite.cache(cacheName).localSize(peekMode);
                }
            }

            assertEquals("Data was lost on " + peekMode + " partitions", tasks.size(), totalSize);
        }
    }

    /**  */
    public int diffCaches(Ignite ignite0, Ignite ignite1) {
        int diff = 0;

        for (String cacheName : CACHE_NAMES) {
            IgniteCache<Object, Object> cache0 = ignite0.cache(cacheName);

            IgniteCache<Object, Object> cache1 = ignite1.cache(cacheName);

            Set<Object> keys = new HashSet<>();

            Map<Object, Cache.Entry<Object, Object>> map0 = collectEntries(cache0, keys);

            Map<Object, Cache.Entry<Object, Object>> map1 = collectEntries(cache1, keys);

            if (log.isInfoEnabled())
                log.info(">>> diff: cache=" + cacheName + ", keys=" + keys +
                    ", ignite0=" + map0 + ", ignite1=" + map1);

            for (Object key : keys) {
                Cache.Entry<Object, Object> entry0 = map0.get(key);

                Cache.Entry<Object, Object> entry1 = map1.get(key);

                if (entry0 == null || entry1 == null || !Objects.equals(entry0.getValue(), entry1.getValue())) {
                    U.warn(log, "Entries differ: cache=" + cacheName +
                        ", ignite0=" + entry0 + ", ignite1=" + entry1);

                    ++diff;
                }
            }
        }

        return diff;
    }

    /**  */
    private Map<Object, Cache.Entry<Object, Object>> collectEntries(IgniteCache<Object, Object> cache0,
        Set<Object> keys) {
        Map<Object, Cache.Entry<Object, Object>> map = new HashMap<>();

        for (Cache.Entry<Object, Object> e : cache0.localEntries(CachePeekMode.ALL)) {
            keys.add(e.getKey());

            map.put(e.getKey(), e);
        }

        return map;
    }

    /**  */
    private static class NotSingleNode implements TopologyValidator {

        @Override public boolean validate(Collection<ClusterNode> nodes) {
            return nodes.size() > 1;
        }
    }
}
