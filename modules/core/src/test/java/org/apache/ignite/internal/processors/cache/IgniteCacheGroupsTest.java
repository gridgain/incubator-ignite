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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import java.util.concurrent.locks.Lock;
import javax.cache.CacheException;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicyFactory;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridPlainCallable;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
@SuppressWarnings("unchecked")
public class IgniteCacheGroupsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String GROUP1 = "grp1";

    /** */
    private static final String GROUP2 = "grp2";

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private boolean client;

    /** */
    private CacheConfiguration[] ccfgs;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        //cfg.setLateAffinityAssignment(false);

        cfg.setClientMode(client);

        cfg.setMarshaller(null);

        if (ccfgs != null) {
            cfg.setCacheConfiguration(ccfgs);

            ccfgs = null;
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCloseCache1() throws Exception {
        startGrid(0);

        client = true;

        Ignite client = startGrid(1);

        IgniteCache c1 = client.createCache(cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 0, false));

        checkCacheGroup(0, GROUP1, true);
        checkCacheGroup(0, GROUP1, true);

        checkCache(0, "c1", 10);
        checkCache(1, "c1", 10);

        c1.close();

        checkCacheGroup(0, GROUP1, true);
        checkCacheGroup(1, GROUP1, false);

        checkCache(0, "c1", 10);

        assertNotNull(client.cache("c1"));

        checkCacheGroup(0, GROUP1, true);
        checkCacheGroup(1, GROUP1, true);

        checkCache(0, "c1", 10);
        checkCache(1, "c1", 10);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateDestroyCaches1() throws Exception {
        createDestroyCaches(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateDestroyCaches2() throws Exception {
        createDestroyCaches(5);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateCacheWithSameNameInAnotherGroup() throws Exception {
        startGridsMultiThreaded(2);

        final Ignite ignite = ignite(0);

        ignite.createCache(cacheConfiguration(GROUP1, CACHE1, PARTITIONED, ATOMIC, 2, false));

        GridTestUtils.assertThrows(null, new GridPlainCallable<Void>() {
            @Override public Void call() throws Exception {
                ignite(1).createCache(cacheConfiguration(GROUP2, CACHE1, PARTITIONED, ATOMIC, 2, false));
                return null;
            }
        }, CacheExistsException.class, "a cache with the same name is already started");
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateDestroyCachesAtomicPartitioned() throws Exception {
        createDestroyCaches(PARTITIONED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateDestroyCachesTxPartitioned() throws Exception {
        createDestroyCaches(PARTITIONED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateDestroyCachesAtomicReplicated() throws Exception {
        createDestroyCaches(REPLICATED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateDestroyCachesTxReplicated() throws Exception {
        createDestroyCaches(REPLICATED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryAtomicPartitioned() throws Exception {
        scanQuery(PARTITIONED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryTxPartitioned() throws Exception {
        scanQuery(PARTITIONED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryAtomicReplicated() throws Exception {
        scanQuery(REPLICATED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryTxReplicated() throws Exception {
        scanQuery(REPLICATED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryAtomicLocal() throws Exception {
        scanQuery(LOCAL, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryTxLocal() throws Exception {
        scanQuery(LOCAL, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntriesTtlAtomicPartitioned() throws Exception {
        entriesTtl(PARTITIONED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntriesTtlTxPartitioned() throws Exception {
        entriesTtl(PARTITIONED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntriesTtlAtomicReplicated() throws Exception {
        entriesTtl(REPLICATED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntriesTtlTxReplicated() throws Exception {
        entriesTtl(REPLICATED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntriesTtlAtomicLocal() throws Exception {
        entriesTtl(LOCAL, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntriesTtlTxLocal() throws Exception {
        entriesTtl(LOCAL, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheIteratorAtomicPartitioned() throws Exception {
        cacheIterator(PARTITIONED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheIteratorTxPartitioned() throws Exception {
        cacheIterator(PARTITIONED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheIteratorAtomicReplicated() throws Exception {
        cacheIterator(REPLICATED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheIteratorTxReplicated() throws Exception {
        cacheIterator(REPLICATED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheIteratorAtomicLocal() throws Exception {
        cacheIterator(LOCAL, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheIteratorTxLocal() throws Exception {
        cacheIterator(LOCAL, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryMultiplePartitionsAtomicPartitioned() throws Exception {
        scanQueryMultiplePartitions(PARTITIONED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryMultiplePartitionsTxPartitioned() throws Exception {
        scanQueryMultiplePartitions(PARTITIONED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryMultiplePartitionsAtomicReplicated() throws Exception {
        scanQueryMultiplePartitions(REPLICATED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testScanQueryMultiplePartitionsTxReplicated() throws Exception {
        scanQueryMultiplePartitions(REPLICATED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    private void scanQuery(CacheMode cacheMode, CacheAtomicityMode atomicityMode) throws Exception {
        int keys = 10000;

        Integer[] data1 = generateData(keys);
        Integer[] data2 = generateData(keys);

        boolean local = cacheMode == LOCAL;

        if (local)
            startGrid(0);
        else
            startGridsMultiThreaded(4);

        Ignite srv0 = ignite(0);

        srv0.createCache(cacheConfiguration(GROUP1, CACHE1, cacheMode, atomicityMode, 2, false));
        srv0.createCache(cacheConfiguration(GROUP1, CACHE2, cacheMode, atomicityMode, 2, false));

        if(!local)
            awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache1;
        IgniteCache<Integer, Integer> cache2;

        if (atomicityMode == TRANSACTIONAL) {
            Ignite ignite = ignite(local ? 0 : 1);

            try (Transaction tx = ignite.transactions().txStart()) {
                cache1 = ignite.cache(CACHE1);
                cache2 = ignite.cache(CACHE2);

                for (int i = 0; i < keys ; i++) {
                    cache1.put(i, data1[i]);
                    cache2.put(i, data2[i]);
                }

                tx.commit();
            }
        }
        else {
            cache1 = ignite(local ? 0 : 1).cache(CACHE1);
            cache2 = ignite(local ? 0 : 2).cache(CACHE2);

            for (int i = 0; i < keys ; i++) {
                cache1.put(i, data1[i]);
                cache2.put(i, data2[i]);
            }
        }

        ScanQuery<Integer, Integer> qry = new ScanQuery<>();

        Set<Integer> keysSet = sequence(keys);

        for (Cache.Entry<Integer, Integer> entry : ignite(local ? 0 : 3).cache(CACHE1).query(qry)) {
            assertTrue(keysSet.remove(entry.getKey()));
            assertEquals(data1[entry.getKey()], entry.getValue());
        }

        assertTrue(keysSet.isEmpty());

        srv0.destroyCache(CACHE1);

        keysSet = sequence(keys);

        for (Cache.Entry<Integer, Integer> entry : ignite(local ? 0 : 3).cache(CACHE2).query(qry)) {
            assertTrue(keysSet.remove(entry.getKey()));
            assertEquals(data2[entry.getKey()], entry.getValue());
        }

        assertTrue(keysSet.isEmpty());
    }

    /**
     * @throws Exception If failed.
     */
    private void scanQueryMultiplePartitions(CacheMode cacheMode, CacheAtomicityMode atomicityMode) throws Exception {
        int keys = 10000;

        Integer[] data1 = generateData(keys);
        Integer[] data2 = generateData(keys);

        startGridsMultiThreaded(4);

        Ignite srv0 = ignite(0);

        srv0.createCache(
            cacheConfiguration(GROUP1, CACHE1, cacheMode, atomicityMode, 2, false)
                .setAffinity(new RendezvousAffinityFunction().setPartitions(32)));
        srv0.createCache(
            cacheConfiguration(GROUP1, CACHE2, cacheMode, atomicityMode, 2, false)
                .setAffinity(new RendezvousAffinityFunction().setPartitions(32)));

        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache1;
        IgniteCache<Integer, Integer> cache2;

        if (atomicityMode == TRANSACTIONAL) {
            Ignite ignite = ignite(1);

            try (Transaction tx = ignite.transactions().txStart()) {
                cache1 = ignite.cache(CACHE1);
                cache2 = ignite.cache(CACHE2);

                for (int i = 0; i < keys ; i++) {
                    cache1.put(i, data1[i]);
                    cache2.put(i, data2[i]);
                }

                tx.commit();
            }
        }
        else {
            cache1 = ignite(1).cache(CACHE1);
            cache2 = ignite(2).cache(CACHE2);

            for (int i = 0; i < keys ; i++) {
                cache1.put(i, data1[i]);
                cache2.put(i, data2[i]);
            }
        }


        int p = ThreadLocalRandom.current().nextInt(32);

        ScanQuery<Integer, Integer> qry = new ScanQuery().setPartition(p);

        Set<Integer> keysSet = new TreeSet<>();

        cache1 = ignite(3).cache(CACHE1);

        Affinity<Integer> aff = affinity(cache1);

        for(int i = 0; i < keys; i++) {
            if (aff.partition(i) == p) {
                keysSet.add(i);
            }
        }

        for (Cache.Entry<Integer, Integer> entry : cache1.query(qry)) {
            assertTrue(keysSet.remove(entry.getKey()));
            assertEquals(data1[entry.getKey()], entry.getValue());
        }

        assertTrue(keysSet.isEmpty());

        srv0.destroyCache(CACHE1);

        keysSet = new TreeSet<>();

        cache2 = ignite(3).cache(CACHE2);

        aff = affinity(cache2);

        for(int i = 0; i < keys; i++) {
            if (aff.partition(i) == p) {
                keysSet.add(i);
            }
        }

        for (Cache.Entry<Integer, Integer> entry : cache2.query(qry)) {
            assertTrue(keysSet.remove(entry.getKey()));
            assertEquals(data2[entry.getKey()], entry.getValue());
        }

        assertTrue(keysSet.isEmpty());
    }

    /**
     * @throws Exception If failed.
     */
    private void cacheIterator(CacheMode cacheMode, CacheAtomicityMode atomicityMode) throws Exception {
        int keys = 10000;

        Integer[] data1 = generateData(keys);
        Integer[] data2 = generateData(keys);

        boolean local = cacheMode == LOCAL;

        if (local)
            startGrid(0);
        else
            startGridsMultiThreaded(4);

        Ignite srv0 = ignite(0);

        srv0.createCache(cacheConfiguration(GROUP1, CACHE1, cacheMode, atomicityMode, 2, false));
        srv0.createCache(cacheConfiguration(GROUP1, CACHE2, cacheMode, atomicityMode, 2, false));

        if(!local)
            awaitPartitionMapExchange();

        if (atomicityMode == TRANSACTIONAL) {
            Ignite ignite = ignite(local ? 0 : 1);

            try (Transaction tx = ignite.transactions().txStart()) {
                IgniteCache cache1 = ignite.cache(CACHE1);
                IgniteCache cache2 = ignite.cache(CACHE2);

                for (int i = 0; i < keys ; i++) {
                    cache1.put(i, data1[i]);
                    cache2.put(i, data2[i]);
                }

                tx.commit();
            }
        }
        else {
            IgniteCache cache1 = ignite(local ? 0 : 1).cache(CACHE1);
            IgniteCache cache2 = ignite(local ? 0 : 2).cache(CACHE2);

            for (int i = 0; i < keys ; i++) {
                cache1.put(i, data1[i]);
                cache2.put(i, data2[i]);
            }
        }


        Set<Integer> keysSet = sequence(keys);

        for (Cache.Entry<Integer, Integer> entry : ignite(local ? 0 : 3).<Integer, Integer>cache(CACHE1)) {
            assertTrue(keysSet.remove(entry.getKey()));
            assertEquals(data1[entry.getKey()], entry.getValue());
        }

        assertTrue(keysSet.isEmpty());

        srv0.destroyCache(CACHE1);

        keysSet = sequence(keys);

        for (Cache.Entry<Integer, Integer> entry : ignite(local ? 0 : 3).<Integer, Integer>cache(CACHE2)) {
            assertTrue(keysSet.remove(entry.getKey()));
            assertEquals(data2[entry.getKey()], entry.getValue());
        }

        assertTrue(keysSet.isEmpty());
    }

    /**
     * @throws Exception If failed.
     */
    private void entriesTtl(CacheMode cacheMode, CacheAtomicityMode atomicityMode) throws Exception {
        int keys = 10000;

        final int ttl = 10000;

        Integer[] data1 = generateData(keys);
        Integer[] data2 = generateData(keys);

        boolean local = cacheMode == LOCAL;

        if (local)
            startGrid(0);
        else
            startGridsMultiThreaded(4);


        Ignite srv0 = ignite(0);

        srv0.createCache(
            cacheConfiguration(GROUP1, CACHE1, cacheMode, atomicityMode, 2, false)
                // -1 = ETERNAL       just created entries are not expiring
                // -2 = NOT_CHANGED   not to change ttl on entry update
                .setExpiryPolicyFactory(new PlatformExpiryPolicyFactory(-1, -2, ttl)).setEagerTtl(true)
        );

        srv0.createCache(cacheConfiguration(GROUP1, CACHE2, cacheMode, atomicityMode, 2, false));

        if (!local)
            awaitPartitionMapExchange();

        if (atomicityMode == TRANSACTIONAL) {
            Ignite ignite = ignite(local ? 0 : 1);

            try (Transaction tx = ignite.transactions().txStart()) {
                IgniteCache cache1 = ignite.cache(CACHE1);
                IgniteCache cache2 = ignite.cache(CACHE2);

                for (int i = 0; i < keys ; i++) {
                    cache1.put(i, data1[i]);
                    cache2.put(i, data2[i]);
                }

                tx.commit();
            }
        }
        else {
            IgniteCache cache1 = ignite(local ? 0 : 1).cache(CACHE1);
            IgniteCache cache2 = ignite(local ? 0 : 2).cache(CACHE2);

            for (int i = 0; i < keys ; i++) {
                cache1.put(i, data1[i]);
                cache2.put(i, data2[i]);
            }
        }

        checkData(local ? 0 : 3, CACHE1, data1);
        checkData(local ? 0 : 3, CACHE2, data2);

        srv0.destroyCache(CACHE2);

        checkData(local ? 0 : 3, CACHE1, data1);

        // Wait for expiration

        Thread.sleep((long)(ttl * 1.2));

        assertEquals(0, ignite(local ? 0 : 3).cache(CACHE1).size());
    }

    /**
     * @throws Exception If failed.
     */
    private void createDestroyCaches(CacheMode cacheMode, CacheAtomicityMode atomicityMode) throws Exception {
        int keys = 10000;

        Integer[] data1 = generateData(keys);
        Integer[] data2 = generateData(keys);

        startGridsMultiThreaded(4);

        Ignite srv0 = ignite(0);

        srv0.createCache(cacheConfiguration(GROUP1, CACHE1, cacheMode, atomicityMode, 2, false));
        srv0.createCache(cacheConfiguration(GROUP1, CACHE2, cacheMode, atomicityMode, 2, false));

        awaitPartitionMapExchange();

        if (atomicityMode == TRANSACTIONAL) {
            Ignite ignite = ignite(1);

            try (Transaction tx = ignite.transactions().txStart()) {
                IgniteCache cache1 = ignite.cache(CACHE1);
                IgniteCache cache2 = ignite.cache(CACHE2);

                for (int i = 0; i < keys ; i++) {
                    cache1.put(i, data1[i]);
                    cache2.put(i, data2[i]);
                }

                tx.commit();
            }
        }
        else {
            // async put ops
            int ldrs = 4;

            List<Callable<?>> cls = new ArrayList<>(ldrs * 2);

            for (int i = 0; i < ldrs ; i++) {
                cls.add(putOperation(1, ldrs, i, CACHE1, data1));
                cls.add(putOperation(2, ldrs, i, CACHE2, data2));
            }

            GridTestUtils.runMultiThreaded(cls, "loaders");
        }

        checkLocalData(3, CACHE1, data1);
        checkLocalData(0, CACHE2, data2);

        checkData(0, CACHE1, data1);
        checkData(3, CACHE2, data2);

        ignite(1).destroyCache(CACHE2);

        startGrid(5);

        awaitPartitionMapExchange();

        checkData(5, CACHE1, data1);
        checkLocalData(5, CACHE1, data1);

        ignite(1).destroyCache(CACHE1);

        checkCacheGroup(5, GROUP1, false);
    }

    /**
     * @param idx Node index.
     * @param ldrs Loaders count.
     * @param ldrIdx Loader index.
     * @param cacheName Cache name.
     * @param data Data.
     * @return Callable for put operation.
     */
    private Callable<Void> putOperation(
            final int idx,
            final int ldrs,
            final int ldrIdx,
            final String cacheName,
            final Integer[] data) {
        return new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                IgniteCache cache = ignite(idx).cache(cacheName);

                for (int j = 0, size = data.length; j < size ; j++) {
                    if (j % ldrs == ldrIdx) {
                        cache.put(j, data[j]);
                    }
                }
                return null;
            }
        };
    }

    /**
     * Creates an array of random integers.
     *
     * @param cnt Array length.
     * @return Array of random integers.
     */
    private Integer[] generateData(int cnt) {
        Random rnd = ThreadLocalRandom.current();

        Integer[] data = new Integer[cnt];

        for (int i = 0; i < data.length; i++)
            data[i] = rnd.nextInt();

        return data;
    }

    /**
     * Creates a map with random integers.
     *
     * @param cnt Map size length.
     * @return Map with random integers.
     */
    private Map<Integer, Integer> generateDataMap(int cnt) {
        Random rnd = ThreadLocalRandom.current();

        Map<Integer, Integer> data = U.newHashMap(cnt);

        for (int i = 0; i < cnt; i++)
            data.put(i, rnd.nextInt());

        return data;
    }

    /**
     * @param cnt Sequence length.
     * @return Sequence of integers.
     */
    private Set<Integer> sequence(int cnt) {
        Set<Integer> res = new TreeSet<>();

        for (int i = 0; i < cnt; i++)
            res.add(i);

        return res;
    }

    /**
     * @param idx Node index.
     * @param cacheName Cache name.
     * @param data Expected data.
     * @throws Exception If failed.
     */
    private void checkData(int idx, String cacheName, Integer[] data) throws Exception {
        Set<Integer> keys = sequence(data.length);

        Set<Map.Entry<Integer, Integer>> entries =
            ignite(idx).<Integer, Integer>cache(cacheName).getAll(keys).entrySet();

        for (Map.Entry<Integer, Integer> entry : entries) {
            assertTrue(keys.remove(entry.getKey()));
            assertEquals(data[entry.getKey()], entry.getValue());
        }

        assertTrue(keys.isEmpty());
    }

    /**
     * @param idx Node index.
     * @param cacheName Cache name.
     * @param data Expected data.
     * @throws Exception If failed.
     */
    private void checkLocalData(int idx, String cacheName, Integer[] data) throws Exception {
        Ignite ignite = ignite(idx);
        ClusterNode node = ignite.cluster().localNode();
        IgniteCache cache = ignite.<Integer, Integer>cache(cacheName);

        Affinity aff = affinity(cache);

        Set<Integer> localKeys = new TreeSet<>();

        for (int key = 0; key < data.length; key++) {
            if(aff.isPrimaryOrBackup(node, key))
                localKeys.add(key);
        }

        Iterable<Cache.Entry<Integer, Integer>> localEntries = cache.localEntries(CachePeekMode.OFFHEAP);

        for (Cache.Entry<Integer, Integer> entry : localEntries) {
            assertTrue(localKeys.remove(entry.getKey()));
            assertEquals(data[entry.getKey()], entry.getValue());
        }

        assertTrue(localKeys.isEmpty());
    }

    /**
     * @param srvs Number of server nodes.
     * @throws Exception If failed.
     */
    private void createDestroyCaches(int srvs) throws Exception {
        startGridsMultiThreaded(srvs);

        checkCacheDiscoveryDataConsistent();

        Ignite srv0 = ignite(0);

        for (int i = 0; i < srvs; i++)
            checkCacheGroup(i, GROUP1, false);

        for (int iter = 0; iter < 3; iter++) {
            log.info("Iteration: " + iter);

            srv0.createCache(cacheConfiguration(GROUP1, CACHE1, PARTITIONED, ATOMIC, 2, false));

            checkCacheDiscoveryDataConsistent();

            for (int i = 0; i < srvs; i++) {
                checkCacheGroup(i, GROUP1, true);

                checkCache(i, CACHE1, 10);
            }

            srv0.createCache(cacheConfiguration(GROUP1, CACHE2, PARTITIONED, ATOMIC, 2, false));

            checkCacheDiscoveryDataConsistent();

            for (int i = 0; i < srvs; i++) {
                checkCacheGroup(i, GROUP1, true);

                checkCache(i, CACHE2, 10);
            }

            srv0.destroyCache(CACHE1);

            checkCacheDiscoveryDataConsistent();

            for (int i = 0; i < srvs; i++) {
                checkCacheGroup(i, GROUP1, true);

                checkCache(i, CACHE2, 10);
            }

            srv0.destroyCache(CACHE2);

            checkCacheDiscoveryDataConsistent();

            for (int i = 0; i < srvs; i++)
                checkCacheGroup(i, GROUP1, false);
        }
    }

    /**
     * @param idx Node index.
     * @param cacheName Cache name.
     * @param ops Number of operations to execute.
     */
    private void checkCache(int idx, String cacheName, int ops) {
        IgniteCache cache = ignite(idx).cache(cacheName);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < ops; i++) {
            Integer key = rnd.nextInt();

            cache.put(key, i);

            assertEquals(i, cache.get(key));
        }
    }

    /**
     * @param cache3 {@code True} if add last cache.
     * @return Cache configurations.
     */
    private CacheConfiguration[] staticConfigurations1(boolean cache3) {
        CacheConfiguration[] ccfgs = new CacheConfiguration[cache3 ? 3 : 2];

        ccfgs[0] = cacheConfiguration(null, "cache1", PARTITIONED, ATOMIC, 2, false);
        ccfgs[1] = cacheConfiguration(GROUP1, "cache2", PARTITIONED, ATOMIC, 2, false);

        if (cache3)
            ccfgs[2] = cacheConfiguration(GROUP1, "cache3", PARTITIONED, ATOMIC, 2, false);

        return ccfgs;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDiscoveryDataConsistency1() throws Exception {
        ccfgs = staticConfigurations1(true);
        Ignite srv0 = startGrid(0);

        ccfgs = staticConfigurations1(true);
        startGrid(1);

        checkCacheDiscoveryDataConsistent();

        ccfgs = null;
        startGrid(2);

        checkCacheDiscoveryDataConsistent();

        srv0.createCache(cacheConfiguration(null, "cache4", PARTITIONED, ATOMIC, 2, false));

        checkCacheDiscoveryDataConsistent();

        ccfgs = staticConfigurations1(true);
        startGrid(3);

        checkCacheDiscoveryDataConsistent();

        srv0.createCache(cacheConfiguration(GROUP1, "cache5", PARTITIONED, ATOMIC, 2, false));

        ccfgs = staticConfigurations1(true);
        startGrid(4);

        checkCacheDiscoveryDataConsistent();

        for (int i = 0; i < 5; i++)
            checkCacheGroup(i, GROUP1, true);

        srv0.destroyCache("cache1");
        srv0.destroyCache("cache2");
        srv0.destroyCache("cache3");

        checkCacheDiscoveryDataConsistent();

        ccfgs = staticConfigurations1(true);
        startGrid(5);

        checkCacheDiscoveryDataConsistent();

        for (int i = 0; i < 6; i++)
            checkCacheGroup(i, GROUP1, true);

        srv0.destroyCache("cache1");
        srv0.destroyCache("cache2");
        srv0.destroyCache("cache3");
        srv0.destroyCache("cache4");
        srv0.destroyCache("cache5");

        ccfgs = staticConfigurations1(true);
        startGrid(6);

        checkCacheDiscoveryDataConsistent();

        srv0.createCache(cacheConfiguration(null, "cache4", PARTITIONED, ATOMIC, 2, false));
        srv0.createCache(cacheConfiguration(GROUP1, "cache5", PARTITIONED, ATOMIC, 2, false));

        checkCacheDiscoveryDataConsistent();

        ccfgs = staticConfigurations1(false);
        startGrid(7);

        checkCacheDiscoveryDataConsistent();

        awaitPartitionMapExchange();
    }

    /**
     * @param cnt Caches number.
     * @param grp Cache groups.
     * @param baseName Caches name prefix.
     * @return Cache configurations.
     */
    private CacheConfiguration[] cacheConfigurations(int cnt, String grp, String baseName) {
        CacheConfiguration[] ccfgs = new CacheConfiguration[cnt];

        for (int i = 0; i < cnt; i++) {
            ccfgs[i] = cacheConfiguration(grp,
                baseName + i, PARTITIONED,
                i % 2 == 0 ? TRANSACTIONAL : ATOMIC,
                2,
                false);
        }

        return ccfgs;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartManyCaches() throws Exception {
        final int CACHES = 5_000;

        final int NODES = 4;

        for (int i = 0; i < NODES; i++) {
            ccfgs = cacheConfigurations(CACHES, GROUP1, "testCache1-");

            client = i == NODES - 1;

            startGrid(i);
        }

        Ignite client = ignite(NODES - 1);

        client.createCaches(Arrays.asList(cacheConfigurations(CACHES, GROUP2, "testCache2-")));

        checkCacheDiscoveryDataConsistent();

        for (int i = 0; i < NODES; i++) {
            for (int c = 0; c < CACHES; c++) {
                if (c % 100 == 0)
                    log.info("Check node: " + i);

                checkCache(i, "testCache1-" + c, 1);
                checkCache(i, "testCache2-" + c, 1);
            }
        }

        GridTestUtils.runMultiThreaded(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer idx) {
                stopGrid(idx);
            }
        }, NODES, "stopThread");
    }

    /**
     * @throws Exception If failed.
     */
    public void testRebalance1() throws Exception {
        Ignite srv0 = startGrid(0);

        IgniteCache<Object, Object> srv0Cache1 =
            srv0.createCache(cacheConfiguration(GROUP1, CACHE1, PARTITIONED, ATOMIC, 2, false));
        IgniteCache<Object, Object> srv0Cache2 =
            srv0.createCache(cacheConfiguration(GROUP1, CACHE2, PARTITIONED, ATOMIC, 2, false));

        for (int i = 0; i < 10; i++)
            srv0Cache1.put(new Key1(i), i);

        assertEquals(10, srv0Cache1.size());
        assertEquals(10, srv0Cache1.localSize());
        assertEquals(0, srv0Cache2.size());

        Ignite srv1 = startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> srv1Cache1 = srv1.cache("cache1");
        IgniteCache<Object, Object> srv1Cache2 = srv1.cache("cache2");

        assertEquals(20, srv0Cache1.size(CachePeekMode.ALL));
        assertEquals(10, srv0Cache1.localSize(CachePeekMode.ALL));
        assertEquals(0, srv0Cache2.size(CachePeekMode.ALL));
        assertEquals(0, srv0Cache2.localSize(CachePeekMode.ALL));

        assertEquals(20, srv1Cache1.size(CachePeekMode.ALL));
        assertEquals(10, srv1Cache1.localSize(CachePeekMode.ALL));
        assertEquals(0, srv1Cache2.size(CachePeekMode.ALL));
        assertEquals(0, srv1Cache2.localSize(CachePeekMode.ALL));

        for (int i = 0; i < 10; i++) {
            assertEquals(i, srv0Cache1.localPeek(new Key1(i)));
            assertEquals(i, srv1Cache1.localPeek(new Key1(i)));
        }

        for (int i = 0; i < 20; i++)
            srv0Cache2.put(new Key1(i), i + 1);

        Ignite srv2 = startGrid(2);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> srv2Cache1 = srv2.cache("cache1");
        IgniteCache<Object, Object> srv2Cache2 = srv2.cache("cache2");

        assertEquals(30, srv2Cache1.size(CachePeekMode.ALL));
        assertEquals(10, srv2Cache1.localSize(CachePeekMode.ALL));
        assertEquals(60, srv2Cache2.size(CachePeekMode.ALL));
        assertEquals(20, srv1Cache2.localSize(CachePeekMode.ALL));

        for (int i = 0; i < 10; i++)
            assertEquals(i, srv2Cache1.localPeek(new Key1(i)));

        for (int i = 0; i < 20; i++)
            assertEquals(i + 1, srv2Cache2.localPeek(new Key1(i)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoKeyIntersectTx() throws Exception {
        testNoKeyIntersect(TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoKeyIntersectAtomic() throws Exception {
        testNoKeyIntersect(ATOMIC);
    }

    /**
     * @param atomicityMode Atomicity mode.
     * @throws Exception If failed.
     */
    private void testNoKeyIntersect(CacheAtomicityMode atomicityMode) throws Exception {
        startGrid(0);

        testNoKeyIntersect(atomicityMode, false);

        testNoKeyIntersect(atomicityMode, true);

        startGridsMultiThreaded(1, 4);

        testNoKeyIntersect(atomicityMode, false);

        testNoKeyIntersect(atomicityMode, true);
    }

    /**
     * @param keys Keys.
     * @param rnd Random.
     * @return Added key.
     */
    private Integer addKey(Set<Integer> keys, ThreadLocalRandom rnd) {
        for (;;) {
            Integer key = rnd.nextInt(100_000);

            if (keys.add(key))
                return key;
        }
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param heapCache On heap cache flag.
     * @throws Exception If failed.
     */
    private void testNoKeyIntersect(CacheAtomicityMode atomicityMode, boolean heapCache) throws Exception {
        Ignite srv0 = ignite(0);

        try {
            IgniteCache cache1 = srv0.
                createCache(cacheConfiguration(GROUP1, "c1", PARTITIONED, atomicityMode, 1, heapCache));

            Set<Integer> keys = new LinkedHashSet<>(30);

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            for (int i = 0; i < 10; i++) {
                Integer key = addKey(keys, rnd);

                cache1.put(key, key);
                cache1.put(new Key1(key), new Value1(key));
                cache1.put(new Key2(key), new Value2(key));
            }

            assertEquals(30, cache1.size());

            IgniteCache cache2 = srv0.
                createCache(cacheConfiguration(GROUP1, "c2", PARTITIONED, atomicityMode, 1, heapCache));

            assertEquals(30, cache1.size());
            assertEquals(0 , cache2.size());

            for (Integer key : keys) {
                assertNull(cache2.get(key));
                assertNull(cache2.get(new Key1(key)));
                assertNull(cache2.get(new Key2(key)));

                cache2.put(key, key + 1);
                cache2.put(new Key1(key), new Value1(key + 1));
                cache2.put(new Key2(key), new Value2(key + 1));
            }

            assertEquals(30, cache1.size());
            assertEquals(30, cache2.size());

            for (int i = 0; i < 10; i++) {
                Integer key = addKey(keys, rnd);

                cache2.put(key, key + 1);
                cache2.put(new Key1(key), new Value1(key + 1));
                cache2.put(new Key2(key), new Value2(key + 1));
            }

            assertEquals(30, cache1.size());
            assertEquals(60, cache2.size());

            int i = 0;

            for (Integer key : keys) {
                if (i++ < 10) {
                    assertEquals(key, cache1.get(key));
                    assertEquals(new Value1(key), cache1.get(new Key1(key)));
                    assertEquals(new Value2(key), cache1.get(new Key2(key)));
                }
                else {
                    assertNull(cache1.get(key));
                    assertNull(cache1.get(new Key1(key)));
                    assertNull(cache1.get(new Key2(key)));
                }

                assertEquals(key + 1, cache2.get(key));
                assertEquals(new Value1(key + 1), cache2.get(new Key1(key)));
                assertEquals(new Value2(key + 1), cache2.get(new Key2(key)));
            }

            IgniteCache cache3 = srv0.
                createCache(cacheConfiguration(GROUP1, "c3", PARTITIONED, atomicityMode, 1, heapCache));

            assertEquals(30, cache1.size());
            assertEquals(60, cache2.size());
            assertEquals(0, cache3.size());

            for (Integer key : keys) {
                assertNull(cache3.get(key));
                assertNull(cache3.get(new Key1(key)));
                assertNull(cache3.get(new Key2(key)));
            }

            for (Integer key : keys) {
                cache3.put(key, key);
                cache3.put(new Key1(key), new Value1(key));
                cache3.put(new Key2(key), new Value2(key));
            }

            i = 0;

            for (Integer key : keys) {
                if (i++ < 10) {
                    assertEquals(key, cache1.get(key));
                    assertEquals(new Value1(key), cache1.get(new Key1(key)));
                    assertEquals(new Value2(key), cache1.get(new Key2(key)));
                }
                else {
                    assertNull(cache1.get(key));
                    assertNull(cache1.get(new Key1(key)));
                    assertNull(cache1.get(new Key2(key)));
                }

                assertEquals(key + 1, cache2.get(key));
                assertEquals(new Value1(key + 1), cache2.get(new Key1(key)));
                assertEquals(new Value2(key + 1), cache2.get(new Key2(key)));

                assertEquals(key, cache3.get(key));
                assertEquals(new Value1(key), cache3.get(new Key1(key)));
                assertEquals(new Value2(key), cache3.get(new Key2(key)));
            }

            i = 0;

            for (Integer key : keys) {
                if (i++ == 3)
                    break;

                cache1.remove(key);
                cache1.remove(new Key1(key));
                cache1.remove(new Key2(key));

                assertNull(cache1.get(key));
                assertNull(cache1.get(new Key1(key)));
                assertNull(cache1.get(new Key2(key)));

                assertEquals(key + 1, cache2.get(key));
                assertEquals(new Value1(key + 1), cache2.get(new Key1(key)));
                assertEquals(new Value2(key + 1), cache2.get(new Key2(key)));

                assertEquals(key, cache3.get(key));
                assertEquals(new Value1(key), cache3.get(new Key1(key)));
                assertEquals(new Value2(key), cache3.get(new Key2(key)));
            }

            cache1.removeAll();

            for (Integer key : keys) {
                assertNull(cache1.get(key));
                assertNull(cache1.get(new Key1(key)));
                assertNull(cache1.get(new Key2(key)));

                assertEquals(key + 1, cache2.get(key));
                assertEquals(new Value1(key + 1), cache2.get(new Key1(key)));
                assertEquals(new Value2(key + 1), cache2.get(new Key2(key)));

                assertEquals(key, cache3.get(key));
                assertEquals(new Value1(key), cache3.get(new Key1(key)));
                assertEquals(new Value2(key), cache3.get(new Key2(key)));
            }

            cache2.removeAll();

            for (Integer key : keys) {
                assertNull(cache1.get(key));
                assertNull(cache1.get(new Key1(key)));
                assertNull(cache1.get(new Key2(key)));

                assertNull(cache2.get(key));
                assertNull(cache2.get(new Key1(key)));
                assertNull(cache2.get(new Key2(key)));

                assertEquals(key, cache3.get(key));
                assertEquals(new Value1(key), cache3.get(new Key1(key)));
                assertEquals(new Value2(key), cache3.get(new Key2(key)));
            }

            if (atomicityMode == TRANSACTIONAL)
                testNoKeyIntersectTxLocks(cache1, cache2);
        }
        finally {
            srv0.destroyCaches(Arrays.asList("c1", "c2", "c3"));
        }
    }

    /**
     * @param cache1 Cache1.
     * @param cache2 Cache2.
     * @throws Exception If failed.
     */
    private void testNoKeyIntersectTxLocks(final IgniteCache cache1, final IgniteCache cache2) throws Exception {
        final Ignite node = (Ignite)cache1.unwrap(Ignite.class);

        for (int i = 0; i < 5; i++) {
            final Integer key = ThreadLocalRandom.current().nextInt(1000);

            Lock lock = cache1.lock(key);

            lock.lock();

            try {
                IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable() {
                    @Override public Object call() throws Exception {
                        Lock lock1 = cache1.lock(key);

                        assertFalse(lock1.tryLock());

                        Lock lock2 = cache2.lock(key);

                        assertTrue(lock2.tryLock());

                        lock2.unlock();

                        return null;
                    }
                }, "lockThread");

                fut.get(10_000);
            }
            finally {
                lock.unlock();
            }

            try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache1.put(key, 1);

                IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            cache2.put(key, 2);

                            tx.commit();
                        }

                        assertEquals(2, cache2.get(key));

                        return null;
                    }
                }, "txThread");

                fut.get(10_000);

                tx.commit();
            }

            assertEquals(1, cache1.get(key));
            assertEquals(2, cache2.get(key));

            try (Transaction tx = node.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                Integer val = (Integer)cache1.get(key);

                cache1.put(key, val + 10);

                IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            cache2.put(key, 3);

                            tx.commit();
                        }

                        assertEquals(3, cache2.get(key));

                        return null;
                    }
                }, "txThread");

                fut.get(10_000);

                tx.commit();
            }

            assertEquals(11, cache1.get(key));
            assertEquals(3, cache2.get(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheApiTxPartitioned() throws Exception {
        cacheApiTest(PARTITIONED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheApiTxReplicated() throws Exception {
        cacheApiTest(REPLICATED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheApiAtomicPartitioned() throws Exception {
        cacheApiTest(PARTITIONED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheApiAtomicReplicated() throws Exception {
        cacheApiTest(REPLICATED, ATOMIC);
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @throws Exception If failed.
     */
    private void cacheApiTest(CacheMode cacheMode, CacheAtomicityMode atomicityMode) throws Exception {
        startGridsMultiThreaded(4);

        client = true;

        startGrid(4);

        int[] backups = cacheMode == REPLICATED ? new int[]{Integer.MAX_VALUE} : new int[]{0, 1, 2, 3};

        for (int backups0 : backups)
            cacheApiTest(cacheMode, atomicityMode, backups0, false, false, false);

        int backups0 = cacheMode == REPLICATED ? Integer.MAX_VALUE :
            backups[ThreadLocalRandom.current().nextInt(backups.length)];

        cacheApiTest(cacheMode, atomicityMode, backups0, true, false, false);

        if (cacheMode == PARTITIONED) {
            // Hire the f variable is used as a bit set where 2 last bits
            // determine whether a near cache is used on server/client side.
            // The case without near cache is already tested at this point.
            for (int f : new int[]{1, 2, 3}) {
                cacheApiTest(cacheMode, atomicityMode, backups0, false, nearSrv(f), nearClient(f));
                cacheApiTest(cacheMode, atomicityMode, backups0, true, nearSrv(f), nearClient(f));
            }
        }
    }

    /**
     * @param flag Flag.
     * @return {@code True} if near cache should be used on a client side.
     */
    private boolean nearClient(int flag) {
        return (flag & 0b01) == 0b01;
    }

    /**
     * @param flag Flag.
     * @return {@code True} if near cache should be used on a server side.
     */
    private boolean nearSrv(int flag) {
        return (flag & 0b10) == 0b10;
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param backups Number of backups.
     * @param heapCache On heap cache flag.
     * @param nearSrv {@code True} if near cache should be used on a server side.
     * @param nearClient {@code True} if near cache should be used on a client side.
     */
    private void cacheApiTest(CacheMode cacheMode,
                              CacheAtomicityMode atomicityMode,
                              int backups,
                              boolean heapCache,
                              boolean nearSrv,
                              boolean nearClient) throws Exception {
        Ignite srv0 = ignite(0);

        NearCacheConfiguration nearCfg = nearSrv ? new NearCacheConfiguration() : null;

        srv0.createCache(cacheConfiguration(GROUP1, "cache-0", cacheMode, atomicityMode, backups, heapCache)
                .setNearConfiguration(nearCfg));

        srv0.createCache(cacheConfiguration(GROUP1, "cache-1", cacheMode, atomicityMode, backups, heapCache));
        srv0.createCache(cacheConfiguration(GROUP2, "cache-2", cacheMode, atomicityMode, backups, heapCache)
                .setNearConfiguration(nearCfg));

        srv0.createCache(cacheConfiguration(null, "cache-3", cacheMode, atomicityMode, backups, heapCache));

        awaitPartitionMapExchange();

        if(nearClient) {
            Ignite clientNode = ignite(4);

            clientNode.createNearCache("cache-0", new NearCacheConfiguration());
            clientNode.createNearCache("cache-2", new NearCacheConfiguration());
        }

        try {
            for (final Ignite node : Ignition.allGrids()) {
                List<Callable<?>> ops = new ArrayList<>();

                for (int i = 0; i < 4; i++)
                    ops.add(testSet(node.cache("cache-" + i), cacheMode, atomicityMode, backups, heapCache, node));

                // async operations
                GridTestUtils.runMultiThreaded(ops, "cacheApiTest");
            }
        }
        finally {
            for (int i = 0; i < 4; i++)
                srv0.destroyCache("cache-" + i);
        }
    }

    /**
     * @param cache Cache.
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param backups Number of backups.
     * @param heapCache On heap cache flag.
     * @param node Ignite node.
     * @return Callable for the test operations.
     */
    private Callable<?> testSet(
        final IgniteCache<Object, Object> cache,
        final CacheMode cacheMode,
        final CacheAtomicityMode atomicityMode,
        final int backups,
        final boolean heapCache,
        final Ignite node) {
        return new Callable<Void>() {
            @Override public Void call() throws Exception {
                log.info("Test cache [node=" + node.name() +
                    ", cache=" + cache.getName() +
                    ", mode=" + cacheMode +
                    ", atomicity=" + atomicityMode +
                    ", backups=" + backups +
                    ", heapCache=" + heapCache +
                    ']');

                cacheApiTest(cache);

                return null;
            }
        };
    }

    /**
     * @param cache Cache.
     */
    private void cacheApiTest(IgniteCache cache) throws Exception {
        cachePutAllGetAll(cache);
        cachePutRemove(cache);
        cachePutGet(cache);
        cachePutGetAndPut(cache);
        cacheQuery(cache);
        cacheInvokeAll(cache);
        cacheInvoke(cache);
        cacheDataStreamer(cache);
    }

    /**
     * @param cache Cache.
     */
    private void tearDown(IgniteCache cache) {
        cache.clear();
        cache.removeAll();
    }

    /**
     * @param cache Cache.
     */
    private void cacheDataStreamer(final IgniteCache cache) throws Exception {
        final int keys = 1000;
        final int loaders = 4;

        final Integer[] data = generateData(keys * loaders);

        // stream through a client node
        Ignite clientNode = ignite(4);

        List<Callable<?>> cls = new ArrayList<>(loaders);

        for (final int i : sequence(loaders)) {
            final IgniteDataStreamer ldr = clientNode.dataStreamer(cache.getName());
            ldr.autoFlushFrequency(0);

            cls.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    List<IgniteFuture> futs = new ArrayList<>(keys);

                    for (int j = 0, size = keys * loaders; j < size; j++) {
                        if (j % loaders == i)
                            futs.add(ldr.addData(j, data[j]));

                        if(j % (100 * loaders) == 0)
                            ldr.flush();
                    }

                    ldr.flush();

                    for (IgniteFuture fut : futs)
                        fut.get();

                    return null;
                }
            });
        }

        GridTestUtils.runMultiThreaded(cls, "loaders");

        Set<Integer> keysSet = sequence(data.length);

        for (Cache.Entry<Integer, Integer> entry : (IgniteCache<Integer, Integer>)cache) {
            assertTrue(keysSet.remove(entry.getKey()));
            assertEquals(data[entry.getKey()], entry.getValue());
        }

        assertTrue(keysSet.isEmpty());

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutAllGetAll(IgniteCache cache) {
        Map<Integer, Integer> data = generateDataMap(1000);

        cache.putAll(data);

        Map data0 = cache.getAll(data.keySet());

        assertEquals(data.size(), data0.size());

        for (Map.Entry<Integer, Integer> entry : data.entrySet()) {
            assertEquals(entry.getValue(), data0.get(entry.getKey()));
        }

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutRemove(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val = rnd.nextInt();

        cache.put(key, val);

        assertTrue(cache.remove(key));

        assertNull(cache.get(key));

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutGet(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val = rnd.nextInt();

        cache.put(key, val);

        Object val0 = cache.get(key);

        assertEquals(val, val0);

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cachePutGetAndPut(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val1 = rnd.nextInt();
        Integer val2 = rnd.nextInt();

        cache.put(key, val1);

        Object val0 = cache.getAndPut(key, val2);

        assertEquals(val1, val0);

        val0 = cache.get(key);

        assertEquals(val2, val0);

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cacheQuery(IgniteCache cache) {
        Map<Integer, Integer> data = generateDataMap(1000);

        cache.putAll(data);

        ScanQuery<Integer, Integer> qry = new ScanQuery<>(new IgniteBiPredicate<Integer, Integer>() {
            @Override public boolean apply(Integer integer, Integer integer2) {
                return integer % 2 == 0;
            }
        });

        List<Cache.Entry<Integer, Integer>> all = cache.query(qry).getAll();

        assertEquals(all.size(), data.size() / 2);

        for (Cache.Entry<Integer, Integer> entry : all) {
            assertEquals(0, entry.getKey() % 2);
            assertEquals(entry.getValue(), data.get(entry.getKey()));
        }

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cacheInvokeAll(IgniteCache cache) {
        int keys = 1000;
        Map<Integer, Integer> data = generateDataMap(keys);

        cache.putAll(data);

        Random rnd = ThreadLocalRandom.current();

        int one = rnd.nextInt();
        int two = rnd.nextInt();

        Map<Integer, CacheInvokeResult<Integer>> res = cache.invokeAll(data.keySet(), new CacheEntryProcessor<Integer, Integer, Integer>() {
            @Override public Integer process(MutableEntry<Integer, Integer> entry, Object... arguments) throws EntryProcessorException {
                Object removed = ((Map)arguments[0]).remove(entry.getKey());

                assertEquals(removed, entry.getValue());

                // Some calculation
                return (Integer)arguments[1] + (Integer)arguments[2];
            }
        }, data, one, two);

        assertEquals(keys, res.size());
        assertEquals(one + two, (Object)res.get(0).get());

        tearDown(cache);
    }

    /**
     * @param cache Cache.
     */
    private void cacheInvoke(IgniteCache cache) {
        Random rnd = ThreadLocalRandom.current();

        Integer key = rnd.nextInt();
        Integer val = rnd.nextInt();

        cache.put(key, val);

        int one = rnd.nextInt();
        int two = rnd.nextInt();

        Object res = cache.invoke(key, new CacheEntryProcessor<Integer, Integer, Integer>() {
            @Override public Integer process(MutableEntry<Integer, Integer> entry, Object... arguments) throws EntryProcessorException {
                assertEquals(arguments[0], entry.getValue());

                // Some calculation
                return (Integer)arguments[1] + (Integer)arguments[2];
            }
        }, val, one, two);

        assertEquals(one + two, res);

        tearDown(cache);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentOperationsSameKeys() throws Exception {
        final int SRVS = 4;
        final int CLIENTS = 4;
        final int NODES = SRVS + CLIENTS;

        startGrid(0);

        Ignite srv0 = startGridsMultiThreaded(1, SRVS - 1);

        client = true;

        startGridsMultiThreaded(SRVS, CLIENTS);

        srv0.createCache(cacheConfiguration(GROUP1, "a0", PARTITIONED, ATOMIC, 1, false));
        srv0.createCache(cacheConfiguration(GROUP1, "a1", PARTITIONED, ATOMIC, 1, false));
        srv0.createCache(cacheConfiguration(GROUP1, "t0", PARTITIONED, TRANSACTIONAL, 1, false));
        srv0.createCache(cacheConfiguration(GROUP1, "t1", PARTITIONED, TRANSACTIONAL, 1, false));

        final List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < 50; i++)
            keys.add(i);

        final AtomicBoolean err = new AtomicBoolean();

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture fut1 = updateFuture(NODES, "a0", keys, false, stop, err);
        IgniteInternalFuture fut2 = updateFuture(NODES, "a1", keys, true, stop, err);
        IgniteInternalFuture fut3 = updateFuture(NODES, "t0", keys, false, stop, err);
        IgniteInternalFuture fut4 = updateFuture(NODES, "t1", keys, true, stop, err);

        try {
            for (int i = 0; i < 15 && !stop.get(); i++)
                U.sleep(1_000);
        }
        finally {
            stop.set(true);
        }

        fut1.get();
        fut2.get();
        fut3.get();
        fut4.get();

        assertFalse("Unexpected error, see log for details", err.get());
    }

    /**
     * @param nodes Total number of nodes.
     * @param cacheName Cache name.
     * @param keys Keys to update.
     * @param reverse {@code True} if update in reverse order.
     * @param stop Stop flag.
     * @param err Error flag.
     * @return Update future.
     */
    private IgniteInternalFuture updateFuture(final int nodes,
        final String cacheName,
        final List<Integer> keys,
        final boolean reverse,
        final AtomicBoolean stop,
        final AtomicBoolean err) {
        final AtomicInteger idx = new AtomicInteger();

        return GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                try {
                    Ignite node = ignite(idx.getAndIncrement() % nodes);

                    log.info("Start thread [node=" + node.name() + ']');

                    IgniteCache cache = node.cache(cacheName);

                    Map<Integer, Integer> map = new LinkedHashMap<>();

                    if (reverse) {
                        for (int i = keys.size() - 1; i >= 0; i--)
                            map.put(keys.get(i), 2);
                    }
                    else {
                        for (Integer key : keys)
                            map.put(key, 1);
                    }

                    while (!stop.get())
                        cache.putAll(map);
                }
                catch (Exception e) {
                    err.set(true);

                    log.error("Unexpected error: " + e, e);

                    stop.set(true);
                }

                return null;
            }
        }, nodes * 2, "update-" + cacheName + "-" + reverse);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentOperationsAndCacheDestroy() throws Exception {
        final int SRVS = 4;
        final int CLIENTS = 4;
        final int NODES = SRVS + CLIENTS;

        startGrid(0);

        Ignite srv0 = startGridsMultiThreaded(1, SRVS - 1);

        client = true;

        startGridsMultiThreaded(SRVS, CLIENTS);

        final int CACHES = 8;

        final int grp1Backups = ThreadLocalRandom.current().nextInt(3);
        final int grp2Backups = ThreadLocalRandom.current().nextInt(3);

        log.info("Start test [grp1Backups=" + grp1Backups + ", grp2Backups=" + grp2Backups + ']');

        for (int i = 0; i < CACHES; i++) {
            srv0.createCache(
                cacheConfiguration(GROUP1, GROUP1 + "-" + i, PARTITIONED, ATOMIC, grp1Backups, i % 2 == 0));

            srv0.createCache(
                cacheConfiguration(GROUP2, GROUP2 + "-" + i, PARTITIONED, TRANSACTIONAL, grp2Backups, i % 2 == 0));
        }

        final AtomicInteger idx = new AtomicInteger();

        final AtomicBoolean err = new AtomicBoolean();

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture opFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    Ignite node = ignite(idx.getAndIncrement() % NODES);

                    log.info("Start thread [node=" + node.name() + ']');

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!stop.get()) {
                        String grp = rnd.nextBoolean() ? GROUP1 : GROUP2;
                        int cacheIdx = rnd.nextInt(CACHES);

                        IgniteCache cache = node.cache(grp + "-" + cacheIdx);

                        for (int i = 0; i < 10; i++)
                            cacheOperation(rnd, cache);
                    }
                }
                catch (Exception e) {
                    err.set(true);

                    log.error("Unexpected error(1): " + e, e);

                    stop.set(true);
                }
            }
        }, (SRVS + CLIENTS) * 2, "op-thread");

        IgniteInternalFuture cacheFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    int cntr = 0;

                    while (!stop.get()) {
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        String grp;
                        int backups;

                        if (rnd.nextBoolean()) {
                            grp = GROUP1;
                            backups = grp1Backups;
                        }
                        else {
                            grp = GROUP2;
                            backups = grp2Backups;
                        }

                        Ignite node = ignite(rnd.nextInt(NODES));

                        log.info("Create cache [node=" + node.name() + ", grp=" + grp + ']');

                        IgniteCache cache = node.createCache(cacheConfiguration(grp, "tmpCache-" + cntr++,
                            PARTITIONED,
                            rnd.nextBoolean() ? ATOMIC : TRANSACTIONAL,
                            backups,
                            rnd.nextBoolean()));

                        for (int i = 0; i < 10; i++)
                            cacheOperation(rnd, cache);

                        log.info("Destroy cache [node=" + node.name() + ", grp=" + grp + ']');

                        node.destroyCache(cache.getName());
                    }
                }
                catch (Exception e) {
                    err.set(true);

                    log.error("Unexpected error(2): " + e, e);

                    stop.set(true);
                }
            }
        }, "cache-destroy-thread");

        try {
            for (int i = 0; i < 30 && !stop.get(); i++)
                U.sleep(1_000);
        }
        finally {
            stop.set(true);
        }

        opFut.get();
        cacheFut.get();

        assertFalse("Unexpected error, see log for details", err.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testConfigurationConsistencyValidation() throws Exception {
        startGrids(2);

        client = true;

        startGrid(2);

        ignite(0).createCache(cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 1, false));

        for (int i = 0; i < 3; i++) {
            try {
                ignite(i).createCache(cacheConfiguration(GROUP1, "c2", REPLICATED, ATOMIC, Integer.MAX_VALUE, false));

                fail();
            }
            catch (CacheException e) {
                assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("Cache mode mismatch for caches related to the same group [groupName=grp1"));
            }

            try {
                ignite(i).createCache(cacheConfiguration(GROUP1, "c2", PARTITIONED, ATOMIC, 2, false));

                fail();
            }
            catch (CacheException e) {
                assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("Backups mismatch for caches related to the same group [groupName=grp1"));
            }
        }
    }

    /**
     * @param rnd Random.
     * @param cache Cache.
     */
    private void cacheOperation(ThreadLocalRandom rnd, IgniteCache cache) {
        Object key = rnd.nextInt(1000);

        switch (rnd.nextInt(4)) {
            case 0:
                cache.put(key, 1);

                break;

            case 1:
                cache.get(key);

                break;

            case 2:
                cache.remove(key);

                break;

            case 3:
                cache.localPeek(key);

                break;
        }
    }

    /**
     *
     */
    static class Key1 implements Serializable {
        /** */
        private int id;

        /**
         * @param id ID.
         */
        Key1(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key1 key = (Key1)o;

            return id == key.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    static class Key2 implements Serializable {
        /** */
        private int id;

        /**
         * @param id ID.
         */
        Key2(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key2 key = (Key2)o;

            return id == key.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    static class Value1 implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        public Value1(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Value1 val1 = (Value1)o;

            return val == val1.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }
    }

    /**
     *
     */
    static class Value2 implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        public Value2(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Value2 val2 = (Value2)o;

            return val == val2.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }
    }

    /**
     * @param grpName Cache group name.
     * @param name Cache name.
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param backups Backups number.
     * @param heapCache On heap cache flag.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(
        String grpName,
        String name,
        CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        int backups,
        boolean heapCache
    ) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setGroupName(grpName);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(backups);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setOnheapCacheEnabled(heapCache);

        return ccfg;
    }

    /**
     * @param idx Node index.
     * @param grpName Cache group name.
     * @param expGrp {@code True} if cache group should be created.
     * @throws IgniteCheckedException If failed.
     */
    private void checkCacheGroup(int idx, final String grpName, final boolean expGrp) throws IgniteCheckedException {
        final IgniteKernal node = (IgniteKernal)ignite(idx);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return expGrp == (cacheGroup(node, grpName) != null);
            }
        }, 1000));

        assertNotNull(node.context().cache().cache(CU.UTILITY_CACHE_NAME));
    }

    /**
     * @param node Node.
     * @param grpName Cache group name.
     * @return Cache group.
     */
    private CacheGroupInfrastructure cacheGroup(IgniteKernal node, String grpName) {
        for (CacheGroupInfrastructure grp : node.context().cache().cacheGroups()) {
            if (grpName.equals(grp.name()))
                return grp;
        }

        return null;
    }
}
