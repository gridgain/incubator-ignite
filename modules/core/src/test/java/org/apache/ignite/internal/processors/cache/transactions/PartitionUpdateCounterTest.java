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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.PartitionAtomicUpdateCounterImpl;
import org.apache.ignite.internal.processors.cache.PartitionTxUpdateCounterImpl;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.T4;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

/**
 * Basic partition counter tests.
 */
public class PartitionUpdateCounterTest extends GridCommonAbstractTest {
    /** */
    private CacheAtomicityMode mode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setClientMode("client".equals(igniteInstanceName));

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(false)
                    .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
            )
            .setWalHistorySize(1000)
            .setWalMode(WALMode.LOG_ONLY)
            .setWalSegmentSize(8 * 1024 * 1024);

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setGroupName("jopa").
            setAffinity(new RendezvousAffinityFunction(false, 32)).
            setBackups(2).
            setCacheMode(CacheMode.PARTITIONED).
            setAtomicityMode(mode));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        //cleanPersistenceDir();
    }

    /**
     * Test applying update multiple times in random order.
     */
    public void testRandomUpdates() {
        List<int[]> tmp = generateUpdates(1000, 5);

        long expTotal = tmp.stream().mapToInt(pair -> pair[1]).sum();

        PartitionUpdateCounter pc = null;

        for (int i = 0; i < 100; i++) {
            Collections.shuffle(tmp);

            PartitionUpdateCounter pc0 = new PartitionTxUpdateCounterImpl();

            for (int[] pair : tmp)
                pc0.update(pair[0], pair[1]);

            if (pc == null)
                pc = pc0;
            else {
                assertEquals(pc, pc0);
                assertEquals(expTotal, pc0.get());
                assertTrue(pc0.sequential());

                pc = pc0;
            }
        }
    }

    /**
     * Test if pc correctly reports stale (before current counter) updates.
     * This information is used for logging rollback records only once.
     */
    public void testStaleUpdate() {
        PartitionUpdateCounter pc = new PartitionTxUpdateCounterImpl();

        assertTrue(pc.update(0, 1));
        assertFalse(pc.update(0, 1));

        assertTrue(pc.update(2, 1));
        assertFalse(pc.update(2, 1));

        assertTrue(pc.update(1, 1));
        assertFalse(pc.update(1, 1));
    }

    /**
     * Test multithreaded updates of pc in various modes.
     *
     * @throws Exception If failed.
     */
    public void testMixedModeMultithreaded() throws Exception {
        PartitionUpdateCounter pc = new PartitionTxUpdateCounterImpl();

        AtomicBoolean stop = new AtomicBoolean();

        Queue<long[]> reservations = new ConcurrentLinkedQueue<>();

        LongAdder reserveCntr = new LongAdder();

        IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
            while(!stop.get() || !reservations.isEmpty()) {
                if (!stop.get() && ThreadLocalRandom.current().nextBoolean()) {
                    int size = ThreadLocalRandom.current().nextInt(9) + 1;

                    reservations.add(new long[] {pc.reserve(size), size}); // Only update if stop flag is set.

                    reserveCntr.add(size);
                }
                else {
                    long[] reserved = reservations.poll();

                    if (reserved == null)
                        continue;

                    pc.update(reserved[0], reserved[1]);
                }
            }
        }, Runtime.getRuntime().availableProcessors() * 2, "updater-thread");

        doSleep(10_000);

        stop.set(true);

        fut.get();

        assertTrue(reservations.isEmpty());

        log.info("counter=" + pc.toString() + ", reserveCntrLocal=" + reserveCntr.sum());

        assertTrue(pc.sequential());

        assertTrue(pc.get() == pc.reserved());

        assertEquals(reserveCntr.sum(), pc.get());
    }

    /**
     * Test logic for handling gaps limit.
     */
    public void testMaxGaps() {
        PartitionUpdateCounter pc = new PartitionTxUpdateCounterImpl();

        int i;
        for (i = 1; i <= PartitionTxUpdateCounterImpl.MAX_MISSED_UPDATES; i++)
            pc.update(i * 3, i * 3 + 1);

        i++;
        try {
            pc.update(i * 3, i * 3 + 1);

            fail();
        }
        catch (Exception e) {
            // Expected.
        }
    }

    /**
     *
     */
    public void testFoldIntermediateUpdates() {
        PartitionUpdateCounter pc = new PartitionTxUpdateCounterImpl();

        pc.update(0, 59);

        pc.update(60, 5);

        pc.update(67, 3);

        pc.update(65, 2);

        Iterator<long[]> it = pc.iterator();

        it.next();

        assertFalse(it.hasNext());

        pc.update(59, 1);

        assertTrue(pc.sequential());
    }

    /**
     *
     */
    public void testOutOfOrderUpdatesIterator() {
        PartitionUpdateCounter pc = new PartitionTxUpdateCounterImpl();

        pc.update(67, 3);

        pc.update(1, 58);

        pc.update(60, 5);

        Iterator<long[]> iter = pc.iterator();

        long[] upd = iter.next();

        assertEquals(1, upd[0]);
        assertEquals(58, upd[1]);

        upd = iter.next();

        assertEquals(60, upd[0]);
        assertEquals(5, upd[1]);

        upd = iter.next();

        assertEquals(67, upd[0]);
        assertEquals(3, upd[1]);

        assertFalse(iter.hasNext());
    }

    /**
     *
     */
    public void testOverlap() {
        PartitionUpdateCounter pc = new PartitionTxUpdateCounterImpl();

        assertTrue(pc.update(13, 3));

        assertTrue(pc.update(6, 7));

        assertFalse(pc.update(13, 3));

        assertFalse(pc.update(6, 7));

        Iterator<long[]> iter = pc.iterator();
        assertTrue(iter.hasNext());

        long[] upd = iter.next();

        assertEquals(6, upd[0]);
        assertEquals(10, upd[1]);

        assertFalse(iter.hasNext());
    }

    /** */
    public void testAtomicUpdateCounterMultithreaded() throws Exception {
        PartitionUpdateCounter cntr = new PartitionAtomicUpdateCounterImpl();

        AtomicInteger id = new AtomicInteger();

        final int max = 1000;

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int val;

                while ((val = id.incrementAndGet()) <= max) {
                    try {
                        cntr.update(val);
                    }
                    catch (IgniteCheckedException e) {
                        fail(X.getFullStackTrace(e));
                    }
                }
            }
        }, Runtime.getRuntime().availableProcessors() * 2, "updater");

        fut.get();

        assertEquals(max, cntr.get());
    }

    /**
     *
     */
    public void testWithPersistentNodeTx() throws Exception {
        testWithPersistentNode(CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     *
     */
    public void testWithPersistentNodeAtomic() throws Exception {
        testWithPersistentNode(CacheAtomicityMode.ATOMIC);
    }

    public void testCommit() throws Exception {
        mode = CacheAtomicityMode.TRANSACTIONAL;

        try {
            IgniteEx crd = startGrids(3);
            crd.cluster().active(true);
            awaitPartitionMapExchange();

            Ignite client = startGrid("client");
            IgniteCache<Integer, SampleObject> cache = client.cache(DEFAULT_CACHE_NAME);

//            try(IgniteDataStreamer<Object, Object> streamer = crd.dataStreamer(DEFAULT_CACHE_NAME)) {
//                for (int i = 0; i < 32 * 1_000; i++)
//                    streamer.addData(i, new SampleObject(i, "test" + i, i * 1000l));
//            }


            int i = 1;

            try(Transaction tx = client.transactions().txStart()) {
                //for (int i = 0; i < 32 * 10_000; i++)
                    //cache.put(i, new SampleObject(i, "test" + i, i * 1000l));

                cache.put(i++, new SampleObject(i, "test" + i, i * 1000l));
                //cache.put(++i, new SampleObject(i, "test" + i, i * 1000l));

                tx.commit();
            }

//            try(Transaction tx = client.transactions().txStart()) {
//                //for (int i = 0; i < 32 * 10_000; i++)
//                //cache.put(i, new SampleObject(i, "test" + i, i * 1000l));
//
//                SampleObject so = cache.get(i);
//                so.setSalary(so.getSalary() + 200);
//
//                cache.put(i, so);
//
//                tx.commit();
//            }
//
//            try(Transaction tx = client.transactions().txStart()) {
//                //for (int i = 0; i < 32 * 10_000; i++)
//                //cache.put(i, new SampleObject(i, "test" + i, i * 1000l));
//
//                cache.remove(i);
//
//                tx.commit();
//            }


//            printDifference(client, 0, DEFAULT_CACHE_NAME, true);
            //printHistory(client, 0, DEFAULT_CACHE_NAME, false);

            Collection<T2<String, String>> hists =
                client.compute(client.cluster().forServers()).broadcast(new GetHistory(), new T2<>(DEFAULT_CACHE_NAME, i));

            for (T2<String, String> hist : hists)
                log.info("ConsistentId=" + hist.get1() + ", hist=" + hist.get2());

            Collection<T2<String, String>> txHist =
                client.compute(client.cluster().forServers()).broadcast(new GetFinishedTx());

            for (T2<String, String> hist : txHist)
                log.info("ConsistentId=" + hist.get1() + ", txHist=" + hist.get2());

//            startGrid(3);
//
//            awaitPartitionMapExchange();
//
//            System.out.println();
        }
        finally {
            stopAllGrids();
        }
    }

    public static class SampleObject {
        private int id;
        private String name;
        private long salary;
        private byte[] data;

        public SampleObject(int id, String name, long salary) {
            this.id = id;
            this.name = name;
            this.salary = salary;
            this.data = new byte[128];
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getSalary() {
            return salary;
        }

        public void setSalary(long salary) {
            this.salary = salary;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            SampleObject object = (SampleObject)o;

            if (id != object.id)
                return false;
            if (salary != object.salary)
                return false;
            return name.equals(object.name);

        }

        @Override public int hashCode() {
            int result = id;
            result = 31 * result + name.hashCode();
            result = 31 * result + (int)(salary ^ (salary >>> 32));
            return result;
        }
    }

    /**
     * @param locNode Local node.
     * @param partId Partition id.
     * @param cacheName Cache name.
     * @param keepBinary Keep binary.
     */
    private void printDifference(Ignite locNode, int partId, String cacheName, boolean keepBinary) {
        IgniteClosure<T3<String, Integer, Boolean>, T2<String, Map<Object, Object>>> clo = new GetLocalPartitionKeys();

        List<T2<String /** Consistent id. */, Map<Object, Object> /** Entries. */>> res =
            new ArrayList<>(locNode.compute(locNode.cluster().forServers()).broadcast(clo, new T3<>(cacheName, partId, keepBinary)));

        // Remove same keys.
        Map<Object, Object> tmp = null;

        for (int i = 0; i < res.size(); i++) {
            T2<String, Map<Object, Object>> cur = res.get(i);

            if (tmp == null)
                tmp = new HashMap<>(cur.get2());
            else
                tmp.entrySet().retainAll(cur.get2().entrySet());
        }

        for (Map.Entry<Object, Object> entry : tmp.entrySet()) {
            for (T2<String, Map<Object, Object>> e : res)
                e.get2().remove(entry.getKey(), entry.getValue());
        }

        for (T2<String, Map<Object, Object>> e : res)
            log.info("ConsistentId=" + e.get1() + ", entries=" + e.get2());
    }

    /** */
    public static final class GetLocalPartitionKeys implements IgniteClosure<T3<String, Integer, Boolean>, T2<String, Map<Object, Object>>> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public T2<String, Map<Object, Object>> apply(T3<String, Integer, Boolean> arg) {
            T2<String, Map<Object, Object>> res = new T2<>((String) ignite.configuration().getConsistentId(), new HashMap<>());

            IgniteCache<Object, Object> cache = arg.get3() ? ignite.cache(arg.get1()).withKeepBinary() : ignite.cache(arg.get1());

            if (cache == null)
                return res;

            Iterable<Cache.Entry<Object, Object>> entries = cache.localEntries();

            for (Cache.Entry<Object, Object> entry : entries) {
                if (ignite.affinity(cache.getName()).partition(entry.getKey()) == arg.get2().intValue())
                    res.get2().put(entry.getKey(), entry.getValue());
            }

            return res;
        }
    }

    /** */
    public static final class GetHistory implements IgniteClosure<T2<String, Integer>, T2<String, String>> {
        /** */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public T2<String, String> apply(T2<String, Integer> arg) {
            IgniteInternalCache<Object, Object> cachex = ignite.cachex(arg.get1());

            @Nullable GridDhtLocalPartition partition = cachex.context().topology().localPartition(arg.get2());

            if (partition == null)
                return new T2<>((String) ignite.configuration().getConsistentId(), "");

            SB b = new SB();

            for (GridDhtLocalPartition.Trace objects : partition.trace) {
                b.a("\nkey=" + objects.key +
                    ", new=" + objects.prev +
                    ", old=" + objects.next +
                    ", op=" + objects.treeOp +
                    ", cntr=" + objects.updateCntr
                );
            }

            return new T2<>((String) ignite.configuration().getConsistentId(), b.toString());
        }
    }

    /** */
    public static final class GetFinishedTx implements IgniteCallable<T2<String, String>> {
        /** */
        @IgniteInstanceResource
        private IgniteEx ignite;

        @Override public T2<String, String> call() throws Exception {
            ConcurrentLinkedHashMap<GridCacheVersion, Object> map = ignite.context().cache().context().tm().completedVers();

            SB b = new SB();

            for (Map.Entry<GridCacheVersion, Object> entry : map.entrySet())
                b.a(entry.getKey().toString()).a(',').a(entry.getValue()).a('\n');


            return new T2<>((String) ignite.configuration().getConsistentId(), b.toString());
        }
    }

    /**
     * @param mode Mode.
     */
    private void testWithPersistentNode(CacheAtomicityMode mode) throws Exception {
        this.mode = mode;

        try {
            Ignite grid0 = startGrid(0);
            grid0.cluster().active(true);

            grid0.cache(DEFAULT_CACHE_NAME).put(0, 0);

            startGrid(1);

            grid0.cluster().setBaselineTopology(2);

            awaitPartitionMapExchange();

            grid0.cache(DEFAULT_CACHE_NAME).put(1, 1);

            assertPartitionsSame(idleVerify(grid0, DEFAULT_CACHE_NAME));

            printPartitionState(DEFAULT_CACHE_NAME, 0);

            stopGrid(grid0.name(), false);

            grid0 = startGrid(grid0.name());

            awaitPartitionMapExchange();

            PartitionUpdateCounter cntr = counter(0, grid0.name());

            switch (mode) {
                case ATOMIC:
                    assertTrue(cntr instanceof PartitionAtomicUpdateCounterImpl);
                    break;

                case TRANSACTIONAL:
                    assertTrue(cntr instanceof PartitionTxUpdateCounterImpl);
                    break;

                default:
                    fail(mode.toString());
            }

            assertEquals(cntr.initial(), cntr.get());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param cnt Count.
     * @param maxTxSize Max tx size.
     */
    private List<int[]> generateUpdates(int cnt, int maxTxSize) {
        int[] ints = new Random().ints(cnt, 1, maxTxSize + 1).toArray();

        int off = 0;

        List<int[]> res = new ArrayList<>(cnt);

        for (int i = 0; i < ints.length; i++) {
            int val = ints[i];

            res.add(new int[] {off, val});

            off += val;
        }

        return res;
    }
}
