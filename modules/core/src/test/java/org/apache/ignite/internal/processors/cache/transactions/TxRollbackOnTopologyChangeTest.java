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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.GridCacheUtilityKey;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.service.GridServiceDeploymentKey;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.tx.VisorTxInfo;
import org.apache.ignite.internal.visor.tx.VisorTxOperation;
import org.apache.ignite.internal.visor.tx.VisorTxTask;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;
import org.apache.ignite.internal.visor.tx.VisorTxTaskResult;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static java.lang.Thread.yield;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests an ability to rollback transactions on topology change.
 */
public class TxRollbackOnTopologyChangeTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "test";

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int SRV_CNT = 6;

    /** */
    private static final int CLNT_CNT = 2;

    /** */
    private static final int TOTAL_CNT = SRV_CNT + CLNT_CNT;

    /** */
    public static final int ITERATIONS = 100;

    /** */
    private int rollbackTimeout = 500;

    /** */
    private boolean rollbackOnlySrvNodes = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setClientMode(getTestIgniteInstanceIndex(igniteInstanceName) >= SRV_CNT);

        if (!rollbackOnlySrvNodes || !cfg.isClientMode())
            cfg.setTransactionConfiguration(new TransactionConfiguration().
                setTxTimeoutOnPartitionMapExchange(rollbackTimeout));

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(2);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Tests rollbacks on topology change.
     */
    public void testRollbackOnTopologyChange() throws Exception {
        startGridsMultiThreaded(TOTAL_CNT);

        final AtomicBoolean stop = new AtomicBoolean();

        final long seed = System.currentTimeMillis();

        final Random r = new Random(seed);

        log.info("Using seed: " + seed);

        AtomicIntegerArray reservedIdx = new AtomicIntegerArray(TOTAL_CNT);

        final int keysCnt = SRV_CNT - 1;

        for (int k = 0; k < keysCnt; k++)
            grid(0).cache(CACHE_NAME).put(k, (long)0);

        final CyclicBarrier b = new CyclicBarrier(keysCnt);

        AtomicInteger idGen = new AtomicInteger();

        final IgniteInternalFuture<?> txFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int key = idGen.getAndIncrement();

                List<Integer> keys = new ArrayList<>();

                for (int k = 0; k < keysCnt; k++)
                    keys.add(k);

                int cntr = 0;

                for (int i = 0; i < ITERATIONS; i++) {
                    cntr++;

                    int nodeId;

                    while (!reservedIdx.compareAndSet((nodeId = r.nextInt(TOTAL_CNT)), 0, 1))
                        doSleep(10);

                    U.awaitQuiet(b);

                    final IgniteEx grid = grid(nodeId);

                    try (final Transaction tx = grid.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 0)) {
                        reservedIdx.set(nodeId, 0);

                        // Construct deadlock
                        grid.cache(CACHE_NAME).get(keys.get(key));

                        // Should block.
                        grid.cache(CACHE_NAME).get(keys.get((key + 1) % keysCnt));

                        fail("Deadlock expected");
                    }
                    catch (Throwable t) {
                        // Expected.
                    }

                    if (key == 0)
                        log.info("Rolled back: " + cntr);
                }
            }
        }, keysCnt, "tx-lock-thread");

        final IgniteInternalFuture<?> restartFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!stop.get()) {
                    final int nodeId = r.nextInt(TOTAL_CNT);

                    if (!reservedIdx.compareAndSet(nodeId, 0, 1)) {
                        yield();

                        continue;
                    }

                    stopGrid(nodeId);

                    doSleep(500 + r.nextInt(1000));

                    startGrid(nodeId);

                    reservedIdx.set(nodeId, 0);
                }

                return null;
            }
        }, 1, "tx-restart-thread");

        txFut.get(); // Wait for iterations to complete.

        stop.set(true);

        restartFut.get();

        checkFutures();
    }

    /**
     *
     */
    public void testRollbackSystemTransaction() throws Exception {
        rollbackOnlySrvNodes = true;

        rollbackTimeout = 10000;

        startGridsMultiThreaded(TOTAL_CNT);

        CountDownLatch finishLatch = new CountDownLatch(1);

        CountDownLatch systemTxLatch = new CountDownLatch(1);

        CountDownLatch startLatch = new CountDownLatch(1);

        AtomicInteger idx = new AtomicInteger();

        List<IgniteInternalFuture> futs = new ArrayList<>(CLNT_CNT);

        Ignite prim = primaryNode(0, CACHE_NAME);

        List<Ignite> backups = backupNodes(0, CACHE_NAME);

        IgniteInternalFuture clTxFut = runAsync(new Runnable() {
            @Override public void run() {
                IgniteEx cl = grid(SRV_CNT);

                assertTrue(cl.configuration().isClientMode());

                try (Transaction transaction = cl.transactions().txStart()) {
                    cl.cache(CACHE_NAME).put(0, 0);

                    finishLatch.await();

                    transaction.commit(); // Will throw the exception.

                    fail();
                }
                catch (Exception e) {
                    // Ex
                }
            }
        });

        IgniteEx crd = null;

        for (Ignite ignite : G.allGrids()) {
            if (ignite.cluster().localNode().order() == 1) {
                crd = (IgniteEx)ignite;

                break;
            }
        }

        assertNotNull(crd);

        doSleep(2000);

        IgniteInternalCache<GridCacheUtilityKey, Object> utilityCache = crd.utilityCache();

        crd.events().enableLocal(EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);

        // Start tx on next topology.
        crd.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event event) {
                runAsync(new Runnable() {
                    @Override public void run() {
                        try (GridNearTxLocal tx = utilityCache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                            log.info("System tx xid: " + tx.xid());

                            systemTxLatch.countDown();

                            Map<String, String> schedules = (Map<String, String>)utilityCache.get(new GridServiceDeploymentKey("8"));

                            finishLatch.await();
                        }
                        catch (Exception e) {
                            log.error("err", e);
                        }
                    }
                });

                return true;
            }
        }, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);

        // Additional check to prevent tx rollback due to dht node leave.
        for (Ignite ignite : G.allGrids()) {
            if (!ignite.configuration().isClientMode() && !ignite.equals(prim) && !backups.contains(ignite)) {
                ignite.close();

                runAsync(new Runnable() {
                    @Override public void run() {
                        int stopIdx = getTestIgniteInstanceIndex(ignite.name());

                        doSleep(1000);

                        startLatch.countDown();

                        try {
                            startGrid(stopIdx);
                        }
                        catch (Exception e) {
                            fail();
                        }
                    }
                });

                break;
            }
        }

        U.awaitQuiet(systemTxLatch);

        doSleep(1000); // Wait for system tx blocking.

        IgniteEx client1 = grid(SRV_CNT);

        U.awaitQuiet(startLatch);

        doSleep(2000);

        txAction(client1, true);

        awaitPartitionMapExchange();

        checkFutures();
    }

    @Override protected long getTestTimeout() {
        return 1000000000L;
    }

    /**
     * @param client Client.
     * @param kill {@code True} to kill, else list.
     */
    private void txAction(Ignite client, boolean kill) {
        VisorTxTaskArg arg = new VisorTxTaskArg(kill ? VisorTxOperation.KILL : VisorTxOperation.LIST, null, null, null, null, null, null, null, null, null);

        Map<ClusterNode, VisorTxTaskResult> res = client.compute(client.cluster().forPredicate(F.alwaysTrue())).
            execute(new VisorTxTask(), new VisorTaskArgument<>(client.cluster().localNode().id(), arg, false));

        for (Map.Entry<ClusterNode, VisorTxTaskResult> entry : res.entrySet()) {
            if (entry.getValue().getInfos().isEmpty())
                continue;

            ClusterNode key = entry.getKey();

            log.info(key.toString());

            for (VisorTxInfo info : entry.getValue().getInfos())
                log.info("    Tx: [xid=" + info.getXid() +
                    ", label=" + info.getLabel() +
                    ", state=" + info.getState() +
                    ", startTime=" + info.getFormattedStartTime() +
                    ", duration=" + info.getDuration() / 1000 +
                    ", isolation=" + info.getIsolation() +
                    ", concurrency=" + info.getConcurrency() +
                    ", timeout=" + info.getTimeout() +
                    ", size=" + info.getSize() +
                    ", dhtNodes=" + (info.getPrimaryNodes() == null ? "N/A" :
                    F.transform(info.getPrimaryNodes(), new IgniteClosure<UUID, String>() {
                        @Override public String apply(UUID id) {
                            return U.id8(id);
                        }
                    })) +
                    ", nearXid=" + info.getNearXid() +
                    ", parentNodeIds=" + (info.getMasterNodeIds() == null ? "N/A" :
                    F.transform(info.getMasterNodeIds(), new IgniteClosure<UUID, String>() {
                        @Override public String apply(UUID id) {
                            return U.id8(id);
                        }
                    })) +
                    ']');
        }
    }

    /**
     * Checks if all tx futures are finished.
     */
    private void checkFutures() {
        for (Ignite ignite : G.allGrids()) {
            IgniteEx ig = (IgniteEx)ignite;

            final Collection<GridCacheFuture<?>> futs = ig.context().cache().context().mvcc().activeFutures();

            for (GridCacheFuture<?> fut : futs)
                log.info("Waiting for future: " + fut);

            assertTrue("Expecting no active futures: node=" + ig.localNode().id(), futs.isEmpty());
        }
    }
}
