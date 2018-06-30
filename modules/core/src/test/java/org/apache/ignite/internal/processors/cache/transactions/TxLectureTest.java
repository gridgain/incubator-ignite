package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * <p> The <code>TxLectureTest</code> </p>
 *
 * @author Alexei Scherbakov
 */
public class TxLectureTest extends GridCommonAbstractTest {
    /** */
    public static final int DURATION = 60_000;

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 3;

    /** */
    public static final long MB = 1024 * 1024;

    /** */
    private boolean persistenceEnabled;

    private int backups = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        boolean client = igniteInstanceName.startsWith("client");

        cfg.setClientMode(client);

        if (persistenceEnabled)
            cfg.setDataStorageConfiguration(new DataStorageConfiguration().setWalMode(LOG_ONLY).setPageSize(1024).
                setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true).
                    setInitialSize(100 * MB).setMaxSize(100 * MB)));

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(backups);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    // Test useless.
    public void testOnePhaseCommit() throws Exception {
        backups = 1;

        startGridsMultiThreaded(GRID_CNT);

        Ignite client = startGrid("client");

        IgniteEx prim = grid(0);

        Integer key = primaryKey(prim.cache(DEFAULT_CACHE_NAME));

        IgniteEx backup = (IgniteEx)backupNode(key, DEFAULT_CACHE_NAME);

        // Start pessimistic tx.
        try (Transaction tx = client.transactions().txStart()) {
            log.info("Near tx: " + tx);

            // Acquire write exclusive lock.
            client.cache(DEFAULT_CACHE_NAME).put(key, key);

            IgniteInternalTx locTx = F.first(txs(prim));

            log.info("Primary tx: " + locTx);

            IgniteInternalTx backupTx = F.first(txs(backup));

            log.info("Backup tx: " + backupTx); // Null, backup transactions are created on prepare.

            CountDownLatch l = new CountDownLatch(1);

            runAsync(new Runnable() {
                @Override public void run() {
                    TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(prim);

                    // Prevent backup commit.
                    spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                        @Override public boolean apply(ClusterNode node, Message msg) {
                            if (msg instanceof GridDhtTxPrepareRequest) {
                                GridDhtTxPrepareRequest req = (GridDhtTxPrepareRequest)msg;

                                assertTrue(req.onePhaseCommit());
                            }

                            return false;
                        }
                    });

                    l.countDown(); // Makes sure message blocked before trying to commit.

                    try {
                        spi.waitForBlocked();
                    }
                    catch (InterruptedException e) {
                        log.error("Interrupted", e);
                    }

                    IgniteInternalTx backupTx = F.first(txs(backup));

                    log.info("Backup tx: " + backupTx);

                    spi.stopBlock();
                }
            });

            U.awaitQuiet(l);

            tx.commit();
        }

        assertEquals(key, client.cache(DEFAULT_CACHE_NAME).get(key));
    }

    public void testBackupRace() throws Exception {
        backups = 2;

        startGridsMultiThreaded(GRID_CNT);

        Ignite client = startGrid("client");

        IgniteEx prim = grid(0);

        Integer key = primaryKey(prim.cache(DEFAULT_CACHE_NAME));

        List<Ignite> backups = backupNodes(key, DEFAULT_CACHE_NAME);

        // Start pessimistic tx.
        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 3000, 1)) {
            log.info("Near tx: " + tx);

            // Acquire write exclusive lock.
            client.cache(DEFAULT_CACHE_NAME).put(key, key);

            CountDownLatch l = new CountDownLatch(1);

            runAsync(new Runnable() {
                @Override public void run() {
                    TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(prim);

                    // Prevent one backup commit.
                    spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                        @Override public boolean apply(ClusterNode node, Message msg) {
                            if (node.id().equals(backups.get(0).cluster().localNode().id()) && (
                                msg instanceof GridDhtTxPrepareRequest || msg instanceof GridDhtTxFinishRequest))
                                return true;

                            return false;
                        }
                    });

                    try {
                        ((IgniteEx)backups.get(1)).context().cache().context().tm().remoteTxFinishFuture(((TransactionProxyImpl)tx).tx().nearXidVersion()).get();
                    }
                    catch (IgniteCheckedException e) {
                        e.printStackTrace();
                    }

                    l.countDown(); // Makes sure message blocked before trying to commit.

                    try {
                        spi.waitForBlocked(2);
                    }
                    catch (InterruptedException e) {
                        log.error("Interrupted", e);
                    }

                    spi.stopBlock();
                }
            });

            U.awaitQuiet(l);

            tx.commit();
        }
        catch (Exception e) {
            System.out.println(e);
        }

        checkFutures();
    }

    public void testBackupRaceOPC() throws Exception {
        backups = 1;

        startGridsMultiThreaded(GRID_CNT);

        Ignite client = startGrid("client");

        IgniteEx prim = grid(0);

        List<Integer> keys = primaryKeys(prim.cache(DEFAULT_CACHE_NAME), 2);

        Ignite backup = backupNode(keys.get(0), DEFAULT_CACHE_NAME);

        CountDownLatch l2 = new CountDownLatch(1);

        // Start pessimistic tx.
        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 3000, 1)) {
            log.info("Near tx: " + tx);

            Map<Integer, Integer> map = new HashMap<>();
            map.put(keys.get(0), keys.get(0));
            map.put(keys.get(1), keys.get(1));

            TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(prim);

            // Prevent one backup commit.
            spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (msg instanceof GridDhtTxPrepareRequest || msg instanceof GridDhtTxFinishRequest)
                        return true;

                    return false;
                }
            });

            // Acquire write exclusive lock.
            client.cache(DEFAULT_CACHE_NAME).putAll(map);

            CountDownLatch l = new CountDownLatch(1);

            runAsync(new Runnable() {
                @Override public void run() {
                    l.countDown(); // Makes sure message blocked before trying to commit.

                    try {
                        spi.waitForBlocked();
                    }
                    catch (InterruptedException e) {
                        log.error("Interrupted", e);
                    }

                    //Latches.lock = true;

                    U.awaitQuiet(l2);

                    spi.stopBlock();
                }
            });

            U.awaitQuiet(l);

            tx.commit();
        }
        catch (Exception e) {
            System.out.println(e);
        }

        l2.countDown();

        doSleep(3000);

        int c1 = 0;
        Iterable<Cache.Entry<Object, Object>> v1 = prim.cache(DEFAULT_CACHE_NAME).localEntries(CachePeekMode.ALL);
        for (Cache.Entry<Object, Object> entry : v1)
            c1++;

        int c2 = 0;
        Iterable<Cache.Entry<Object, Object>> v2 = backup.cache(DEFAULT_CACHE_NAME).localEntries(CachePeekMode.ALL);
        for (Cache.Entry<Object, Object> entry : v2)
            c2++;

        assertEquals(c1, c2);

        checkFutures();
    }

    public void testPrimaryRace() throws Exception {
        backups = 2;

        startGridsMultiThreaded(GRID_CNT);

        Ignite client = startGrid("client");

        int k1 = 0;

        Ignite prim1 = primaryNode(k1, DEFAULT_CACHE_NAME);

        // Delay prepare to second primary.
        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(client);

        spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message message) {
                if (node.id().equals(prim1.cluster().localNode().id()) &&
                    (message instanceof GridNearLockRequest || message instanceof GridNearTxFinishRequest))
                    return true;

                return false;
            }
        });

        // Start pessimistic tx.
        TransactionProxyImpl ref = null;

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 500, 1)) {
            ref = (TransactionProxyImpl)tx;

            log.info("Near tx: " + tx);

            runAsync(new Runnable() {
                @Override public void run() {
                    try {
                        spi.waitForBlocked(2);
                    }
                    catch (InterruptedException e) {
                        fail();
                    }

                    spi.stopBlock(true, new IgnitePredicate<T2<ClusterNode, GridIoMessage>>() {
                        @Override public boolean apply(T2<ClusterNode, GridIoMessage> objects) {
                            return objects.get2().message() instanceof GridNearTxFinishRequest;
                        }
                    });

                    doSleep(500);

                    spi.stopBlock();
                }
            });

            client.cache(DEFAULT_CACHE_NAME).put(k1, k1);
        }
        catch (Exception e) {
            System.out.println(e);
        }

        assertNotNull(ref);

//        IgniteInternalFuture<?> fut = U.field(ref.tx(), "finishFut");
//
//        fut.get();

        doSleep(5000);

        checkFutures();
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

            Collection<IgniteInternalTx> txs = ig.context().cache().context().tm().activeTransactions();

            for (IgniteInternalTx tx : txs)
                log.info("Waiting for tx: " + tx);

            assertTrue("Expecting no active transactions: node=" + ig.localNode().id(), txs.isEmpty());
        }
    }

    private Iterable<IgniteInternalTx> txs(IgniteEx prim) {
        return txs(prim, null);
    }

    private Collection<IgniteInternalTx> txs(IgniteEx prim, @Nullable IgnitePredicate<IgniteInternalTx> pred) {
        return F.view(prim.context().cache().context().tm().activeTransactions(), pred == null ? F.alwaysTrue() : pred);
    }

    public void testLockChaining() throws Exception {
        Ignite client = startGrid("client");

        List<Integer> keys = primaryKeys(grid(0).cache(DEFAULT_CACHE_NAME), 10_000);

        TreeMap<Integer, Integer> map = new TreeMap<>(keys.stream().collect(Collectors.toMap(t -> t, t -> t)));

        IgniteInternalFuture fut1 = runAsync(new Runnable() {
            @Override public void run() {
                try (Transaction tx = client.transactions().txStart()) {
                    client.cache(DEFAULT_CACHE_NAME).putAll(map);

                    tx.commit();
                }
            }
        });

        IgniteInternalFuture fut2 = runAsync(new Runnable() {
            @Override public void run() {
                try (Transaction tx = client.transactions().txStart()) {
                    client.cache(DEFAULT_CACHE_NAME).putAll(map.descendingMap());

                    tx.commit();
                }
            }
        });

        fut1.get();
        fut2.get();
    }

    public void testRecoveryOnPrimaryLeft() throws Exception {
        backups = 2;

        startGridsMultiThreaded(GRID_CNT);

        Ignite client = startGrid("client");

        IgniteEx prim = grid(0);

        Integer key = primaryKey(prim.cache(DEFAULT_CACHE_NAME));

        IgniteEx backup = (IgniteEx)backupNode(key, DEFAULT_CACHE_NAME);

        Ignite other = null;

        for (Ignite ignite : G.allGrids()) {
            if (ignite != prim && ignite != backup) {
                other = ignite;

                break;
            }
        }

        Integer key2 = primaryKey(backup.cache(DEFAULT_CACHE_NAME));

        assertFalse(Objects.equals(key, key2));

        // Start pessimistic tx.
        try (Transaction tx = client.transactions().txStart()) {
            log.info("Near tx: " + tx);

            // Acquire write exclusive lock.
            client.cache(DEFAULT_CACHE_NAME).put(key, key);
            client.cache(DEFAULT_CACHE_NAME).put(key2, key2);

            IgniteInternalTx locTx = F.first(txs(prim));

            log.info("Primary tx: " + locTx);

            IgniteInternalTx backupTx = F.first(txs(backup));

            log.info("Backup tx: " + backupTx); // Null, backup transactions are created on prepare.

            CountDownLatch l = new CountDownLatch(1);

            runAsync(new Runnable() {
                @Override public void run() {
                    TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(prim);

                    // Prevent backup commit.
                    spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                        @Override public boolean apply(ClusterNode node, Message msg) {
                            return node.equals(backup.localNode()) && msg instanceof GridDhtTxFinishRequest;
                        }
                    });

                    l.countDown(); // Makes sure message blocked before trying to commit.

                    try {
                        spi.waitForBlocked();
                    }
                    catch (InterruptedException e) {
                        log.error("Interrupted", e);
                    }

                    IgniteInternalTx backupTx = F.first(txs(backup));

                    log.info("Backup tx: " + backupTx);

                    assertTrue(backupTx.state() == TransactionState.PREPARED);

                    prim.close();
                }
            });

            U.awaitQuiet(l);

            tx.commit();
        }

        assertEquals(key, client.cache(DEFAULT_CACHE_NAME).get(key));
    }
}
