package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

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
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    public void testOnePhaseCommit() throws Exception {
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

                    spi.stopBlock();
                }
            });

            U.awaitQuiet(l);

            tx.commit();
        }

        assertEquals(key, client.cache(DEFAULT_CACHE_NAME).get(key));
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

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        final IgniteEx crd = startGrid(0);

        startGridsMultiThreaded(1, GRID_CNT - 1);

        crd.cluster().active(true);

        awaitPartitionMapExchange();
    }
}
