package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.configuration.WALMode.LOG_ONLY;

/**
 */
public class TxMetadataChangeTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    public static final long MB = 1024 * 1024;

    /** */
    public static final int FIELDS = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setClientMode(igniteInstanceName.startsWith("client"));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setWalMode(LOG_ONLY).setPageSize(1024).
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true).
                setInitialSize(100 * MB).setMaxSize(100 * MB)));

        List<QueryEntity> ql = new ArrayList<>();

        QueryEntity qryEntity = new QueryEntity("java.lang.Integer", "Value");

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        for(int i = 0; i < FIELDS; i++)
            fields.put("f" + i, "java.lang.String");

        qryEntity.setFields(fields);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
                setBackups(2).
                setCacheMode(CacheMode.PARTITIONED).
                setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).
                setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC).
                setAffinity(new RendezvousAffinityFunction(false, 32)).
                setQueryEntities(Collections.singleton(qryEntity))
        );

        return cfg;
    }

    public void testMetadata() throws Exception {
        Ignite crd = startGridsMultiThreaded(3);

        Ignite client1 = startGrid("client1");

        assertTrue(client1.configuration().isClientMode());
        assertNotNull(client1.cache(DEFAULT_CACHE_NAME));

        int range = 100;

        Random r = new Random(0);

        AtomicInteger updates = new AtomicInteger();

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                int cnt = 1;

                while (cnt-- > 0) {
                    int key = r.nextInt(range);

                    IgniteCache<Integer, BinaryObject> cache = client1.cache(DEFAULT_CACHE_NAME).withKeepBinary();

                    try (Transaction tx = client1.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE)) {
                        BinaryObjectBuilder val = client1.binary().builder("Value");

                        int toUpdate = 1 + r.nextInt(FIELDS);

                        for (int j = 0; j < toUpdate; j++)
                            val.setField("f" + r.nextInt(FIELDS), "testVal");

                        cache.put(key, val.build());

                        tx.commit();

                        updates.incrementAndGet();
                    }
                }
            }
        });

//        doSleep(20_000);
//
//        stop.set(true);

        fut.get();

        log.info("Updates: " + updates.get());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
    }
}
