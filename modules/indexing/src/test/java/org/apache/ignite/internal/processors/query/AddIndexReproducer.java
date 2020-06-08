package org.apache.ignite.internal.processors.query;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

/**
 *
 */
public class AddIndexReproducer extends GridCommonAbstractTest {
    /** Client node name. */
    private static final String CLIENT_NODE_NAME = "client";

    /** Table name. */
    private static final String TABLE_NAME = "test";

    /** Column name. */
    private static final String COLUMN_NAME = "val";

    /** Index name. */
    private static final String INDEX_NAME = "IDX_TEST";

    /** Keys count. */
    private static final int KEY_CNT = 10_000;

    /** Logger */
    private final ListeningTestLogger testLogger = new ListeningTestLogger(false, log);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        testLogger.clearListeners();

        BPlusTree.pageHndWrapper = (tree, hnd) -> {
            if (hnd instanceof BPlusTree.Insert) {
                try {
                    Thread.sleep(100);
                }
                catch (Exception ignore) {
                }
            }
            return hnd;
        };
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        testLogger.clearListeners();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);

        configuration.setFailureHandler(new StopNodeOrHaltFailureHandler());
        configuration.setClientFailureDetectionTimeout(10000);
        configuration.setAutoActivationEnabled(false);
        configuration.setPeerClassLoadingEnabled(true);

        configuration.setConsistentId(igniteInstanceName);

        configuration.setClientMode(CLIENT_NODE_NAME.equals(igniteInstanceName));

        configuration.setMetricsLogFrequency(10_000);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setWalMode(WALMode.LOG_ONLY)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(256 * 1024 * 1024)
                    .setMetricsRateTimeInterval(1000)
                    .setPersistenceEnabled(true)
            );

        configuration.setDataStorageConfiguration(dsCfg);

        QueryEntity qryEntity2 = new QueryEntity(Long.class.getName(), Long.class.getName())
            .setTableName(TABLE_NAME)
            .addQueryField("id", Long.class.getName(), null)
            .addQueryField(COLUMN_NAME, Long.class.getName(), null)
            .setKeyFieldName("id")
            .setValueFieldName(COLUMN_NAME);

        configuration.setCacheConfiguration(
            new CacheConfiguration<Integer, Integer>()
                .setName(DEFAULT_CACHE_NAME)
                //TODO
                //.setSqlSchema("TEST")
                .setBackups(1)
                .setCacheMode(CacheMode.PARTITIONED)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setQueryEntities(Collections.singleton(qryEntity2))
                .setAffinity(new RendezvousAffinityFunction(false, 16))
        );

        configuration.setGridLogger(testLogger);

        return configuration;
    }

    /**
     * Reproducer.
     *
     * @throws Exception if failed.
     */
    public void testAddIndex() throws Exception {
        IgniteEx igniteEx = startGrids(2);
        igniteEx.cluster().active(true);

        final IgniteEx client = (IgniteEx)startGrid(CLIENT_NODE_NAME);

        awaitPartitionMapExchange();

        final IgniteCache<Long, Long> cache = client.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < KEY_CNT; i++) {
            cache.put(Long.valueOf(i), Long.valueOf(i));
        }

        //TODO
        int i = 0;
        final Iterator<List<?>> iterator = igniteEx.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("select * from  \"" + DEFAULT_CACHE_NAME + "\"." + TABLE_NAME)).iterator();
        while (iterator.hasNext()) {
            final List<?> next = iterator.next();
            i++;
        }
        assertEquals(KEY_CNT, i);

        final ListeningTestLogger testLogger = new ListeningTestLogger(false, igniteEx.log());

        LogListener startLsnr = LogListener.matches(s -> {
            System.out.println("!!!!! " + s); //TODO
            return s.startsWith("Started indexes rebuilding for cache [name=" + DEFAULT_CACHE_NAME);
        }).times(1).build();
        testLogger.registerListener(startLsnr);

        LogListener finishLsnr = LogListener.matches(s -> s.startsWith("Finished indexes rebuilding for cache [name=" + DEFAULT_CACHE_NAME)).times(1).build();
        testLogger.registerListener(finishLsnr);

        GridTestUtils.runAsync(() -> {
                try {
                    igniteEx.compute(igniteEx.cluster().forClients()).run(new IgniteRunnable() {
                        @IgniteInstanceResource
                        Ignite ignite;

                        @Override public void run() {
                            try {
                                ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE INDEX " + INDEX_NAME + " ON \"" + DEFAULT_CACHE_NAME + "\"." + TABLE_NAME + "(" + COLUMN_NAME + ")"));
                            }
                            catch (Exception e) {
                                System.out.println("!!!!!");//TODO
                                e.printStackTrace();
                            }
                        }
                    });
                }
                catch (Exception e) {
                    System.out.println("!!!!!");//TODO
                    e.printStackTrace();
                }
            }
        );

        //TODO
//        System.out.println("!!!!! 33"+igniteEx.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("select INDEX_NAME from INFORMATION_SCHEMA.INDEXES ")).getAll());

        //TODO
//        assertTrue(GridTestUtils.waitForCondition(() -> {
//            String selectIdxSql = "select * from INFORMATION_SCHEMA.INDEXES ";
//            String selectIdxSql = "select * from INFORMATION_SCHEMA.INDEXES where index_name='" + INDEX_NAME + "'";
//        String selectIdxSql = "select * from ignite.indexes where index_name='" + INDEX_NAME + "'";
//            List<List<?>> all = igniteEx.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(selectIdxSql)).getAll();
//            return all.size() > 0;
//            return all.size() > 3;
//        }, 180_000));

        //TODO
//        System.out.println("!!!!! 44"+igniteEx.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("select INDEX_NAME from INFORMATION_SCHEMA.INDEXES ")).getAll());

        //TODO
//        CacheGroupMetricsMXBean mxBean0Grp1 = mxBean(0, GROUP_NAME);
//
//        assertTrue("Timeout wait start rebuild index",
//            GridTestUtils.waitForCondition(() -> mxBean0Grp1.getIndexBuildCountPartitionsLeft() > 0, 30_000)
//        );
        //TODO
        Thread.sleep(500);
        //TODO
//        assertTrue(GridTestUtils.waitForCondition(startLsnr::check, 60_000));

        IgniteProcessProxy.kill(CLIENT_NODE_NAME);

        //TODO
//        assertTrue(GridTestUtils.waitForCondition(finishLsnr::check, 30_000));

        assertTrue(GridTestUtils.waitForCondition(() -> {
            String selectIdxSql = "select index_name from INFORMATION_SCHEMA.INDEXES where index_name='" + INDEX_NAME + "'";
//        String selectIdxSql = "select index_name from ignite.indexes where index_name='" + INDEX_NAME + "'";
            List<List<?>> all = igniteEx.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(selectIdxSql)).getAll();
            return all.size() > 0;
        }, 30_000));
    }

    /**
     * Scenario:
     * 1. Launch some nodes
     * 2. Initiate indexes rebuild
     * 3. Stop cluster BEFORE indexes rebuild is finished
     * 4. Start cluster again
     * 5. You may or may not get error "Maximum number of retries 1000 reached for Put operation" on some nodes.
     * 6. Make this error appear somehow.
     */
    public void testAddIndexV1() throws Exception {
        final Random rand = new Random();
        IgniteEx igniteEx = startGrids(2);

        final IgniteEx client = (IgniteEx)startGrid(CLIENT_NODE_NAME);

        igniteEx.cluster().active(true);

        IgniteDataStreamer<Long, Long> streamer = igniteEx.dataStreamer(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 10_000; i++)
            streamer.addData(rand.nextLong(), rand.nextLong());

        U.sleep(5000);

        //GridTestUtils.waitForCondition(() -> streamer.future().isDone(), 60_0000);

        System.out.println("debug: cache populated");

        GridTestUtils.runAsync(() -> {
                try {
                    client.compute().run(new IgniteRunnable() {
                        @IgniteInstanceResource
                        Ignite ignite;

                        @Override public void run() {
                            try {
                                ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE INDEX " + INDEX_NAME + " ON \"" + DEFAULT_CACHE_NAME + "\"." + TABLE_NAME + "(" + COLUMN_NAME + ")"));
                                ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE INDEX " + INDEX_NAME + "1 ON \"" + DEFAULT_CACHE_NAME + "\"." + TABLE_NAME + "(" + COLUMN_NAME + ")"));
                                ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE INDEX " + INDEX_NAME + "2 ON \"" + DEFAULT_CACHE_NAME + "\"." + TABLE_NAME + "(" + COLUMN_NAME + ")"));
                                ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE INDEX " + INDEX_NAME + "3 ON \"" + DEFAULT_CACHE_NAME + "\"." + TABLE_NAME + "(" + COLUMN_NAME + ")"));
                            }
                            catch (Exception e) {
                                System.out.println("!!!!!");//TODO
                                e.printStackTrace();
                            }
                        }
                    });
                }
                catch (Exception e) {
                    System.out.println("!!!!!");//TODO
                    e.printStackTrace();
                }
            }
        );

        //TODO achieve the task launch and its interruption

        System.out.println("debug: going to stop");

        stopAllGrids(true);

        U.sleep(500);

        System.out.println("debug: going to restart");

        IgniteEx igniteEx1 = startGrids(2);

        igniteEx1.cluster().active(true);

        IgniteEx cl = (IgniteEx)startGrid(CLIENT_NODE_NAME);

        awaitPartitionMapExchange();

        cl.compute().run(new IgniteRunnable() {
            @IgniteInstanceResource
            Ignite ignite;

            @Override public void run() {
                try {
                    ignite.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE INDEX " + INDEX_NAME + " ON \"" + DEFAULT_CACHE_NAME + "\"." + TABLE_NAME + "(" + COLUMN_NAME + ")"));
                }
                catch (Exception e) {
                    System.out.println("????");
                    e.printStackTrace();
                }
            }
        });
    }

    public void testAddIndexV2() throws Exception {
        final Random rand = new Random();
        IgniteEx igniteEx = startGrids(2);
        igniteEx.cluster().active(true);

        final IgniteEx client = (IgniteEx)startGrid(CLIENT_NODE_NAME);

        final IgniteCache<Long, Long> cache = client.cache(DEFAULT_CACHE_NAME);

        System.out.println("!!!!! 1 "+System.currentTimeMillis());

        for (int i = 0; i < 10_000; i++) {
            cache.put(Long.valueOf(i), rand.nextLong());
        }

        System.out.println("!!!!! 2 "+System.currentTimeMillis());

        final LogListener startLsnr = LogListener.matches(s -> s.startsWith("Started indexes rebuilding for cache [name=" + DEFAULT_CACHE_NAME)).times(1).build();
        testLogger.registerListener(startLsnr);

        final LogListener finishLsnr = LogListener.matches(s -> s.startsWith("Finished indexes rebuilding for cache [name=" + DEFAULT_CACHE_NAME)).times(1).build();
        testLogger.registerListener(finishLsnr);

        final LogListener pmeLsnr = LogListener.matches(s -> s.startsWith("Completed partition exchange")).times(1).build();
        testLogger.registerListener(pmeLsnr);

        final LogListener ioLsnr = LogListener.matches(s -> s.startsWith("Started local index operation")).times(1).build();
        testLogger.registerListener(ioLsnr);

        GridTestUtils.runAsync(() -> {
                try {
                    System.out.println("!!!!! 3 "+System.currentTimeMillis());
                    client.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE INDEX " + INDEX_NAME + " ON \"" + DEFAULT_CACHE_NAME + "\"." + TABLE_NAME + "(" + COLUMN_NAME + ")"));
                    System.out.println("!!!!! 4 "+System.currentTimeMillis());
                }
                catch (Exception e) {
                    System.out.println("!!!!!");//TODO
                    e.printStackTrace();
                }
            }
        );

        System.out.println("!!!!! 5 "+System.currentTimeMillis());

        //TODO achieve the task launch and its interruption
//        assertTrue(GridTestUtils.waitForCondition(ioLsnr::check, 60_000));

        System.out.println("!!!!! 6 "+System.currentTimeMillis());

        System.out.println("debug: going to stop");

        stopAllGrids(true);

        System.out.println("debug: going to restart");

        IgniteEx igniteEx1 = startGrids(2);

        igniteEx1.cluster().active(true);

        assertTrue(GridTestUtils.waitForCondition(finishLsnr::check, 60_000));
    }
}
