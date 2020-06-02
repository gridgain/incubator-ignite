package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.*;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

import java.util.Collections;
import java.util.List;

/**
 *
 */
public class AddIndexReproducer extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "TEST";

    /** Keys count. */
    private static final int KEY_CNT = 10_000;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);

        configuration.setFailureHandler(new StopNodeOrHaltFailureHandler());
        configuration.setClientFailureDetectionTimeout(30000);
        configuration.setAutoActivationEnabled(false);
        configuration.setPeerClassLoadingEnabled(true);

        configuration.setConsistentId(igniteInstanceName);

        configuration.setClientMode("client".equals(igniteInstanceName));

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
                .setTableName("test")
                .addQueryField("id", Long.class.getName(), null)
                .addQueryField("val", Long.class.getName(), null)
                .setKeyFieldName("id")
                .setValueFieldName("val");

        configuration.setCacheConfiguration(
                new CacheConfiguration<Integer, Integer>()
                        .setName(CACHE_NAME)
                        .setSqlSchema("TEST")
                        .setBackups(1)
                        .setCacheMode(CacheMode.PARTITIONED)
                        .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                        .setQueryEntities(Collections.singleton(qryEntity2))
                        .setAffinity(new RendezvousAffinityFunction(false, 16))
        );

        return configuration;
    }

    /**
     * Reproducer.
     *
     * @throws Exception if failed.
     */
    public void testAddIndex() throws Exception {
        IgniteEx igniteEx = startGrids(4);

        igniteEx.cluster().active(true);

        IgniteEx client = (IgniteEx) startGrid("client");

        awaitPartitionMapExchange();

        IgniteCache<Long, Long> cache = igniteEx.cache(CACHE_NAME);

        for (int i = 0; i < KEY_CNT; i++)
            query(cache,"insert into TEST (id, val) values (?, ?)", i, i);

        Thread thread = new Thread(() -> client.context().query().querySqlFields(new SqlFieldsQuery("CREATE INDEX IDX_TEST ON TEST(VAL)"), true));

        thread.start();

        Thread.sleep(500);

        IgniteProcessProxy.kill("client");
    }

    /** */
    private List<List<?>> query(IgniteCache<Long, Long> cache, String qry, Object... args) {
        return cache.query(new SqlFieldsQuery(qry).setArgs(args)).getAll();
    }
}
