package org.apache.ignite.internal.processors.cache.distributed;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Created by A.Scherbakov on 2/26/2019.
 */
public class MixedLoadTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_ATOMIC_PARTITIONED = "cache10";

    /** */
    public static final String CACHE_ATOMIC_REPLICATED = "cache11";

    /** */
    public static final String CACHE_TX_PARTITIONED = "cache20";

    /** */
    public static final String CACHE_TX_REPLICATED = "cache21";

    /** */
    public static final int KEYS_CNT = 10_000;

    public static final String[] CACHES = new String[] {
        CACHE_ATOMIC_PARTITIONED,
        CACHE_ATOMIC_REPLICATED,
        CACHE_TX_PARTITIONED,
        CACHE_TX_REPLICATED
    };

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(
            cacheConfiguration(CACHE_ATOMIC_PARTITIONED, PARTITIONED),
            cacheConfiguration(CACHE_ATOMIC_REPLICATED, REPLICATED),
            cacheConfiguration(CACHE_TX_PARTITIONED, PARTITIONED),
            cacheConfiguration(CACHE_TX_REPLICATED, REPLICATED));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setPageSize(1024).
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().
                setInitialSize(1024 * 1024 * 1024).
                setMaxSize(1024 * 1024 * 1024)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @param name Name.
     * @param cacheMode Cache mode.
     */
    private CacheConfiguration<Integer, TestValue> cacheConfiguration(String name, CacheMode cacheMode) {
        return new CacheConfiguration<Integer, TestValue>(CACHE_ATOMIC_PARTITIONED).
            setName(name).
            setCacheMode(cacheMode).
            setWriteSynchronizationMode(FULL_SYNC).
            setIndexedTypes(Integer.class, TestValue.class).
            setBackups(2);
    }

    /**
     *
     */
    public void testMixedLoad() throws Exception {
        LongAdder opCnt = new LongAdder();

        final int preload = 200;

        AtomicInteger idx = new AtomicInteger();

        IgniteInternalFuture<?> preloadFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int idx0 = idx.getAndIncrement();

                try(IgniteDataStreamer<Integer, Object> streamer = grid(0).dataStreamer(CACHES[idx0])) {
                    for (int i = 0; i < preload; i++)
                        streamer.addData(i, new TestValue(i, "test" + i, "test" + i + "@test.ru", BigDecimal.valueOf((i + 1) * 1_000)));
                }
            }
        }, CACHES.length, "loader");

        preloadFut.get();

        grid(0).context().io().dumpProcessedMessagesStats();
        grid(1).context().io().dumpProcessedMessagesStats();
        grid(2).context().io().dumpProcessedMessagesStats();
    }

    private static class TestValue {
        /** */
        @QuerySqlField(index = true)
        private int id;

        @QuerySqlField(index = true)
        private String login;

        @QuerySqlField(index = true)
        private String email;

        @QuerySqlField(index = true)
        private BigDecimal salary;

        public TestValue(int id, String login, String email, BigDecimal salary) {
            this.id = id;
            this.login = login;
            this.email = email;
            this.salary = salary;
        }

        /**
         * @return Id.
         */
        public int id() {
            return id;
        }

        /**
         * @param id New id.
         */
        public void id(int id) {
            this.id = id;
        }

        /**
         * @return Login.
         */
        public String login() {
            return login;
        }

        /**
         * @param login New login.
         */
        public void login(String login) {
            this.login = login;
        }

        /**
         * @return Email.
         */
        public String email() {
            return email;
        }

        /**
         * @param email New email.
         */
        public void email(String email) {
            this.email = email;
        }

        /**
         * @return Salary.
         */
        public BigDecimal salary() {
            return salary;
        }

        /**
         * @param salary New salary.
         */
        public void salary(BigDecimal salary) {
            this.salary = salary;
        }
    }
}
