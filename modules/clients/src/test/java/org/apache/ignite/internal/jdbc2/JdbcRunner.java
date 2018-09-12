package org.apache.ignite.internal.jdbc2;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class JdbcRunner {
    /** IP finder. */
    private static TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Number of account IDs. */
    private static final int ACCT_ID_CNT = 5000;

    /** Number of records per account ID. */
    private static final int RECORDS_PER_ACCT_ID = 500;

    /** Account ID counter. */
    private static final AtomicLong ACCT_ID_CTR = new AtomicLong();

    /** Generated account IDs. */
    private static final ArrayList<String> ACCT_IDS = new ArrayList<>(ACCT_ID_CNT);

    /** Path to work directory. */
    private static final String WORK_DIR_PATH = "C:\\Personal\\code\\incubator-ignite\\work";

    /** Number of load threads. */
    private static final int LOAD_THREADS = 64;

    /** Load duration. */
    private static final long LOAD_DUR = 120_000L;

    /** Warmup duration. */
    private static final long LOAD_WARMUP_DUR = 10_000L;

    /** Number of load counters. */
    private static final int LOAD_CTR_CNT = 50;

    /** Load counters. */
    private static volatile LongAdder[] loadCtrs;

    /** Warmup flag. */
    private static volatile boolean warmup = true;

    /** Stop flag. */
    private static volatile boolean stop;

    static {
        loadCtrs = new LongAdder[LOAD_CTR_CNT];

        for (int i = 0; i < LOAD_CTR_CNT; i++)
            loadCtrs[i] = new LongAdder();
    }

    /**
     * Entry point.
     */
    @SuppressWarnings("unused")
    public static void main(String[] args) throws Exception {
        U.delete(new File(WORK_DIR_PATH));

        try (Ignite srv1 = Ignition.start(config("srv1", false))) {
            try (Ignite srv2 = Ignition.start(config("srv2", false))) {
                srv1.cluster().active(true);

                try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
                    executeUpdate(conn, "create table TRAN_HISTORY\n" +
                        "(TRAN_ID  varchar(50) PRIMARY KEY,\n" +
                        "ACCT_ID varchar(16) null,\n" +
                        "ACCT_CURR varchar(3) null,\n" +
                        "TRAN_CURR varchar(3) null,\n" +
                        "LEDGER_BAL decimal(20, 4) null,\n" +
                        "AVAIL_BAL decimal(20, 4) null,\n" +
                        "VALUE_DATE date null,\n" +
                        "POSTING_DATE date null,\n" +
                        "TRAN_DATE date not null,\n" +
                        "PART_TRAN_SERL_NUM varchar(4) not null,\n" +
                        "TRAN_PARTICLR_CODE varchar(5) null,\n" +
                        "TRAN_PARTICLRS varchar(50) null,\n" +
                        "TRAN_PARTICLRS2 varchar(50) null,\n" +
                        "TRAN_AMOUNT decimal(20, 4) null,\n" +
                        "TXN_TYPE varchar(1) null,\n" +
                        "PART_TXN_TYPE varchar(1) null,\n" +
                        "TRAN_STATUS varchar(1) null,\n" +
                        "TRAN_REF_NUM varchar(32) null,\n" +
                        "TRAN_REMARKS varchar(50) null,\n" +
                        "CHANNEL_ID varchar(10) null,\n" +
                        "LST_TRAN_DATE date null,\n" +
                        " SCHEME_CODE varchar(5) null,\n" +
                        "REF_CURR_CODE varchar(3) null,\n" +
                        "EXCHANGE_RATE_CODE varchar(5) null,\n" +
                        " EXCHANGE_RATE decimal(21, 10) null,\n" +
                        "DEAL_REF_NUM varchar(16) null,\n" +
                        "LST_UPDT_SYS_ID varchar(255) null,\n" +
                        "LST_UPDT_DTTM date null)");

                    executeUpdate(conn, "CREATE INDEX accid_index ON TRAN_HISTORY(ACCT_ID)");

                    generateData(conn);
                }

                runLoad(LOAD_THREADS, LOAD_WARMUP_DUR, LOAD_DUR,
                    "jdbc:ignite:thin://127.0.0.1:10800,127.0.0.1:10801"
                );
            }
        }
    }

    /**
     * Run load.
     *
     * @param threads Threads.
     * @param warmupDur Warmup duration.
     * @param dur Duration.
     * @param connStrs Connection strings.
     * @throws Exception If failed.
     */
    private static void runLoad(int threads, long warmupDur, long dur, String... connStrs) throws Exception {
        ThreadLocalRunner[] runners = new ThreadLocalRunner[threads];

        for (int i = 0; i < threads; i++) {
            ThreadLocalRunner runner = new ThreadLocalRunner(connStrs);

            Thread thread = new Thread(runner);

            thread.setName("load-runner-" + i);

            thread.start();

            runners[i] = runner;
        }

        Thread.sleep(warmupDur);

        warmup = false;

        System.out.println(">>> WARMUP finished.");

        Thread printThread = new Thread(new Runnable() {
            @Override public void run() {
                while (!stop) {
                    try {
                        Thread.sleep(5000L);

                        printResults();

                        LongAdder[] newCtrs = new LongAdder[LOAD_CTR_CNT];

                        for (int i = 0; i < LOAD_CTR_CNT; i++)
                            newCtrs[i] = new LongAdder();

                        loadCtrs = newCtrs;
                    }
                    catch (Exception e) {
                        return;
                    }
                }
            }
        });

        printThread.setName("load-result-printer");
        printThread.start();

        Thread.sleep(dur);

        stop = true;

        for (ThreadLocalRunner runner : runners)
            runner.stop();

        for (ThreadLocalRunner runner : runners)
            runner.awaitStop();

        System.out.println(">>> LOAD finished.");
    }

    /**
     * Print current results.
     */
    @SuppressWarnings("StringConcatenationInsideStringBufferAppend")
    private static void printResults() {
        StringBuilder sb = new StringBuilder(">>> RESULTS [");

        boolean first = true;

        LongAdder[] ctrs = loadCtrs;

        for (int i = 0; i < LOAD_CTR_CNT; i++) {
            long val = ctrs[i].longValue();

            if (val != 0L) {
                if (first)
                    first = false;
                else
                    sb.append(", ");

                sb.append(((i + 1) * 100) + "=" + val);
            }
        }

        sb.append("]");

        System.out.println(sb.toString());
    }

    /**
     * Log query duration.
     *
     * @param dur Duration.
     */
    private static void logQueryDuration(long dur) {
        if (warmup)
            return;

        if (dur < 0) {
            loadCtrs[0].increment();

            return;
        }

        int dur0 = (int)dur;

        if ((long)dur0 == dur) {
            int slot = dur0 / 100;

            if (slot < LOAD_CTR_CNT)
                loadCtrs[slot].increment();

            return;
        }

        loadCtrs[LOAD_CTR_CNT - 1].increment();
    }

    /**
     * Query Ignite through the given connection.
     *
     * @param conn Connection.
     * @return Duration in milliseconds.
     * @throws Exception if failed.
     */
    @SuppressWarnings("StatementWithEmptyBody")
    private static long query(Connection conn) throws Exception {
        long start = System.currentTimeMillis();

        String acctId = ACCT_IDS.get(ThreadLocalRandom.current().nextInt(ACCT_ID_CNT));

        try (PreparedStatement stmt = conn.prepareStatement("SELECT * FROM TRAN_HISTORY WHERE ACCT_ID='" + acctId + "' ORDER BY POSTING_DATE LIMIT 50 OFFSET 0")) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    // No-op.
                }
            }
        }

        return System.currentTimeMillis() - start;
    }

    /**
     * Create Ignite configuration.
     *
     * @param name Node name.
     * @param cli Client flag.
     * @return Configuration.
     */
    private static IgniteConfiguration config(String name, boolean cli) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setIgniteInstanceName(name);

        if (cli)
            cfg.setClientMode(true);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(2L * 1024 * 1024 * 1024))
        );

        cfg.setLongQueryWarningTimeout(Long.MAX_VALUE);

        return cfg;
    }

    /**
     * Execute single update statment.
     *
     * @param conn Connection.
     * @param sql SQL statement.
     * @throws Exception If failed.
     */
    private static void executeUpdate(Connection conn, String sql) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    /**
     * Generate data.
     *
     * @param conn Connection.
     * @throws Exception If failed.
     */
    private static void generateData(Connection conn) throws Exception {
        long start = System.currentTimeMillis();

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("SET STREAMING 1");

            insertAll(stmt);

            stmt.executeUpdate("SET STREAMING 0");
        }
        finally {
            long dur = System.currentTimeMillis() - start;

            System.out.println(">>> GENERATED data in " + dur + " ms");
        }
    }

    /**
     * Insert all records.
     */
    private static void insertAll(Statement stmt) throws Exception {
        int ctr = 0;

        for (int i = 0; i < ACCT_ID_CNT; i++) {
            String acctId = generateAccountId();

            ACCT_IDS.add(acctId);
        }

        for (int j = 0; j < RECORDS_PER_ACCT_ID; j++) {
            for (String acctId : ACCT_IDS) {
                insertAccountId(stmt, acctId);

                ctr++;

                if (ctr % 50_000 == 0)
                    System.out.println(">>> INSERTED: " + ctr);
            }
        }
    }

    /**
     * Insert single record for the given account ID.
     *
     * @param stmt Statement.
     * @param acctId Account ID.
     * @throws Exception If failed.
     */
    private static void insertAccountId(Statement stmt, String acctId) throws Exception {
        String id = UUID.randomUUID().toString();

        stmt.executeUpdate("INSERT INTO TRAN_HISTORY VALUES ('" + id + "','" + acctId + "','TWD','TWD',0.0000,906.0000,'2018-04-19', '2018-04-19','2018-04-19','25','CHQ03','982668','',26500.0000, 'L','D','','50000000','Zone Serial [ 25]','','2018-09-09','RQODA','TWD','', 1.0000000000,'','','2018-09-09')");
    }

    /**
     * @return Generated account ID.
     */
    private static String generateAccountId() {
        return String.format("%016d", ACCT_ID_CTR.incrementAndGet());
    }

    /**
     * Thread-local runner.
     */
    private static class ThreadLocalRunner implements Runnable {
        /** Connection strings. */
        private final String[] connStrs;

        /** Connections. */
        private ArrayList<Connection> conns;

        /** Stop latch. */
        private final CountDownLatch stopLatch = new CountDownLatch(1);

        /** Stop flag. */
        private volatile boolean stopped;

        /**
         * Constructor.
         *
         * @param connStrs Connection strings.
         */
        private ThreadLocalRunner(String... connStrs) {
            this.connStrs = connStrs;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                conns = new ArrayList<>(connStrs.length);

                for (String connStr : connStrs) {
                    Connection conn = DriverManager.getConnection(connStr);

                    conns.add(conn);
                }

                while (!stopped)
                    runSingle();
            }
            catch (Exception e) {
                throw new RuntimeException("Thread local runner failed.", e);
            }
            finally {
                try{
                    for (Connection conn : conns)
                        U.closeQuiet(conn);
                }
                finally {
                    stopLatch.countDown();
                }
            }
        }

        /**
         * Run single iteration.
         *
         * @throws Exception If failed.
         */
        private void runSingle() throws Exception {
            Connection conn = conns.get(ThreadLocalRandom.current().nextInt(conns.size()));

            long dur = query(conn);

            logQueryDuration(dur);
        }

        /**
         * Stop runner.
         */
        private void stop() {
            stopped = true;
        }

        /**
         * Await runner stop.
         */
        private void awaitStop() {
            try {
                stopLatch.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new RuntimeException("Interrupted while waiting on thread local worker stop.", e);
            }
        }
    }
}
