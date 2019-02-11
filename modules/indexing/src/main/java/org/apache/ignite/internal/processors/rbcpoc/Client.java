package org.apache.ignite.internal.processors.rbcpoc;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.util.typedef.G;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Client {
    private static final int NUM_OF_ITERATIONS = 20;

    public static void main(String... args) {
        Server.main();
        Loader.main();

        Ignition.setClientMode(true);
        Ignite ignite = Ignition.start(Server.getConfiguration().setIgniteInstanceName("client"));

        H2TreeIndex.USE_HASH = false;

        System.out.println(">>>> Benchmarking original query ...");
        benchmark(() -> {
            String sql = "SELECT  COUNT(*)\n" +
                "FROM    RISK R, TRADE T, BATCH B\n" +
                "WHERE    R.BATCHKEY = B.BATCHKEY\n" +
                "AND R.TRADEIDENTIFIER = T.TRADEIDENTIFIER\n" +
                "AND    R.TRADEVERSION = T.TRADEVERSION\n" +
                "AND    T.BOOK = 'RBCEUR0'\n" +
                "AND B.ISLATEST = TRUE;";

            runSql(ignite, sql, false);
        });

        H2TreeIndex.USE_HASH = true;

        System.out.println(">>>> Benchmarking original query (hash)...");
        benchmark(() -> {
            String sql = "SELECT  COUNT(*)\n" +
                "FROM    RISK R, TRADE T, BATCH B\n" +
                "WHERE    R.BATCHKEY = B.BATCHKEY\n" +
                "AND R.TRADEIDENTIFIER = T.TRADEIDENTIFIER\n" +
                "AND    R.TRADEVERSION = T.TRADEVERSION\n" +
                "AND    T.BOOK = 'RBCEUR0'\n" +
                "AND B.ISLATEST = TRUE;";

            runSql(ignite, sql, false);
        });

        // TODO: No batch, ignore
        System.out.println(">>>> Benchmarking query without BATCH ...");
        benchmark(() -> {
            String sql = "SELECT    COUNT(*)\n" +
                "FROM    RISK R, TRADE T\n" +
                "WHERE    R.TRADEIDENTIFIER = T.TRADEIDENTIFIER\n" +
                "AND    R.TRADEVERSION = T.TRADEVERSION\n" +
                "AND    T.BOOK = 'RBCEUR0';";
            runSql(ignite, sql, false);
        });

        // TODO: Bad, ignore
//        System.out.println(">>>> Benchmarking R -> T -> B ...");
//        benchmark(() -> {
//            String sql =
//                "SELECT  COUNT(*)\n" +
//                "FROM           RISK R " +
//                "    INNER JOIN TRADE T ON (R.TRADEIDENTIFIER = T.TRADEIDENTIFIER AND R.TRADEVERSION = T.TRADEVERSION) " +
//                "    INNER JOIN BATCH B ON (R.BATCHKEY = B.BATCHKEY) " +
//                "WHERE" +
//                "    T.BOOK = 'RBCEUR0' AND B.ISLATEST = TRUE";
//
//            runSql(ignite, sql, true);
//        });

        // TODO: Bad, ignore
//        System.out.println(">>>> Benchmarking R -> B -> T ...");
//        benchmark(() -> {
//            String sql =
//                "SELECT  COUNT(*)\n" +
//                    "FROM           RISK R " +
//                    "    INNER JOIN BATCH B ON (R.BATCHKEY = B.BATCHKEY) " +
//                    "    INNER JOIN TRADE T ON (R.TRADEIDENTIFIER = T.TRADEIDENTIFIER AND R.TRADEVERSION = T.TRADEVERSION) " +
//                    "WHERE" +
//                    "    T.BOOK = 'RBCEUR0' AND B.ISLATEST = TRUE";
//
//            runSql(ignite, sql, true);
//        });

//        // TODO: Same as "original", ignore
//        System.out.println(">>>> Benchmarking T -> R -> B ...");
//        benchmark(() -> {
//            String sql =
//                "SELECT  COUNT(*)\n" +
//                    "FROM           TRADE T " +
//                    "    INNER JOIN RISK R ON (R.TRADEIDENTIFIER = T.TRADEIDENTIFIER AND R.TRADEVERSION = T.TRADEVERSION) " +
//                    "    INNER JOIN BATCH B ON (R.BATCHKEY = B.BATCHKEY) " +
//                    "WHERE" +
//                    "    T.BOOK = 'RBCEUR0' AND B.ISLATEST = TRUE";
//
//            runSql(ignite, sql, true);
//        });

        // TODO: Bad, ignore
//        System.out.println(">>>> Benchmarking B -> R -> T ...");
//        benchmark(() -> {
//            String sql =
//                "SELECT  COUNT(*)\n" +
//                    "FROM           BATCH B " +
//                    "    INNER JOIN RISK R ON (R.BATCHKEY = B.BATCHKEY) " +
//                    "    INNER JOIN TRADE T ON (R.TRADEIDENTIFIER = T.TRADEIDENTIFIER AND R.TRADEVERSION = T.TRADEVERSION) " +
//                    "WHERE" +
//                    "    T.BOOK = 'RBCEUR0' AND B.ISLATEST = TRUE";
//
//            runSql(ignite, sql, true);
//        });

        // TODO: Good, but depends on B.ISLATEST selectivity
//        System.out.println(">>>> Benchmarking B -> T -> R ...");
//        benchmark(() -> {
//            String sql =
//                "SELECT COUNT(*) FROM " +
//                    "BATCH B INNER JOIN " +
//                    "(SELECT R.BATCHKEY, T.BOOK FROM TRADE T INNER JOIN RISK R ON R.TRADEIDENTIFIER = T.TRADEIDENTIFIER AND R.TRADEVERSION = T.TRADEVERSION) TR " +
//                    "ON B.BATCHKEY = TR.BATCHKEY " +
//                    "WHERE TR.BOOK = 'RBCEUR0' AND B.ISLATEST = TRUE";
//
//            runSql(ignite, sql, true);
//        });

        G.stopAll(true);
    }

    private static void benchmark(Runnable r) {
        // Warmup
        for (int i = 0; i < NUM_OF_ITERATIONS; i++)
            r.run();

        long totalTime = 0;
        List<Long> times = new ArrayList<>();
        for (int i = 0; i < NUM_OF_ITERATIONS; i++) {
            long start = System.currentTimeMillis();
            r.run();
            long time = System.currentTimeMillis() - start;
            totalTime += time;
            times.add(time);
        }

        Collections.sort(times);

        System.out.printf(">>>>>> Avg: %d, Min: %d, Med: %d, Max: %d\n",
            totalTime / NUM_OF_ITERATIONS, times.get(0), times.get(NUM_OF_ITERATIONS / 2), times.get(NUM_OF_ITERATIONS - 1));

        for (int i = 0; i < 5; i++)
            System.out.println();
    }

    private static void runSql(Ignite ignite, String sql, boolean enforceJoinOrder) {
        IgniteCache cache = ignite.getOrCreateCache("gateway");

//        if (PLANS.add(sql)) {
//            List<?> planParts = (List<?>)cache.query(new SqlFieldsQuery("EXPLAIN " + sql).setSchema("PUBLIC").setEnforceJoinOrder(enforceJoinOrder)).getAll().get(0);
//
//            for (Object planPart : planParts)
//                System.out.println(planPart);
//
//            System.out.println();
//        }

        cache.query(new SqlFieldsQuery(sql).setSchema("PUBLIC").setEnforceJoinOrder(enforceJoinOrder)).getAll();
    }

    private static final Set<String> PLANS = Collections.newSetFromMap(new ConcurrentHashMap<>());
}
