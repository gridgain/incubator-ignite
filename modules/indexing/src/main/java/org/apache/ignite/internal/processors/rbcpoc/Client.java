package org.apache.ignite.internal.processors.rbcpoc;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Client {
    private static final int NUM_OF_ITERATIONS = 20;

    public static void main(String[] args) {
        Ignition.setClientMode(true);
        Ignite ignite = Ignition.start(Server.getConfiguration());

        System.out.println(">>>> Benchmarking original query...");
        benchmark(() -> {
            String sql = "SELECT  *\n" +
                "FROM    RISK R, TRADE T, BATCH B\n" +
                "WHERE    R.BATCHKEY = B.BATCHKEY\n" +
                "AND R.TRADEIDENTIFIER = T.TRADEIDENTIFIER\n" +
                "AND    R.TRADEVERSION = T.TRADEVERSION\n" +
                "AND    T.BOOK = 'RBCEUR0'\n" +
                "AND B.ISLATEST = TRUE;";

            runSql(ignite, sql, false);
        });

        System.out.println(">>>> Benchmarking query without BATCH...");
        benchmark(() -> {
            String sql = "SELECT    *\n" +
                "FROM    RISK R, TRADE T\n" +
                "WHERE    R.TRADEIDENTIFIER = T.TRADEIDENTIFIER\n" +
                "AND    R.TRADEVERSION = T.TRADEVERSION\n" +
                "AND    T.BOOK = 'RBCEUR0';";
            runSql(ignite, sql, false);
        });

        System.out.println(">>>> Benchmarking query T -> B -> R...");
        benchmark(() -> {
            String sql = "SELECT  *\n" +
                "FROM TRADE T, BATCH B, RISK R\n" +
                "WHERE T.BOOK = 'RBCEUR0'\n" +
                "    AND B.ISLATEST = TRUE\n" +
                "    AND R.TRADEIDENTIFIER = T.TRADEIDENTIFIER\n" +
                "    AND R.TRADEVERSION = T.TRADEVERSION\n" +
                "    AND R.BATCHKEY = B.BATCHKEY";
            runSql(ignite, sql, true);
        });

        System.out.println(">>>> Benchmarking query B -> T -> R...");
        benchmark(() -> {
            String sql = "SELECT  *\n" +
                "FROM TRADE T, BATCH B, RISK R\n" +
                "WHERE T.BOOK = 'RBCEUR0'\n" +
                "    AND B.ISLATEST = TRUE\n" +
                "    AND R.TRADEIDENTIFIER = T.TRADEIDENTIFIER\n" +
                "    AND R.TRADEVERSION = T.TRADEVERSION\n" +
                "    AND R.BATCHKEY = B.BATCHKEY";
            runSql(ignite, sql, true);
        });

        System.out.println(">>>> Benchmarking query B -> (T -> R)...");
        benchmark(() -> {
            String sql = "SELECT *\n" +
                "FROM BATCH B, \n" +
                "    (SELECT T.VALUE1, T.BOOK, R.* \n" +
                "        FROM TRADE T, RISK R\n" +
                "        WHERE R.TRADEIDENTIFIER = T.TRADEIDENTIFIER\n" +
                "            AND R.TRADEVERSION = T.TRADEVERSION\n" +
                "            AND T.BOOK = 'RBCEUR0') TR\n" +
                "    WHERE TR.BATCHKEY = B.BATCHKEY\n" +
                "        AND B.ISLATEST = TRUE;";
            runSql(ignite, sql, true);
        });

        System.out.println(">>>> Benchmarking query T -> (B -> R)...");
        benchmark(() -> {
            String sql = "SELECT *\n" +
                "FROM TRADE T, \n" +
                "    (SELECT R.*\n" +
                "        FROM BATCH B, RISK R\n" +
                "        WHERE R.BATCHKEY = B.BATCHKEY\n" +
                "            AND B.ISLATEST = TRUE) BR\n" +
                "    WHERE BR.TRADEIDENTIFIER = T.TRADEIDENTIFIER\n" +
                "            AND BR.TRADEVERSION = T.TRADEVERSION\n" +
                "            AND T.BOOK = 'RBCEUR0'";
            runSql(ignite, sql, true);
        });

        ignite.close();
    }

    private static void benchmark(Runnable r) {
        // Warmup
        for (int i = 0; i < 5; i++)
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

        if (PLANS.add(sql)) {
            List<?> planParts = (List<?>)cache.query(new SqlFieldsQuery("EXPLAIN " + sql).setSchema("PUBLIC").setEnforceJoinOrder(enforceJoinOrder)).getAll().get(0);

            for (Object planPart : planParts)
                System.out.println(planPart);

            for (int i = 0; i < 5; i++)
                System.out.println();
        }

        cache.query(new SqlFieldsQuery(sql).setSchema("PUBLIC").setEnforceJoinOrder(enforceJoinOrder)).getAll();
    }

    private static final Set<String> PLANS = Collections.newSetFromMap(new ConcurrentHashMap<>());
}
