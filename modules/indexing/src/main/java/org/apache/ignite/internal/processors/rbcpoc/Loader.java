package org.apache.ignite.internal.processors.rbcpoc;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

public class Loader {
    private static final String SQL_PUBLIC_BATCH_TRADE_LOOKUP = "BATCH_TRADE_LOOKUP_CACHE";
    private static final String SQL_PUBLIC_BATCH_TRADE_FULL = "BATCH_TRADE_FULL_CACHE";
    private static final String SQL_PUBLIC_BATCH = "BATCH_CACHE";
    private static final String SQL_PUBLIC_BATCH_ONHEAP = "BATCH_ONHEAP_CACHE";
    private static final String SQL_PUBLIC_TRADE = "TRADE_CACHE";
    private static final String SQL_PUBLIC_RISK = "RISK_CACHE";
    public static final int RISKS = 25_000_000 / 20;
    public static final int TRADES = 50_000;
    public static final int BOOKS = 5;
    public static final int BATCHES = 100;

    public static void main(String... args) {
        Ignition.setClientMode(true);
        Ignite ignite = Ignition.start(Server.getConfiguration().setIgniteInstanceName("loader"));

        ignite.addCacheConfiguration(
            new CacheConfiguration<>("ONHEAP")
                .setSqlOnheapCacheEnabled(true)
                .setCopyOnRead(false)
                .setCacheMode(CacheMode.REPLICATED)
        );

        String[] ddls = readDdl();

        for (String ddl : ddls)
            runSql(ignite, ddl);

        System.out.println(">>>> Created tables");

        fill(ignite);

        System.out.println(">>>> Filled tables");

        ignite.close();
    }

    private static String[] readDdl() {
        StringBuilder sb = new StringBuilder();

        try (
            InputStream resource = new FileInputStream("C:\\Personal\\code\\incubator-ignite\\modules\\indexing\\src\\main\\java\\org\\apache\\ignite\\internal\\processors\\rbcpoc\\ddl.sql");
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(resource, StandardCharsets.UTF_8))
        ) {
            bufferedReader
                .lines()
                .forEachOrdered(sb::append);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        String sql = sb.toString();

        return sql.split(";");
    }

    private static void runSql(Ignite ignite, String sql) {
        IgniteCache cache = ignite.getOrCreateCache("gateway");

        cache.query(new SqlFieldsQuery(sql).setSchema("PUBLIC")).getAll();
    }

    private static void fill(Ignite ignite) {
        IgniteCache<BinaryObject, BinaryObject> tradeCache = ignite.cache(SQL_PUBLIC_TRADE).withKeepBinary();
        IgniteCache<BinaryObject, BinaryObject> riskCache = ignite.cache(SQL_PUBLIC_RISK).withKeepBinary();
        IgniteCache<BinaryObject, BinaryObject> batchCache = ignite.cache(SQL_PUBLIC_BATCH).withKeepBinary();
        IgniteCache<BinaryObject, BinaryObject> batchOnheapCache = ignite.cache(SQL_PUBLIC_BATCH_ONHEAP).withKeepBinary();
        IgniteCache<BinaryObject, BinaryObject> batchTradeLookupCache = ignite.cache(SQL_PUBLIC_BATCH_TRADE_LOOKUP).withKeepBinary();
        IgniteCache<BinaryObject, BinaryObject> batchTradeFullCache = ignite.cache(SQL_PUBLIC_BATCH_TRADE_FULL).withKeepBinary();

        for (long tradeId = 0; tradeId < TRADES; tradeId++)
            putTradeRecord(tradeCache, ignite, tradeId);

        System.out.println(">>>>>> Loaded trades");

        for (long i = 0; i < BATCHES; i++) {
            long batchKey = i;

            putBatchRecord(batchCache, ignite, batchKey);
            putBatchRecord(batchOnheapCache, ignite, batchKey);

            IntStream.range(0, RISKS / BATCHES).parallel().forEach(batchRisk -> {
                    long riskId = batchKey * RISKS / BATCHES + batchRisk;
                    long tradeId = ThreadLocalRandom.current().nextInt(TRADES);

                    putRiskRecord(riskCache, ignite, riskId, tradeId, batchKey);

                    putBatchTradeLookupRecord(batchTradeLookupCache, ignite, tradeId, batchKey);

                    putBatchTradeFullRecord(batchTradeFullCache, ignite, tradeId, batchKey);
                }
            );

            if ((batchKey + 1) % 10 == 0)
                System.out.println(">>>>>> Loaded batch " + (batchKey + 1));
        }
    }

    private static void putBatchTradeFullRecord(IgniteCache<BinaryObject, BinaryObject> cache, Ignite ignite,
        long tradeId, long batchKey) {
        BinaryObjectBuilder bKey = ignite.binary().builder("BATCH_TRADE_FULL_KEY");
        bKey.setField("BATCHKEY", batchKey);
        bKey.setField("TRADEIDENTIFIER", tradeId);
        bKey.setField("TRADEVERSION", 1L);

        BinaryObjectBuilder bValue = ignite.binary().builder("BATCH_TRADE_FULL_VALUE");
        long bookIdx = tradeId % BOOKS;
        bValue.setField("BOOK", "RBCEUR" + bookIdx, String.class);
        bValue.setField("ISLATEST", true, Boolean.class);
        bValue.setField("VALUE1", "tradeValue" + tradeId, String.class);

        cache.put(bKey.build(), bValue.build());
    }

    private static void putBatchTradeLookupRecord(IgniteCache<BinaryObject, BinaryObject> cache, Ignite ignite,
        long tradeId, long batchKey) {
        BinaryObjectBuilder bKey = ignite.binary().builder("BATCH_TRADE_KEY");
        bKey.setField("BATCHKEY", batchKey);
        bKey.setField("TRADEIDENTIFIER", tradeId);
        bKey.setField("TRADEVERSION", 1L);

        BinaryObjectBuilder bValue = ignite.binary().builder("BATCH_TRADE_VALUE");
        bValue.setField("DUMMY_VALUE", 0);

        cache.put(bKey.build(), bValue.build());
    }

    private static void putRiskRecord(IgniteCache<BinaryObject, BinaryObject> riskCache, Ignite ignite,
        long riskId, long tradeId, long batchKey) {
        BinaryObjectBuilder bKey = ignite.binary().builder("RISK_KEY");
        bKey.setField("RISK_ID", riskId);

        BinaryObjectBuilder bValue = ignite.binary().builder("RISK_VALUE");
        bValue.setField("TRADEIDENTIFIER", tradeId);
        bValue.setField("TRADEVERSION", 1L);
        bValue.setField("BATCHKEY", batchKey);
        bValue.setField("VALUE2", ThreadLocalRandom.current().nextDouble(), Double.class);

        riskCache.put(bKey.build(), bValue.build());
    }

    private static void putTradeRecord(IgniteCache<BinaryObject, BinaryObject> cache, Ignite ignite, long tradeId) {
        BinaryObjectBuilder bKey = ignite.binary().builder("TRADE_KEY");
        bKey.setField("TRADEIDENTIFIER", tradeId);
        bKey.setField("TRADEVERSION", 1L);

        BinaryObjectBuilder bValue = ignite.binary().builder("TRADE_VALUE");
        long bookIdx = tradeId % BOOKS;
        bValue.setField("BOOK", "RBCEUR" + bookIdx, String.class);
        bValue.setField("VALUE1", "tradeValue" + tradeId, String.class);

        cache.put(bKey.build(), bValue.build());
    }

    private static void putBatchRecord(IgniteCache<BinaryObject, BinaryObject> cache, Ignite ignite, long batchKey) {
        BinaryObjectBuilder bKey = ignite.binary().builder("BATCH_KEY");
        bKey.setField("BATCHKEY", batchKey);

        BinaryObjectBuilder bValue = ignite.binary().builder("BATCH_VALUE");
        bValue.setField("ISLATEST", true, Boolean.class);

        cache.put(bKey.build(), bValue.build());
    }
}
