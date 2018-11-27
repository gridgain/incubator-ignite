package org.apache.ignite.yardstick.jdbc.mvcc;

import org.apache.ignite.cache.query.SqlFieldsQuery;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class MvccSelectBenchmark extends AbstractDistributedMvccBenchmark {
    public String select = "SELECT FROM PUBLIC.test_long WHERE val BETWEEN ? AND ?";

    @Override
    public boolean test(Map<Object, Object> map) throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        int start = rnd.nextInt(args.range() - args.sqlRange() + 1);

        int end = start + args.sqlRange() - 1;

        execute(new SqlFieldsQuery(select).setArgs(start, end));

        return true;
    }
}
