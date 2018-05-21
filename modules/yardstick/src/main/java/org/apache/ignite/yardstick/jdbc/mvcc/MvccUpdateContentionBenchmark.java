/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.yardstick.jdbc.mvcc;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.apache.ignite.yardstick.jdbc.JdbcUtils.fillData;
import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Benchmark used to eliminate update contention in mvcc mode.
 * Designed to be ran in many threads
 */
public class MvccUpdateContentionBenchmark extends IgniteAbstractBenchmark {
    private int memberId;

    private static final String UPDATE_QRY = "UPDATE test_long SET val = (val + 1) WHERE id BETWEEN ? AND ?";

    private static final String MVCC_EXC_MSG = "Mvcc version mismatch.";

    private static final String NO_MVCC_EXC_MSG_PREFIX = "Failed to UPDATE some keys because they had been modified concurrently";

    private final AtomicLong contentions = new AtomicLong();

    // todo: count write misses.

    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        memberId = cfg.memberId();

        if (memberId < 0)
            throw new IllegalStateException("Member id should be initialized with non-negative value");

        // assume there is no client nodes in the cluster except clients that are yardstick drivers.
        int driversNodesCnt = ignite().cluster().forClients().nodes().size();

        // todo: simplify just to use 1 value and count down only for memberId = 0;
        IgniteCountDownLatch dataIsReady = ignite().countDownLatch("fillDataLatch", driversNodesCnt, true, true);

        try {
            if (memberId == 0)
                fillData(cfg, (IgniteEx)ignite(), args.range(), args.atomicMode());
            else
                println(cfg, "No need to upload data (memberId = " + memberId + ").");

            dataIsReady.countDown();

            dataIsReady.await(); // todo: timeout;
        }
        catch (Throwable th) {
            dataIsReady.countDownAll();

            throw new RuntimeException("Fill Data failed.", th);
        }

        // workaround for Table not found
        ((IgniteEx)ignite())
            .context()
            .query()
            .querySqlFields(new SqlFieldsQuery("SELECT COUNT(*) FROM test_long"), false)
            .getAll();
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long start = rnd.nextLong(args.mvccContentionRange() - (args.sqlRange() - 1)) + 1;

        long end = start + (args.sqlRange() - 1);

        // todo : count write misses.
        try {
            ((IgniteEx)ignite())
                .context()
                .query()
                .querySqlFields(new SqlFieldsQuery(UPDATE_QRY)
                    .setArgs(start, end), false)
                .getAll();
        }
        catch (IgniteSQLException exc) {
            if ((args.mvccEnabled() && !exc.getMessage().equals(MVCC_EXC_MSG)) ||
                (!args.mvccEnabled() && !exc.getMessage().startsWith(NO_MVCC_EXC_MSG_PREFIX)))
                throw new RuntimeException("Exception with unexpected message is thrown.", exc);

            contentions.incrementAndGet();
        }
        catch (Exception e) {
            throw new RuntimeException("Could not perform update.", e);
        }

        return true;
    }

    @Override public void tearDown() throws Exception {
        try {
            super.tearDown();
        }
        finally {
            println("Update contention count : " + contentions.get());
        }
    }
}
