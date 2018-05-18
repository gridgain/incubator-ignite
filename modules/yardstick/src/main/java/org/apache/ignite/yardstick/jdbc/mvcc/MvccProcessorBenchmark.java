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
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.jdbc.DisjointRangeGenerator;
import org.yardstickframework.BenchmarkConfiguration;

import static org.apache.ignite.yardstick.jdbc.JdbcUtils.fillData;
import static org.yardstickframework.BenchmarkUtils.println;

public class MvccProcessorBenchmark extends IgniteAbstractBenchmark {
    private int memberId;
    private static final String UPDATE_QRY = "UPDATE test_long SET val = (val + 1) WHERE id BETWEEN ? AND ?";
    private DisjointRangeGenerator locIdGen;
    private int idOffset;

    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        memberId = cfg.memberId();

        if (memberId < 0)
            throw new IllegalStateException("Member id should be initialized with non-negative value");

        // assume there is no client nodes in the cluster except clients that are yardstick drivers.
        int driversNodesCnt = ignite().cluster().forClients().nodes().size();

        IgniteCountDownLatch dataIsReady = ignite().countDownLatch("fillDataLatch", driversNodesCnt, true, true);

        try {

            if (memberId == 0)
                fillData(cfg, (IgniteEx)ignite(), args.range(), args.atomicMode());
            else
                println(cfg, "No need to upload data (memberId = " + memberId + ").");

            dataIsReady.countDown();
            dataIsReady.await(); // todo: timeout;
        } catch (Throwable th) {
            dataIsReady.countDownAll();

            throw new RuntimeException("Fill Data failed.", th);
        }


        assert memberId < driversNodesCnt
            : "Incorrect memberId : [memberId= " + memberId + ", driversNodeCnt=" + driversNodesCnt + "]";

        int locIdRangeWidth = args.range() / driversNodesCnt;

        locIdGen = new DisjointRangeGenerator(cfg.threads(), locIdRangeWidth, args.sqlRange());

        idOffset = locIdRangeWidth * memberId;
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        long locStart = locIdGen.nextRangeStartId();

        long start = idOffset + locStart;

        long end = idOffset + locIdGen.endRangeId(locStart);

        ((IgniteEx)ignite())
            .context()
            .query()
            .querySqlFields(new SqlFieldsQuery(UPDATE_QRY)
                .setArgs(start, end), false)
            .getAll();

        return true;
    }
}
