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

package org.apache.ignite.examples.ml.util.benchmark.thinclient;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.examples.ml.util.benchmark.thinclient.utils.Measure;

/**
 * Start {@link ServerMock} prior to launching this benchmark.
 */
public class Benchmark {
    private static final int SAMPLES = 20;
    private static final int[] PAGE_SIZES = new int[] {5/*, 10, 20, 50, 100, 150, 200, 300, 400, 500, 600*/};

    private static final int THREAD_COUNT = 10;
    public static final boolean DISTINGUISH_PARTITIONS = true;
    public static final boolean USE_FILTER = false;

    static {
        assert THREAD_COUNT <= ServerMock.COUNT_OF_PARTITIONS : "THREAD_COUNT <= ServerMock.COUNT_OF_PARTITIONS";
        assert (!DISTINGUISH_PARTITIONS && !USE_FILTER) || DISTINGUISH_PARTITIONS ^ USE_FILTER : "(!DISTINGUISH_PARTITIONS && !USE_FILTER) || DISTINGUISH_PARTITIONS ^ USE_FILTER";
    }

    private static ExecutorService POOL = Executors.newFixedThreadPool(THREAD_COUNT);

    public static AtomicLong downloadedBytes = new AtomicLong(0L);

    public static void main(String... args) throws Exception {
        Long start = System.currentTimeMillis();
        Long end = 0L;
        try {
            for (int pageSize : PAGE_SIZES) {
                ArrayList<Measure> times = new ArrayList<>(SAMPLES);
                for (int i = 0; i < SAMPLES; i++)
                    times.add(oneMeasure(pageSize, USE_FILTER, DISTINGUISH_PARTITIONS));

                Measure.computeStatsAndPrint(pageSize, times);
            }
            end = System.currentTimeMillis();
        } finally {
            POOL.shutdown();
            POOL.awaitTermination(1, TimeUnit.DAYS);
            System.out.println(String.format("Done [downloaded kbytes = %d by %d s]", downloadedBytes.get() / 1024, (end - start) / 1000));
        }
    }

    private static Measure oneMeasure(int pageSize, boolean useFilter, boolean distinguishPartitions) throws Exception {
        ArrayList<Future<Optional<Measure>>> futures = new ArrayList<>(THREAD_COUNT);
        for(int i = 0; i < THREAD_COUNT; i++) {
            final int clientID = i;
            futures.add(POOL.submit(() -> {
                return new ThinClientMock(pageSize, clientID, THREAD_COUNT, useFilter, distinguishPartitions).measure();
            }));
        }

        ArrayList<Measure> bucket = new ArrayList<>(futures.size());
        for(Future<Optional<Measure>> f : futures)
            f.get().ifPresent(bucket::add);
        return Measure.sumOf(bucket);
    }
}
