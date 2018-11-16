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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.examples.ml.util.benchmark.thinclient.utils.Measure;

public class Benchmark {
    private static final int SAMPLES = 20;
    private static final int[] PAGE_SIZES = new int[] {/*5, 10, 20, 50, 100, 150, 200, 300, 400, 500, */600};

    private static final int THREAD_COUNT = 1;
    private static ExecutorService POOL = Executors.newFixedThreadPool(THREAD_COUNT);

    public static void main(String... args) throws Exception {
        for (int pageSize : PAGE_SIZES) {
            ArrayList<Measure> times = new ArrayList<>(SAMPLES);
            for (int i = 0; i < SAMPLES; i++)
                times.add(oneMeasure(pageSize, false, false));

            Measure.computeStatsAndPrint(pageSize, times);
        }

        System.out.println("Done");
        POOL.shutdown();
        POOL.awaitTermination(1, TimeUnit.DAYS);
    }

    private static Measure oneMeasure(int pageSize, boolean useFilter, boolean distinguishPartitions) throws Exception {
        ArrayList<Future<Measure>> futures = new ArrayList<>(THREAD_COUNT);
        for(int i = 0; i < THREAD_COUNT; i++) {
            futures.add(POOL.submit(() -> {
                return new ThinClientMock(pageSize, THREAD_COUNT, useFilter, distinguishPartitions).measure();
            }));
        }

        ArrayList<Measure> bucket = new ArrayList<>(futures.size());
        for(Future<Measure> f : futures)
            bucket.add(f.get());
        return Measure.sumOf(bucket);
    }
}
