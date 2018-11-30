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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.examples.ml.util.benchmark.thinclient.utils.BenchParameters;
import org.apache.ignite.examples.ml.util.benchmark.thinclient.utils.Measure;
import org.jetbrains.annotations.NotNull;

/**
 * Start {@link ServerMock} prior to launching this benchmark.
 */
public class Benchmark {
    private static final int SAMPLES = 5;
    private static final int[] PAGE_SIZES = new int[] {10, 20, 50, 100, 200, 300, 400, 500, 1000, 2000};

    private static ExecutorService POOL;
    private static BenchParameters PARAMETERS;

    public static AtomicLong downloadedBytes = new AtomicLong(0L);

    public static void main(String... args) throws Exception {
        PARAMETERS = BenchParameters.parseArguments(args);
        System.out.println("Start benchmark with such configuration: [" + PARAMETERS.toString() + "]");

        Long start = System.currentTimeMillis();

        List<MeasureWithMeta> benchMeta = new ArrayList<>();

        for (int threadCount = 1; threadCount <= PARAMETERS.getMaxThreadCount(); ) {
            try {
                POOL = Executors.newFixedThreadPool(threadCount);

                for (int pageSize : PAGE_SIZES) {
                    MeasureWithMeta measuresWithMeta = measureSpecificConfiguration(threadCount, pageSize);
                    benchMeta.add(measuresWithMeta);
                }
            }
            finally {
                POOL.shutdown();
                POOL.awaitTermination(1, TimeUnit.DAYS);
            }

            threadCount = Math.min(PARAMETERS.getMaxThreadCount() + 1, threadCount * 2);
        }

        Long end = System.currentTimeMillis();
        System.out.println(String.format("Done [downloaded kbytes = %d in %d sec.]", downloadedBytes.get() / 1024, (end - start) / 1000));
        System.out.println();
        printTable(benchMeta);
    }

    @NotNull private static Benchmark.MeasureWithMeta measureSpecificConfiguration(int threadCount,
        int pageSize) throws Exception {
        long alreadyDownloadedKBytes = downloadedBytes.get() / 1024;
        long startTS = System.currentTimeMillis();
        ArrayList<Measure> times = new ArrayList<>(SAMPLES);
        for (int i = 0; i < SAMPLES; i++)
            times.add(oneMeasure(pageSize, threadCount, false, true));

        long endTS = System.currentTimeMillis();
        long payloadInKB = (downloadedBytes.get() / 1024) - alreadyDownloadedKBytes;
        double throughputInKB = (1.0 * payloadInKB) / (0.001 * (endTS - startTS));
        double throughputInRows = (1.0 * SAMPLES * PARAMETERS.getCountOfRows()) / (0.001 * (endTS - startTS));
        System.out.println(String.format("measure [page size = %d, thread count = %d, throughput (kbytes/s) = %.2f, throughput (rows/s) = %.2f]",
            pageSize, threadCount, throughputInKB, throughputInRows
        ));
        Measure.computeStatsAndPrint(pageSize, times);

        return new MeasureWithMeta(threadCount, pageSize, times, throughputInKB, throughputInRows);
    }

    private static Measure oneMeasure(int pageSize, int currentClientCount, boolean useFilter,
        boolean distinguishPartitions) throws Exception {
        ArrayList<Future<Optional<Measure>>> futures = new ArrayList<>(currentClientCount);
        for (int i = 0; i < currentClientCount; i++) {
            final int clientID = i;
            futures.add(POOL.submit(() -> {
                return new ThinClientMock(
                    pageSize, clientID, currentClientCount,
                    distinguishPartitions, PARAMETERS
                ).measure();
            }));
        }

        ArrayList<Measure> bucket = new ArrayList<>(futures.size());
        for (Future<Optional<Measure>> f : futures)
            f.get().ifPresent(bucket::add);
        return Measure.sumOf(bucket);
    }

    private static class MeasureWithMeta {

        private final List<Measure> measures;
        private final int threadCount;
        private final int pageSize;
        private final double throughputInKB;
        private final double throughputInRows;

        public MeasureWithMeta(int threadCount, int pageSize, List<Measure> measures,
            double throughputInKB, double throughputInRows) {

            this.throughputInKB = throughputInKB;
            this.throughputInRows = throughputInRows;
            this.measures = measures;
            this.threadCount = threadCount;
            this.pageSize = pageSize;
        }
    }

    private static void printTable(List<MeasureWithMeta> benchMeta) {
        StringBuilder header = new StringBuilder("count of ignites\tquery parallelism\trows\tcount of partitions\tthread count\tpage size\tthroughput (kbytes/s)\tthroughput (rows/s)");
        Measure.tableHeader().forEach(name -> header.append("\t" + name));
        System.out.println(header);
        benchMeta.forEach(meta -> {
            StringBuilder row = new StringBuilder();
            row.append(PARAMETERS.getCountOfIgnites()).append("\t")
                .append(PARAMETERS.getQueryParallelism()).append("\t")
                .append(PARAMETERS.getCountOfRows()).append("\t")
                .append(PARAMETERS.getCountOfPartitions()).append("\t")
                .append(meta.threadCount).append("\t")
                .append(meta.pageSize).append("\t")
                .append(meta.throughputInKB).append("\t")
                .append(meta.throughputInRows).append("\t");
            Measure.computeAllMetrics(meta.measures).forEach(r -> {
                row.append(r.mean).append("\t");
            });
            System.out.println(row);
        });
    }
}
