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
import org.apache.ignite.examples.ml.util.benchmark.thinclient.utils.Measure;

/**
 * Start {@link ServerMock} prior to launching this benchmark.
 */
public class Benchmark {
    private static final int SAMPLES = 20;
    private static final int[] PAGE_SIZES = new int[] {5, 10, 20, 50, 100, 150, 200, 300, 400, 500, 600};

    private static final int MAX_THREAD_COUNT = 8;
    public static final boolean DISTINGUISH_PARTITIONS = true;
    public static final boolean USE_FILTER = false;

    static {
        assert MAX_THREAD_COUNT <= ServerMock.COUNT_OF_PARTITIONS : "THREAD_COUNT <= ServerMock.COUNT_OF_PARTITIONS";
        assert (!DISTINGUISH_PARTITIONS && !USE_FILTER) || DISTINGUISH_PARTITIONS ^ USE_FILTER : "(!DISTINGUISH_PARTITIONS && !USE_FILTER) || DISTINGUISH_PARTITIONS ^ USE_FILTER";
    }

    private static ExecutorService POOL;

    public static AtomicLong downloadedBytes = new AtomicLong(0L);

    public static void main(String... args) throws Exception {
        Long start = System.currentTimeMillis();

        List<MeasureWithMeta> benchMeta = new ArrayList<>();

        for (int threadCount = 1; threadCount <= MAX_THREAD_COUNT; ) {
            try {
                POOL = Executors.newFixedThreadPool(threadCount);

                for (int pageSize : PAGE_SIZES) {
                    long alreadyDownloadedKBytes = downloadedBytes.get() / 1024;
                    ArrayList<Measure> times = new ArrayList<>(SAMPLES);
                    for (int i = 0; i < SAMPLES; i++)
                        times.add(oneMeasure(pageSize, USE_FILTER, DISTINGUISH_PARTITIONS));

                    System.out.println(String.format("measure [page size = %d, thread count = %d, downloaded kbytes = %d]",
                        pageSize, threadCount, (downloadedBytes.get() / 1024) - alreadyDownloadedKBytes
                    ));
                    Measure.computeStatsAndPrint(pageSize, times);

                    benchMeta.add(new MeasureWithMeta(threadCount, pageSize, times));
                }
            }
            finally {
                POOL.shutdown();
                POOL.awaitTermination(1, TimeUnit.DAYS);
            }

            threadCount = Math.min(MAX_THREAD_COUNT + 1, threadCount * 2);
        }

        Long end = System.currentTimeMillis();
        System.out.println(String.format("Done [downloaded kbytes = %d in %d sec.]", downloadedBytes.get() / 1024, (end - start) / 1000));
        System.out.println();
        printTable(benchMeta);
    }

    private static Measure oneMeasure(int pageSize, boolean useFilter, boolean distinguishPartitions) throws Exception {
        ArrayList<Future<Optional<Measure>>> futures = new ArrayList<>(MAX_THREAD_COUNT);
        for (int i = 0; i < MAX_THREAD_COUNT; i++) {
            final int clientID = i;
            futures.add(POOL.submit(() -> {
                return new ThinClientMock(
                    pageSize, clientID, MAX_THREAD_COUNT,
                    useFilter, distinguishPartitions
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

        public MeasureWithMeta(int threadCount, int pageSize, List<Measure> measures) {
            this.measures = measures;
            this.threadCount = threadCount;
            this.pageSize = pageSize;
        }
    }

    private static void printTable(List<MeasureWithMeta> benchMeta) {
        StringBuilder header = new StringBuilder("rows\tcount of partitions\tthread count\tpage size");
        Measure.tableHeader().forEach(name -> header.append("\t" + name));
        System.out.println(header);
        benchMeta.forEach(meta -> {
            StringBuilder row = new StringBuilder();
            row.append(ServerMock.COUNT_OF_ROWS).append("\t")
                .append(ServerMock.COUNT_OF_PARTITIONS).append("\t")
                .append(meta.threadCount).append("\t")
                .append(meta.pageSize).append("\t");
            Measure.computeAllMetrics(meta.measures).forEach(r -> {
                row.append(r.mean).append("\t");
            });
            System.out.println(row);
        });
    }
}
