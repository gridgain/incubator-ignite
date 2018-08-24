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

package org.apache.ignite.yardstick.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.cache.model.SampleValue;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 *
 */
public class IgnitePutAllMultithreadedBenchmark extends IgniteAbstractBenchmark {
    /** */
    private String cacheName;

    /** */
    private ExecutorService executor;

    /** */
    private int entries;

    /** Batch size for a single putAll operation. */
    private int batchSize;

    /** */
    private int threadsNum;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        entries = args.range();

        cacheName = args.loadCacheName();

        threadsNum = args.loadThreadsNumber();

        batchSize = args.batch();

        if (entries <= 0)
            throw new IllegalArgumentException("Invalid number of entries: " + entries);

        if (cfg.threads() != 1)
            throw new IllegalArgumentException("IgnitePutAllMultithreadedBenchmark should be run with single thread. " +
                "Internally it starts multiple threads.");

        IgniteCache<Integer, SampleValue> cache = ignite().cache(cacheName);

        if (cache == null)
            throw new IllegalArgumentException("Cache \"" + cacheName + "\" was not found.");

        executor = Executors.newFixedThreadPool(threadsNum);

        BenchmarkUtils.println("IgnitePutAllMultithreadedBenchmark start [cacheName=" + cacheName +
            ", threadsNum=" + threadsNum +
            ", entries=" + entries +
            ", batchSize=" + batchSize + "]");

        if (cfg.warmup() > 0) {
            BenchmarkUtils.println("IgnitePutAllMultithreadedBenchmark start warmup [warmupTimeMillis=" + cfg.warmup() + ']');

            final long warmupEnd = System.currentTimeMillis() + cfg.warmup();

            final AtomicBoolean stop = new AtomicBoolean();

            try {
                List<Future<Void>> futs = new ArrayList<>();

                for (int i = 0; i < threadsNum; i++) {
                    int threadIdx = i;

                    futs.add(executor.submit(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            Thread.currentThread().setName("putAll-" + cacheName + "-" + threadIdx);

                            BenchmarkUtils.println("IgnitePutAllMultithreadedBenchmark start warmup for cache " +
                                "[name=" + cacheName + ']');

                            final int KEYS = Math.min(100_000, entries);

                            int key = threadIdx * KEYS + 1;

                            Map<Integer, SampleValue> batch = new TreeMap<>();

                            while (System.currentTimeMillis() < warmupEnd && !stop.get()) {
                                for (int i1 = 0; i1 < batchSize; i1++)
                                    batch.put(-key++, new SampleValue(key));

                                cache.putAll(batch);

                                batch.clear();
                            }

                            BenchmarkUtils.println("IgnitePutAllMultithreadedBenchmark finished warmup for cache " +
                                "[name=" + cacheName + ']');

                            return null;
                        }
                    }));
                }

                for (Future<Void> fut : futs)
                    fut.get();
            }
            finally {
                cache.clear();

                stop.set(true);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        BenchmarkUtils.println("IgnitePutAllMultithreadedBenchmark start test.");

        long start = System.currentTimeMillis();

        final AtomicBoolean stop = new AtomicBoolean();

        try {
            List<Future<Void>> futs = new ArrayList<>();

            BenchmarkUtils.println("IgnitePutAllMultithreadedBenchmark start load cache [name=" + cacheName + ']');

            IgniteCache<Integer, SampleValue> cache = ignite().cache(cacheName);

            for (int i = 0; i < threadsNum; i++) {
                int threadIdx = i;

                futs.add(executor.submit(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        Thread.currentThread().setName("putAll-" + cacheName + "-" + threadIdx);

                        long start1 = System.currentTimeMillis();

                        Map<Integer, SampleValue> batch = new TreeMap<>();

                        for (int i1 = 0; i1 < entries; i1++) {
                            int key = ThreadLocalRandom.current().nextInt();

                            batch.put(key, new SampleValue(key));

                            if (batch.size() == batchSize || i1 == entries - 1) {
                                cache.putAll(batch);

                                batch.clear();
                            }

                            if (i1 > 0 && i1 % 1000 == 0) {
                                if (stop.get())
                                    break;

                                if (i1 % 100_000 == 0) {
                                    BenchmarkUtils.println("IgnitePutAllMultithreadedBenchmark cache load progress [" +
                                        "thread=" + Thread.currentThread().getName() +
                                        ", entries=" + i1 +
                                        ", timeMillis=" + (System.currentTimeMillis() - start1) + ']');
                                }
                            }
                        }

                        long time = System.currentTimeMillis() - start1;

                        BenchmarkUtils.println("IgnitePutAllMultithreadedBenchmark finished load cache [" +
                            "thread=" + Thread.currentThread().getName() +
                            ", entries=" + entries +
                            ", batchSize=" + batchSize +
                            ", totalTimeMillis=" + time + ']');

                        return null;
                    }
                }));
            }

            for (Future<Void> fut : futs)
                fut.get();
        }
        finally {
            stop.set(true);
        }

        long time = System.currentTimeMillis() - start;

        BenchmarkUtils.println("IgnitePutAllMultithreadedBenchmark finished [totalTimeMillis=" + time +
            ", entries=" + entries +
            ", batchSize=" + batchSize + ']');

        BenchmarkUtils.println("Cache size [cacheName=" + cacheName +
            ", size=" + ignite().cache(cacheName).size() + ']');

        return false;
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        if (executor != null)
            executor.shutdown();

        super.tearDown();
    }
}
