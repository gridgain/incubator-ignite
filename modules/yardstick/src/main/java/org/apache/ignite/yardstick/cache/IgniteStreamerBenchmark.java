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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.cache.model.WorkdayValue;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 *
 */
public class IgniteStreamerBenchmark extends IgniteAbstractBenchmark {
    /** */
    private static final int AVG_VAL_SIZE = 200;

    /** */
    private static final int MIN_VAL_SIZE = 100;

    /** */
    private static final int VAL_SIZE_DEVIATION = 100;

    /** */
    private static final int BIG_VAL_SIZE = 12_000;

    /** */
    private static final double BIG_VAL_FRAC = 0.05;

    /** */
    private String cacheName;

    /** */
    private ExecutorService executor;

    /** */
    private int entries;

    /** */
    private int threadsNum;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        entries = args.range();

        cacheName = args.loadCacheName();

        threadsNum = args.loadThreadsNumber();

        if (entries <= 0)
            throw new IllegalArgumentException("Invalid number of entries: " + entries);

        if (cfg.threads() != 1)
            throw new IllegalArgumentException("IgniteStreamerBenchmark should be run with single thread. " +
                "Internally it starts multiple threads.");

        IgniteCache<Integer, WorkdayValue> cache = ignite().cache(cacheName);

        if (cache == null)
            throw new IllegalArgumentException("Cache \"" + cacheName + "\" was not found.");

        executor = Executors.newFixedThreadPool(threadsNum);

        BenchmarkUtils.println("IgniteStreamerBenchmark start [cacheName=" + cacheName +
            ", threadsNum=" + threadsNum +
            ", entries=" + entries +
            ", bufferSize=" + args.streamerBufferSize() + "]");
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        BenchmarkUtils.println("IgniteStreamerBenchmark start test.");

        long start = System.currentTimeMillis();

        final AtomicBoolean stop = new AtomicBoolean();

        try (IgniteDataStreamer<Integer, WorkdayValue> streamer = streamer()) {
            List<Future<Void>> futs = new ArrayList<>();

            BenchmarkUtils.println("IgniteStreamerBenchmark start load cache [name=" + cacheName + ']');

            for (int i = 0; i < threadsNum; i++) {
                int threadIdx = i;

                futs.add(executor.submit(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        Thread.currentThread().setName("streamer-" + cacheName + "-" + threadIdx);

                        long start1 = System.currentTimeMillis();

                        for (int i1 = 0; i1 < entries; i1++) {
                            int key = ThreadLocalRandom.current().nextInt();

                            streamer.addData(key, generateValue());

                            if (i1 > 0 && i1 % 1000 == 0) {
                                if (stop.get())
                                    break;

                                if (i1 % 100_000 == 0) {
                                    BenchmarkUtils.println("IgniteStreamerBenchmark cache load progress [" +
                                        "thread=" + Thread.currentThread().getName() +
                                        ", entries=" + i1 +
                                        ", timeMillis=" + (System.currentTimeMillis() - start1) + ']');
                                }
                            }
                        }

                        long time = System.currentTimeMillis() - start1;

                        BenchmarkUtils.println("IgniteStreamerBenchmark finished load cache [" +
                            "thread=" + Thread.currentThread().getName() +
                            ", entries=" + entries +
                            ", bufferSize=" + args.streamerBufferSize() +
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

        BenchmarkUtils.println("IgniteStreamerBenchmark finished [totalTimeMillis=" + time +
            ", entries=" + entries +
            ", bufferSize=" + args.streamerBufferSize() + ']');

        BenchmarkUtils.println("Cache size [cacheName=" + cacheName +
            ", size=" + ignite().cache(cacheName).size() + ']');

        return false;
    }

    /**
     * @return Data streamer instance.
     */
    private IgniteDataStreamer<Integer, WorkdayValue> streamer() {
        IgniteDataStreamer<Integer, WorkdayValue> streamer = ignite().dataStreamer(cacheName);

        streamer.allowOverwrite(args.streamerAllowOverwrite());

        streamer.perNodeBufferSize(args.streamerBufferSize());

        return streamer;
    }

    /**
     * @return Generated value.
     */
    private static WorkdayValue generateValue() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int len;
        if (random.nextDouble() < BIG_VAL_FRAC)
            len = BIG_VAL_SIZE;
        else {
            double std = random.nextGaussian();

            len = Math.max(MIN_VAL_SIZE, (int)(std * VAL_SIZE_DEVIATION + AVG_VAL_SIZE));
        }

        byte[] data = new byte[len];
        ThreadLocalRandom.current().nextBytes(data);

        return new WorkdayValue(data);
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        if (executor != null)
            executor.shutdown();

        super.tearDown();
    }
}
