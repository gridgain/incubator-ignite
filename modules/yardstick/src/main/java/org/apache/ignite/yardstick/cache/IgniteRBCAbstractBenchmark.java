package org.apache.ignite.yardstick.cache;

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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.yardstick.cache.model.data.PositionData;
import org.apache.ignite.yardstick.cache.model.data.PositionDataGenerator;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 *
 */
public abstract class IgniteRBCAbstractBenchmark extends IgniteCacheAbstractBenchmark {

    /** */
    private ExecutorService executor;

    private final PositionDataGenerator generator = new PositionDataGenerator();

    /** */
    private long start = 0;

    /** */
    private long finish = 0;

    /** */
    private IgniteDataStreamer<Object, Object> positionStreamer;

    /** */
    private IgniteDataStreamer<Object, Object> sensitivityStreamer;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        executor = Executors.newFixedThreadPool(args.streamerConcurrentCaches());

        positionStreamer = ignite().dataStreamer("Position");
        sensitivityStreamer = ignite().dataStreamer("Sensitivity");
        positionStreamer.autoFlushFrequency(50);
        sensitivityStreamer.autoFlushFrequency(50);

        if (cfg.warmup() > 0) {
            BenchmarkUtils.println("IgniteStreamerBenchmark start warmup [warmupTimeMillis=" + cfg.warmup() + ']');

            final AtomicBoolean stop = new AtomicBoolean();

            try {
                for (int i = 0; i < 50; i++)
                    loadData();

                BenchmarkUtils.println("IgniteStreamerBenchmark finished warm up");
            }
            finally {
                stop.set(true);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache cache() {
        return ignite().cache("Position");
    }

    /** */
    public void loadData() throws InterruptedException, java.util.concurrent.ExecutionException {
        start = finish;
        finish += 100;
        List<Future<Void>> futs = new ArrayList<>();

        for (long i = start; i < finish; i++) {
            futs.add(executor.submit(new Callable() {
                @Override public Object call() throws Exception {
                    PositionData positionData = generator.generate();
                    positionStreamer.addData(positionData.getPosition().getKey(), positionData.getPosition().getValue());
                    positionData.getS().stream().forEach(s -> sensitivityStreamer.addData(s.getKey(), s.getValue()));
                    return null;
                }
            }));
        }


        for (Future<Void> fut : futs)
            fut.get();
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        if (executor != null)
            executor.shutdown();

        positionStreamer.close();
        sensitivityStreamer.close();

        super.tearDown();
    }

    /** */
    public void executeQuery(String query) {
        List all = cache.query(new SqlFieldsQuery(query)).getAll();
    }
}

