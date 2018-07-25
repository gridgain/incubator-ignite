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

package org.apache.ignite.yardstick.streamer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

/** */
public class IgniteCustomStreamerBenchmark extends IgniteAbstractBenchmark {
    private static final String CACHE_NAME = "default";
    private final ArrayList<CustomKey> keys = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        ignite().cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) {
        Map<CustomKey, CustomValue> entries = new HashMap<>();
        for (int i = 0; i < args.batch(); i++) {
            CustomKey key = getKey();
            CustomValue val = DataGenerator.randomValue(args.streamer.payloadSize());
            entries.put(key, val);
        }

        try (IgniteDataStreamer<CustomKey, CustomValue> dataStreamer = ignite().dataStreamer(CACHE_NAME)) {
            if (args.streamer.allowOverwrite())
                dataStreamer.allowOverwrite(true);
            if (args.streamer.useReceiver())
                dataStreamer.receiver(new CustomEntryResolver());

            entries.entrySet().parallelStream().forEach(new Consumer<Map.Entry<CustomKey, CustomValue>>() {
                @Override public void accept(Map.Entry<CustomKey, CustomValue> entry) {
                    dataStreamer.addData(entry);
                }
            });
        }

        return true;
    }

    private CustomKey getKey() {
        boolean useNewKey = ThreadLocalRandom.current().nextDouble() > args.streamer.repeatingKeysPercent()
            || keys.isEmpty();
        CustomKey key;
        if (useNewKey) {
            key = DataGenerator.randomKey();
            keys.add(key);
        }
        else
            key = keys.get(ThreadLocalRandom.current().nextInt(keys.size()));

        return key;
    }
}
