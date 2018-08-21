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

package org.apache.ignite.yardstick.upload;

import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.IgniteDataStreamer;

/**
 * Benchmark that performs single upload of number of entries using {@link IgniteDataStreamer}.
 */
public class IntegerStreamerBenchmark extends AbstractNativeBenchmark {
    /**
     * Uploads randomly generated entries to specified cache.
     *
     * @param cacheName - name of the cache.
     * @param insertsCnt - how many entries should be uploaded.
     */
    @Override protected void upload(String cacheName, long insertsCnt) {
        try (IgniteDataStreamer<Integer, Integer> streamer = getStreamer(cacheName)) {
            Integer batchSize = args.upload.streamerLocalBatchSize();

            // IgniteDataStreamer.addData(Object, Object) has known performance issue,
            // so we have an option to work it around.
            if (batchSize == null) {
                for (int i = 1; i <= insertsCnt; i++) {
                    int key = nextRandom(args.range());

                    streamer.addData(key, key);
                }
            }
            else {
                Map<Integer, Integer> buf = new TreeMap<>();

                for (int i = 1; i <= insertsCnt; i++) {
                    int key = nextRandom(args.range());

                    buf.put(key, key);

                    if (i > 0 && i % batchSize == 0 || i == insertsCnt) {
                        streamer.addData(buf);

                        buf.clear();
                    }
                }
            }
        }
    }

    /**
     * @param cacheName Cache name.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return {@link IgniteDataStreamer} instance for the specified cache.
     */
    private <K, V> IgniteDataStreamer<K, V> getStreamer(String cacheName) {
        IgniteDataStreamer<K, V> streamer = ignite().dataStreamer(cacheName);

        if (args.upload.streamerPerNodeBufferSize() != null)
            streamer.perNodeBufferSize(args.upload.streamerPerNodeBufferSize());

        if (args.upload.streamerPerNodeParallelOperations() != null)
            streamer.perNodeParallelOperations(args.upload.streamerPerNodeParallelOperations());

        if (args.upload.streamerAllowOverwrite() != null)
            streamer.allowOverwrite(args.upload.streamerAllowOverwrite());

        return streamer;
    }
}
