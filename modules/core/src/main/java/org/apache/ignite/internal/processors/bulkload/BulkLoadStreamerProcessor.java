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

package org.apache.ignite.internal.processors.bulkload;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.internal.processors.bulkload.pipeline.PipelineBlock;
import org.apache.ignite.internal.util.lang.IgniteClosureX;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Bulk load (COPY) command processor used on server to keep various context data and process portions of input
 * received from the client side.
 */
public class BulkLoadStreamerProcessor extends BulkLoadProcessor {
    /** Streamer that puts actual key/value into the cache. */
    private final IgniteDataStreamer<Object, Object> outputStreamer;

    /** Becomes true after {@link #close()} method is called. */
    private boolean isClosed;

    private final CollectorBlock collectorBlock;

    /**
     * Creates bulk load processor.
     *
     * @param inputParser Parser of the input bytes.
     * @param dataConverter Converter, which transforms the list of strings parsed from the input stream to the
     *     key+value entry to add to the cache.
     * @param outputStreamer Streamer that puts actual key/value into the cache.
     */
    public BulkLoadStreamerProcessor(BulkLoadParser inputParser,
        IgniteClosureX<List<?>, IgniteBiTuple<?, ?>> dataConverter, IgniteDataStreamer<Object, Object> outputStreamer) {
        super(inputParser, dataConverter);

        this.outputStreamer = outputStreamer;
        isClosed = false;

        collectorBlock = new CollectorBlock();

        inputParser.collectorBlock(collectorBlock);
    }

    /**
     * Processes the incoming batch and writes data to the cache by calling the data converter and output streamer.
     *
     * @param batchData Data from the current batch.
     * @param isLastBatch true if this is the last batch.
     * @throws IgniteIllegalStateException when called after {@link #close()}.
     */
    public void processBatch(byte[] batchData, boolean isLastBatch) throws IgniteCheckedException {
        if (isClosed)
            throw new IgniteIllegalStateException("Attempt to process a batch on a closed BulkLoadProcessor");

        inputParser.parseBatch(batchData, isLastBatch);
    }

    /**
     * Aborts processing and closes the underlying objects ({@link IgniteDataStreamer}).
     */
    @Override public void close() throws Exception {
        if (isClosed)
            return;

        isClosed = true;

        collectorBlock.joinThreads();

        outputStreamer.close();
    }

    private class StreamerThread extends Thread {
        private final List<List<Object>> records;
        private int threadUpdateCnt;

        StreamerThread(List<List<Object>> records) {
            this.records = records;
            threadUpdateCnt = 0;
        }

        @Override public void run() {
            for (List<Object> entry : records) {
                IgniteBiTuple<?, ?> kv = dataConverter.apply(entry);

                outputStreamer.addData(kv.getKey(), kv.getValue());

                threadUpdateCnt++;
            }
        }

        int threadUpdateCnt() {
            return threadUpdateCnt;
        }
    }

    private class CollectorBlock extends PipelineBlock<List<Object>,Object> {
        private static final int ITEMS_PER_THREAD = 3_000;

        private List<List<Object>> input = new ArrayList<>(ITEMS_PER_THREAD);
        private List<StreamerThread> threads = new LinkedList<>();

        @Override public void accept(List<Object> inputPortion, boolean isLastPortion) throws IgniteCheckedException {
            input.add(inputPortion);

            if (input.size() >= ITEMS_PER_THREAD || isLastPortion) {
                List<List<Object>> threadInput = input;

                input = new ArrayList<>(ITEMS_PER_THREAD);

                StreamerThread thr = new StreamerThread(threadInput);

                threads.add(thr);

                thr.start();
            }
        }

        void joinThreads() throws InterruptedException {
            for (StreamerThread thr : threads) {
                thr.join();
                updateCnt += thr.threadUpdateCnt();
            }

            threads.clear();
        }
    }
}
