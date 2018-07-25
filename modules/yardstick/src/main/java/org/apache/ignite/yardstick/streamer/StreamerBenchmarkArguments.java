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

import com.beust.jcommander.Parameter;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.DataStorageConfiguration;

/**
 *
 */
public class StreamerBenchmarkArguments {
    @Parameter(names = {"-rkp", "--repeatingKeysPercent"}, description = "Repeating keys percent")
    private double repeatingKeysPercent = 0.0;

    @Parameter(names = {"-rcv", "--useReceiver"}, description = "Enable stream receiver")
    private boolean useReceiver = false;

    @Parameter(names = {"-alow", "--allowOverwrite"}, description = "Enable allowOverwrite mode")
    private boolean allowOverwrite = false;

    @Parameter(names = {"-cbs", "--checkpointBufferSize"}, description = "Checkpoint buffer size")
    private long checkpointBufferSize = 0;

    @Parameter(names = {"-cfrq", "--checkpointFrequency"}, description = "Checkpoint frequency")
    private long checkpointFrequency = DataStorageConfiguration.DFLT_CHECKPOINT_FREQ;

    @Parameter(names = {"-cthr", "--checkpointThreads"}, description = "Checkpoint threads")
    private int checkpointThreads = DataStorageConfiguration.DFLT_CHECKPOINT_THREADS;

    @Parameter(names = {"-inl", "--inlineSize"}, description = "Index inline size")
    private int inlineSize = QueryIndex.DFLT_INLINE_SIZE;

    @Parameter(names = {"-drsz", "--dataRegionSize"}, description = "Data region size")
    private long dataRegionSize = DataStorageConfiguration.DFLT_DATA_REGION_MAX_SIZE;

    @Parameter(names = {"-pld", "--payloadSize"}, description = "Payload size")
    private int payloadSize = 256;

    public double repeatingKeysPercent() {
        return repeatingKeysPercent;
    }

    public boolean useReceiver() {
        return useReceiver;
    }

    public boolean allowOverwrite() {
        return allowOverwrite;
    }

    public long checkpointBufferSize() {
        return checkpointBufferSize;
    }

    public long checkpointFrequency() {
        return checkpointFrequency;
    }

    public int checkpointThreads() {
        return checkpointThreads;
    }

    public int inlineSize() {
        return inlineSize;
    }

    public long dataRegionSize() {
        return dataRegionSize;
    }

    public int payloadSize() {
        return payloadSize;
    }
}
