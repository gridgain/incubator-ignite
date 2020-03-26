/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/** Statistics for rebalance cache group by supplier. */
public class CacheGroupSupplierRebalanceStatistics {
    /** Start time of rebalance in milliseconds. */
    private volatile long start;

    /** End time of rebalance in milliseconds. */
    private volatile long end;

    /**
     * Rebalanced partitions.
     * Key - partition id, value - fully({@code true}) or
     * historically({@code false}) rebalanced.
     */
    private final Map<Integer, Boolean> parts = new ConcurrentHashMap<>();

    /** Counter of entries received by full rebalance. */
    private final LongAdder fullEntries = new LongAdder();

    /** Counter of entries received by historical rebalance. */
    private final LongAdder histEntries = new LongAdder();

    /** Counter of bytes received by full rebalance. */
    private final LongAdder fullBytes = new LongAdder();

    /** Counter of bytes received by historical rebalance. */
    private final LongAdder histBytes = new LongAdder();

    /**
     * Set start time of rebalance in milliseconds.
     *
     * @param start Start time of rebalance in milliseconds.
     */
    public void start(long start) {
        this.start = start;
    }

    /**
     * Return start time of rebalance in milliseconds.
     *
     * @return Start time of rebalance in milliseconds.
     */
    public long start() {
        return start;
    }

    /**
     * Set end time of rebalance in milliseconds.
     *
     * @param end End time of rebalance in milliseconds.
     */
    public void end(long end) {
        this.end = end;
    }

    /**
     * Return end time of rebalance in milliseconds.
     *
     * @return End time of rebalance in milliseconds.
     */
    public long end() {
        return end;
    }

    /**
     * Returns rebalanced partitions.
     * Key - partition id, value - fully({@code true}) or
     * historically({@code false}) rebalanced.
     *
     * @return Rebalanced partitions.
     */
    public Map<Integer, Boolean> partitions() {
        return parts;
    }

    /**
     * Return counter of entries received by full rebalance.
     *
     * @return Counter of entries received by full rebalance.
     */
    public LongAdder fullEntries() {
        return fullEntries;
    }

    /**
     * Return counter of entries received by historical rebalance.
     *
     * @return Counter of entries received by historical rebalance.
     */
    public LongAdder histEntries() {
        return histEntries;
    }

    /**
     * Return counter of bytes received by full rebalance.
     *
     * @return Counter of bytes received by full rebalance.
     */
    public LongAdder fullBytes() {
        return fullBytes;
    }

    /**
     * Return counter of bytes received by historical rebalance.
     *
     * @return Counter of bytes received by historical rebalance.
     */
    public LongAdder histBytes() {
        return histBytes;
    }
}
