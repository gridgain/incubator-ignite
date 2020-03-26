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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class CacheGroupTotalSupplierRebalanceStatistics {
    /** Start time of rebalance in milliseconds. */
    private final AtomicLong start = new AtomicLong();

    /** End time of rebalance in milliseconds. */
    private final AtomicLong end = new AtomicLong();

    /** Counter of partitions received by full rebalance. */
    private final LongAdder fullParts = new LongAdder();

    /** Counter of partitions received by historical rebalance. */
    private final LongAdder histParts = new LongAdder();

    /** Counter of entries received by full rebalance. */
    private final LongAdder fullEntries = new LongAdder();

    /** Counter of entries received by historical rebalance. */
    private final LongAdder histEntries = new LongAdder();

    /** Counter of bytes received by full rebalance. */
    private final LongAdder fullBytes = new LongAdder();

    /** Counter of bytes received by historical rebalance. */
    private final LongAdder histBytes = new LongAdder();

    /**
     * Return start time of rebalance in milliseconds.
     *
     * @return Start time of rebalance in milliseconds.
     */
    public AtomicLong start() {
        return start;
    }

    /**
     * Return end time of rebalance in milliseconds.
     *
     * @return End time of rebalance in milliseconds.
     */
    public AtomicLong end() {
        return end;
    }

    /**
     * Return counter of partitions received by full rebalance.
     *
     * @return Counter of partitions received by full rebalance.
     */
    public LongAdder fullParts() {
        return fullParts;
    }

    /**
     * Return counter of partitions received by historical rebalance.
     *
     * @return Counter of partitions received by historical rebalance.
     */
    public LongAdder histParts() {
        return histParts;
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
