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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import java.util.Optional;
import org.apache.ignite.internal.processors.cache.distributed.dht.PartitionUpdateCountersMessage;
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.internal.util.collection.IntMap;
import org.jetbrains.annotations.Nullable;

/**
 * Values which should be tracked during transaction execution and applied on commit.
 */
public final class TxCounters {
    /** Per-partition update counter accumulator. */
    private final IntMap<IntMap<MutableInt>> updCntrsAcc = new IntHashMap<>();

    /** Final update counters for cache partitions in the end of transaction */
    private IntMap<PartitionUpdateCountersMessage> updCntrs;

    private static IntMap<MutableInt> newMap(int k) {
        return new IntHashMap<>();
    }

    private static MutableInt newInt(int k) {
        return new MutableInt();
    }

    /**
     * @param updCntrs Final update counters.
     */
    public void updateCounters(Collection<PartitionUpdateCountersMessage> updCntrs) {
        this.updCntrs = new IntHashMap<>(updCntrs.size());

        for (PartitionUpdateCountersMessage cntr : updCntrs)
            this.updCntrs.put(cntr.cacheId(), cntr);
    }

    /**
     * @return Final update counters.
     */
    @Nullable public Collection<PartitionUpdateCountersMessage> updateCounters() {
        return updCntrs == null ? null : updCntrs.values();
    }

    /**
     * @return Accumulated update counters.
     */
    public IntMap<IntMap<MutableInt>> accumulatedUpdateCounters() {
        return updCntrsAcc;
    }

    /**
     * @param cacheId Cache id.
     * @param part Partition number.
     */
    public void incrementUpdateCounter(int cacheId, int part) {
        accumulator(updCntrsAcc, cacheId, part).increment();
    }

    /**
     * @param cacheId Cache id.
     * @param part Partition number.
     */
    public void decrementUpdateCounter(int cacheId, int part) {
        accumulator(updCntrsAcc, cacheId, part).decrement();
    }

    public Collection<PartitionUpdateCountersMessage> calculateCounters() {
        return updCntrs == null ? null : updCntrs.values();
    }

    /**
     * @param accMap Map to obtain accumulator from.
     * @param cacheId Cache id.
     * @param part Partition number.
     * @return Accumulator.
     */
    private MutableInt accumulator(IntMap<IntMap<MutableInt>> accMap, int cacheId, int part) {
        IntMap<MutableInt> cacheAccs = accMap.computeIfAbsent(cacheId, TxCounters::newMap);

        return cacheAccs.computeIfAbsent(part, TxCounters::newInt);
    }

    /**
     * @param cacheId Cache id.
     * @param partId Partition id.
     *
     * @return Counter or {@code null} if cache partition has not updates.
     */
    public Long generateNextCounter(int cacheId, int partId) {
        return Optional.ofNullable(updCntrs.get(cacheId))
            .map(m -> m.nextCounter(partId))
            .orElse(null);
    }

    public final static class MutableInt {
        private int val;

        int get() {
            return val;
        }

        void increment() {
            val++;
        }

        void decrement() {
            val--;
        }

        void applyDelta(int delta) {
            val+=delta;
        }
    }
}
