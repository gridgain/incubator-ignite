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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;

/**
 *
 */
public final class CountersMap {
    /** Initial capacity. */
    public static final int INITIAL_CAPACITY = 8;

    /** Maximum capacity. */
    public static final int MAXIMUM_CAPACITY = 1 << 30;

    /** Magic hash mixer. */
    private static final int MAGIC_HASH_MIXER = 0x9E3779B9;

    /** Array load percentage before resize. */
    private static final float SCALE_LOAD_FACTOR = 0.7F;
    public static final LongPredicate NOT_NULL = entry0 -> entry0 != 0;

    /** Scale threshold. */
    private int scaleThreshold;

    /** Entries. */
    private Entry[] entries;

    /** Count of elements in Map. */
    private int size;

    /** */
    private static final class EntryConsumer implements Consumer<Entry>, LongConsumer {
        private final TriIntConsumer act;

        private int cacheId;

        private EntryConsumer(TriIntConsumer act) {
            this.act = act;
        }

        @Override public void accept(Entry entry) {
            cacheId = entry.cacheId;

            Arrays.stream(entry.data).filter(NOT_NULL).forEach(this);
        }

        @Override public void accept(long entry) {
            act.accept(cacheId, part(entry), counter(entry));
        }
    }

    private static final class Entry {
        /** */
        private final int cacheId;

        /** Scale threshold. */
        private int scaleThreshold;

        /** Count of elements in Map. */
        private int size;

        /** data array. */
        private long[] data;

        /** Default constructor. */
        private Entry(int cacheId) {
            scaleThreshold = (int)(INITIAL_CAPACITY * SCALE_LOAD_FACTOR);

            data = new long[INITIAL_CAPACITY];

            this.cacheId = cacheId;
        }

        private void increment(int part) {
            applyDelta(part, 1);
        }

        private void decrement(int part) {
            applyDelta(part, -1);
        }

        private void applyDelta(int part, int delta) {
            int idx = find(part);

            if (idx < 0)
                put0(entry(part, delta));
            else
                data[idx] = entry(part, counter(data[idx] + delta));
        }

        private void put0(long entry) {
            if (size >= scaleThreshold)
                resize();

            int tabLen = data.length;

            long savedEntry = entry;

            int startKey = part(savedEntry);

            for (int i = 0; i < tabLen; i++) {
                int idx = (index(startKey, tabLen) + i) & (tabLen - 1);

                long curEntry = data[idx];

                if (curEntry == 0) {
                    data[idx] = savedEntry;

                    size++;

                    return;
                }
                else if ((curEntry ^ savedEntry) >>> 32 == 0) {
                    data[idx] = savedEntry;

                    return;
                }

                int curDist = distance(idx, part(curEntry), tabLen);
                int savedDist = distance(idx, part(savedEntry), tabLen);

                if (curDist < savedDist) {
                    data[idx] = savedEntry;

                    savedEntry = curEntry;
                }
            }

            throw new AssertionError("Unreachable state exception. Insertion position not found. " +
                "Entry: " + entry + " map state: " + toString());
        }

        private int find(int part) {
            int idx = index(part, data.length);

            for (int dist = 0; dist < data.length; dist++) {
                int curIdx = (idx + dist) & (data.length - 1);

                long entry = data[curIdx];

                if (entry == 0)
                    return -1;
                else if (part(entry) == part)
                    return curIdx;

                int entryDist = distance(curIdx, part(entry), data.length);

                if (dist > entryDist)
                    return -1;
            }

            return -1;
        }

        private void resize() {
            int tabLen = data.length;

            if (MAXIMUM_CAPACITY == tabLen)
                throw new IllegalStateException("Maximum capacity: " + MAXIMUM_CAPACITY + " is reached.");

            long[] oldEntries = data;

            data = new long[tabLen << 1];

            scaleThreshold = (int)(tabLen * SCALE_LOAD_FACTOR);

            size = 0;

            for (long entry : oldEntries)
                if (entry != 0)
                    put0(entry);
        }
    }

    @FunctionalInterface
    public interface TriIntConsumer {
        void accept(int i1, int i2, int i3);
    }

    /** */
    public CountersMap() {
        scaleThreshold = (int)(INITIAL_CAPACITY * SCALE_LOAD_FACTOR);

        entries = new Entry[INITIAL_CAPACITY];
    }

    void increment(int cacheId, int partId) {
        entry(cacheId).increment(partId);
    }

    void decrement(int cacheId, int partId) {
        entry(cacheId).decrement(partId);
    }

    void applyDelta(int cacheId, int partId, int delta) {
        entry(cacheId).applyDelta(partId, delta);
    }

    public int size() {
        return size;
    }

    void forEach(TriIntConsumer act) {
        Arrays.stream(entries).filter(Objects::nonNull).forEach(new EntryConsumer(act));
    }

    private void put0(Entry entry) {
        if (size >= scaleThreshold)
            resize();

        int tabLen = entries.length;

        Entry savedEntry = entry;

        int startKey = savedEntry.cacheId;

        for (int i = 0; i < tabLen; i++) {
            int idx = (index(startKey, tabLen) + i) & (tabLen - 1);

            Entry curEntry = entries[idx];

            if (curEntry == null) {
                entries[idx] = savedEntry;

                size++;

                return;
            }
            else if (curEntry.cacheId == savedEntry.cacheId) {
                entries[idx] = savedEntry;

                return;
            }

            int curDist = distance(idx, curEntry.cacheId, tabLen);
            int savedDist = distance(idx, savedEntry.cacheId, tabLen);

            if (curDist < savedDist) {
                entries[idx] = savedEntry;

                savedEntry = curEntry;
            }
        }

        throw new AssertionError("Unreachable state exception. Insertion position not found. " +
            "Entry: " + entry + " map state: " + toString());
    }

    private int find(int cacheId) {
        int tabLen = entries.length;

        int idx = index(cacheId, tabLen);

        for (int keyDist = 0; keyDist < tabLen; keyDist++) {
            int curIdx = (idx + keyDist) & (tabLen - 1);

            Entry entry = entries[curIdx];

            if (entry == null)
                return -1;
            else if (entry.cacheId == cacheId)
                return curIdx;

            int entryDist = distance(curIdx, entry.cacheId, tabLen);

            if (keyDist > entryDist)
                return -1;
        }

        return -1;
    }

    private void resize() {
        int tabLen = entries.length;

        if (MAXIMUM_CAPACITY == tabLen)
            throw new IllegalStateException("Maximum capacity: " + MAXIMUM_CAPACITY + " is reached.");

        Entry[] oldEntries = entries;

        entries = new Entry[tabLen << 1];

        scaleThreshold = (int)(tabLen * SCALE_LOAD_FACTOR);

        size = 0;

        for (Entry entry : oldEntries)
            if (entry != null)
                put0(entry);
    }

    private Entry entry(int cacheId) {
        Entry entry;

        int idx = find(cacheId);

        if (idx < 0)
            put0(entry = new Entry(cacheId));
        else
            entry = entries[idx];

        assert entry != null;
        return entry;
    }

    private static int distance(int curIdx, int key, int tabLen) {
        int keyIdx = index(key, tabLen);

        return curIdx >= keyIdx ? curIdx - keyIdx : tabLen - keyIdx + curIdx;
    }

    private static int index(int key, int tabLen) {
        return (tabLen - 1) & ((key ^ (key >>> 16)) * MAGIC_HASH_MIXER);
    }

    private static int part(long entry) {
        return (int) (entry >>> 32);
    }

    private static int counter(long entry) {
        return (int) entry;
    }

    private static long entry(int part, int counter) {
        return (long)part << 32 | counter;
    }
}
