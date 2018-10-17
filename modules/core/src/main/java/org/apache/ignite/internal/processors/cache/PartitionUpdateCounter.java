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

package org.apache.ignite.internal.processors.cache;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Partition update counter with MVCC delta updates capabilities.
 */
public class PartitionUpdateCounter {
    public static final String METASTORE_PARTITION_UPDATE_COUNTER_KEY = "partitionUpdateCntrKey";
    /** */
    private IgniteLogger log;

    /** Queue of counter update tasks*/
    private final Queue<Item> queue = new PriorityQueue<>();

    /** Counter. */
    private final AtomicLong cntr = new AtomicLong();

    /** Initial counter. */
    private long initCntr;

    /** For debugging purposes only. */
    private final GridCacheSharedContext ctx;

    /** Cache group id. */
    private final int grpId;

    /** Partition id. */
    private final int partId;

    /**
     * @param log Logger.
     * @param ctx Cache shared context
     */
    PartitionUpdateCounter(IgniteLogger log, GridCacheSharedContext ctx, int grpId, int partId) {
        this.log = log;
        this.ctx = ctx;
        this.grpId = grpId;
        this.partId = partId;
    }

    /**
     * Sets init counter.
     *
     * @param updateCntr Init counter valus.
     */
    public void init(long updateCntr) {
        initCntr = updateCntr;

        cntr.set(updateCntr);

        logCounterUpdate(updateCntr, -1);
    }

    /**
     * @return Initial counter value.
     */
    public long initial() {
        return initCntr;
    }

    /**
     * @return Current update counter value.
     */
    public long get() {
        return cntr.get();
    }

    /**
     * Adds delta to current counter value.
     *
     * @param delta Delta.
     * @return Value before add.
     */
    public long getAndAdd(long delta) {
        long ret = cntr.getAndAdd(delta);

        logCounterUpdate(ret, delta);

        return ret;
    }

    /**
     * @return Next update counter.
     */
    public long next() {
        long ret = cntr.incrementAndGet();

        logCounterUpdate(ret, 1);

        return ret;
    }

    /**
     * Sets value to update counter.
     *
     * @param val Values.
     */
    public void update(long val) {
        while (true) {
            long val0 = cntr.get();

            if (val0 >= val)
                break;

            if (cntr.compareAndSet(val0, val)) {
                logCounterUpdate(val, 0);

                break;
            }
        }
    }

    /**
     * Updates counter by delta from start position.
     *
     * @param start Start.
     * @param delta Delta.
     */
    public synchronized void update(long start, long delta) {
        long cur = cntr.get(), next;

        if (cur > start) {
            log.warning("Stale update counter task [cur=" + cur + ", start=" + start + ", delta=" + delta + ']');

            return;
        }

        if (cur < start) {
            // backup node with gaps
            offer(new Item(start, delta));

            return;
        }

        while (true) {
            boolean res = cntr.compareAndSet(cur, next = start + delta);

            assert res;

            logCounterUpdate(next, delta);

            Item peek = peek();

            if (peek == null || peek.start != next)
                return;

            Item item = poll();

            assert peek == item;

            start = item.start;
            delta = item.delta;
            cur = next;
        }
    }

    /**
     * @param cntr Sets initial counter.
     */
    public void updateInitial(long cntr) {
        if (get() < cntr)
            update(cntr);

        initCntr = cntr;
    }

    private void logCounterUpdate(long newVal, long delta) {
        ctx.database().checkpointReadLock();

        try {
            if (ctx.wal() != null) {
                ctx.wal().log(new MetastoreDataRecord(
                    METASTORE_PARTITION_UPDATE_COUNTER_KEY,
                    ctx.kernalContext().marshallerContext().jdkMarshaller().marshal(
                        PartitionCounterUpdateLog.create(grpId, partId, newVal, delta))));
            }
        }
        catch (IgniteCheckedException e) {
            log.error("failed to store partition update record", e);
        }
        finally {
            ctx.database().checkpointReadUnlock();
        }
    }

    /**
     * @return Retrieves the minimum update counter task from queue.
     */
    private Item poll() {
        return queue.poll();
    }

    /**
     * @return Checks the minimum update counter task from queue.
     */
    private Item peek() {
        return queue.peek();
    }

    /**
     * @param item Adds update task to priority queue.
     */
    private void offer(Item item) {
        queue.offer(item);
    }

    /**
     * Update counter task. Update from start value by delta value.
     */
    private static class Item implements Comparable<Item> {
        /** */
        private final long start;

        /** */
        private final long delta;

        /**
         * @param start Start value.
         * @param delta Delta value.
         */
        private Item(long start, long delta) {
            this.start = start;
            this.delta = delta;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull Item o) {
            int cmp = Long.compare(this.start, o.start);

            assert cmp != 0;

            return cmp;
        }
    }

    public static class PartitionCounterUpdateLog implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private String threadName;

        private String stackTrace;

        private long val;

        private long delta;

        private int grpId;

        private int partId;

        public PartitionCounterUpdateLog() {
        }

        public PartitionCounterUpdateLog(int grpId, int partId, long val, long delta, String threadName, String stackTrace) {
            this.grpId = grpId;
            this.partId = partId;
            this.val = val;
            this.delta = delta;
            this.threadName = threadName;
            this.stackTrace = stackTrace;
        }

        public String threadName() {
            return threadName;
        }

        public String stackTrace() {
            return stackTrace;
        }

        public long val() {
            return val;
        }

        public long delta() {
            return delta;
        }

        public int grpId() {
            return grpId;
        }

        public int partId() {
            return partId;
        }

        public static PartitionCounterUpdateLog create(int grpId, int partId, long val, long delta) {
            Exception e = new RuntimeException("PartitionUpdateCounterLogMessage");

            StringWriter wr = new StringWriter();

            e.printStackTrace(new PrintWriter(wr));

            return new PartitionCounterUpdateLog(grpId, partId, val, delta, Thread.currentThread().getName(), wr.toString());
        }

        @Override public String toString() {
            return S.toString(PartitionCounterUpdateLog.class, this);
        }
    }
}
