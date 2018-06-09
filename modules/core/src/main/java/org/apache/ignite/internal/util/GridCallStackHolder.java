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

package org.apache.ignite.internal.util;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.T4;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Holds call stacks recorded sequence. Useful for debugging.
 */
public class GridCallStackHolder {
    /** */
    private static final int MAX_RECORDED_STACKS = 100;

    /** */
    private static final DateTimeFormatter DEFAULT_DATE_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    /** */
    private static ZoneOffset DEFAULT_OFFSET = OffsetDateTime.now().getOffset();

    /** */
    private Queue<T4<Object, Long, Thread, StackTraceElement[]>> stacks = new ConcurrentLinkedQueue<>();

    /** */
    private LongAdder size = new LongAdder();

    /**
     * Logs call stack.
     *
     * @param ctx Context.
     */
    public void logStack(Object ctx) {
        // Prevent overflow.
        while(size.sum() > MAX_RECORDED_STACKS && !stacks.isEmpty()) {
            stacks.poll();

            size.decrement();
        }

        stacks.add(new T4<>(
            ctx, U.currentTimeMillis(), Thread.currentThread(), new Exception().getStackTrace()));

        size.increment();
    }

    /**
     * Consumes and prints currently recorded stacks.
     *
     * @param log Logger.
     */
    public void print(IgniteLogger log) {
        log.info(">>>>>>>>>>>>>>> Printing recorded call stacks:");

        T4<Object, Long, Thread, StackTraceElement[]> el;

        int cnt = 0;

        while((el = stacks.poll()) != null) {
            cnt--;

            log.info("    Entry: [context=" + el.get1() +
                ", time=" + LocalDateTime.ofEpochSecond(el.get2() / 1000, 0, DEFAULT_OFFSET).format(DEFAULT_DATE_FORMAT) +
                ", thread=" + el.get3().getName() + ']');

            for (StackTraceElement element : el.get4())
                log.info("        " + element);
        }

        size.add(cnt);
    }
}
