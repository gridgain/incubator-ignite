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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerListener;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;

/**
 * Striped executor.
 */
public class StripedExecutor implements ExecutorService {
    /** Stripes. */
    private final Stripe[] stripes;

    /** For starvation checks. */
    private final long[] completedCntrs;

    /** */
    private final IgniteLogger log;

    /**
     * @param cnt Count.
     * @param igniteInstanceName Node name.
     * @param poolName Pool name.
     * @param log Logger.
     * @param errHnd Critical failure handler.
     * @param gridWorkerLsnr listener to link with every stripe worker.
     */
    public StripedExecutor(
        int cnt,
        String igniteInstanceName,
        String poolName,
        final IgniteLogger log,
        IgniteInClosure<Throwable> errHnd,
        GridWorkerListener gridWorkerLsnr
    ) {
        this(cnt, igniteInstanceName, poolName, log, errHnd, false, gridWorkerLsnr);
    }

    /**
     * @param cnt Count.
     * @param igniteInstanceName Node name.
     * @param poolName Pool name.
     * @param log Logger.
     * @param errHnd Critical failure handler.
     * @param stealTasks {@code True} to steal tasks.
     * @param gridWorkerLsnr listener to link with every stripe worker.
     */
    public StripedExecutor(
        int cnt,
        String igniteInstanceName,
        String poolName,
        final IgniteLogger log,
        IgniteInClosure<Throwable> errHnd,
        boolean stealTasks,
        GridWorkerListener gridWorkerLsnr
    ) {
        A.ensure(cnt > 0, "cnt > 0");

        boolean success = false;

        stripes = new Stripe[cnt];

        completedCntrs = new long[cnt];

        Arrays.fill(completedCntrs, -1);

        this.log = log;

        try {
            for (int i = 0; i < cnt; i++) {
                stripes[i] = stealTasks
                    ? new StripeConcurrentQueue(igniteInstanceName, poolName, i, log, stripes, errHnd, gridWorkerLsnr)
                    : new StripeConcurrentQueue(igniteInstanceName, poolName, i, log, errHnd, gridWorkerLsnr);
            }

            for (int i = 0; i < cnt; i++)
                stripes[i].start();

            success = true;
        }
        catch (Error | RuntimeException e) {
            U.error(log, "Failed to initialize striped pool.", e);

            throw e;
        }
        finally {
            if (!success) {
                for (Stripe stripe : stripes)
                    U.cancel(stripe);

                for (Stripe stripe : stripes)
                    U.join(stripe, log);
            }
        }
    }

    /**
     * Checks starvation in striped pool. Maybe too verbose
     * but this is needed to faster debug possible issues.
     */
    public void checkStarvation() {
        for (int i = 0; i < stripes.length; i++) {
            Stripe stripe = stripes[i];

            long completedCnt = stripe.completedCnt;

            boolean active = stripe.active;

            if (completedCntrs[i] != -1 &&
                completedCntrs[i] == completedCnt &&
                active) {
                boolean deadlockPresent = U.deadlockPresent();

                GridStringBuilder sb = new GridStringBuilder();

                sb.a(">>> Possible starvation in striped pool.").a(U.nl())
                    .a("    Thread name: ").a(stripe.thread.getName()).a(U.nl())
                    .a("    Queue: ").a(stripe.queueToString()).a(U.nl())
                    .a("    Deadlock: ").a(deadlockPresent).a(U.nl())
                    .a("    Completed: ").a(completedCnt).a(U.nl());

                U.printStackTrace(
                    stripe.thread.getId(),
                    sb);

                String msg = sb.toString();

                U.warn(log, msg);
            }

            if (active || completedCnt > 0)
                completedCntrs[i] = completedCnt;
        }
    }

    /**
     *
     */
    public void checkContention() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        ZoneId sysZoneId = ZoneId.systemDefault();

        for (int i = 0; i < stripes.length; i++) {
            Stripe stripe = stripes[i];

            if (!stripe.contHist.isEmpty() || !stripe.taskHist.isEmpty()) {
                GridStringBuilder sb = new GridStringBuilder();

                sb.a(">>> Possible contention in striped pool.").a(U.nl())
                        .a("    Thread name: ").a(stripe.thread.getName()).a(U.nl());

                stripe.contHist.descendingMap().values().stream().limit(5)
                        .forEach((snaps) -> {
                            for (StripeSnap snap: snaps)
                                sb.a("  ").a(LocalDateTime.ofInstant(
                                        Instant.ofEpochMilli(snap.timestamp()),
                                        sysZoneId).format(formatter))
                                    .a(" ")
                                    .a("queue size: ").a(snap.queueSize()).a(" ")
                                    .a("runnables: ").a(snap.message()).a(U.nl());
                });

                stripe.taskHist.descendingMap().values().stream().limit(5)
                        .forEach(snap -> {
                                sb.a("  ").a(LocalDateTime.ofInstant(
                                            Instant.ofEpochMilli(snap.timestamp()),
                                            sysZoneId).format(formatter))
                                        .a(" ")
                                        .a("task duration: ").a(TimeUnit.NANOSECONDS.toMillis(snap.duration())).a("ms ")
                                        .a("runnable: ").a(snap.message()).a(U.nl());
                });

                String msg = sb.toString();

                U.warn(log, msg);

                U.printStackTrace(
                        stripe.thread.getId(),
                        sb);

                stripe.contHist.clear();

                stripe.taskHist.clear();
            }
        }
    }

    /**
     * @return Stripes count.
     */
    public int stripes() {
        return stripes.length;
    }

    /**
     * Execute command.
     *
     * @param idx Index.
     * @param cmd Command.
     */
    public void execute(int idx, Runnable cmd) {
        if (idx == -1)
            execute(cmd);
        else {
            assert idx >= 0 : idx;

            stripes[idx % stripes.length].execute(cmd);
        }
    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
        signalStop();
    }

    /** {@inheritDoc} */
    @Override public void execute(@NotNull Runnable cmd) {
        stripes[ThreadLocalRandom.current().nextInt(stripes.length)].execute(cmd);
    }

    /**
     * {@inheritDoc}
     *
     * @return Empty list (always).
     */
    @NotNull @Override public List<Runnable> shutdownNow() {
        signalStop();

        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public boolean awaitTermination(
        long timeout,
        @NotNull TimeUnit unit
    ) throws InterruptedException {
        awaitStop();

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isShutdown() {
        for (Stripe stripe : stripes) {
            if (stripe != null && stripe.isCancelled())
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isTerminated() {
        for (Stripe stripe : stripes) {
            if (stripe.thread.getState() != Thread.State.TERMINATED)
                return false;
        }

        return true;
    }

    /**
     * Stops executor.
     */
    public void stop() {
        signalStop();

        awaitStop();
    }

    /**
     * Signals all stripes.
     */
    private void signalStop() {
        for (Stripe stripe : stripes)
            U.cancel(stripe);
    }

    /**
     * Waits for all stripes to stop.
     */
    private void awaitStop() {
        for (Stripe stripe : stripes)
            U.join(stripe, log);
    }

    /**
     * @return Return total queue size of all stripes.
     */
    public int queueSize() {
        int size = 0;

        for (Stripe stripe : stripes)
            size += stripe.queueSize();

        return size;
    }

    /**
     * @return Completed tasks count.
     */
    public long completedTasks() {
        long cnt = 0;

        for (Stripe stripe : stripes)
            cnt += stripe.completedCnt;

        return cnt;
    }

    /**
     * @return Completed tasks per stripe count.
     */
    public long[] stripesCompletedTasks() {
        long[] res = new long[stripes()];

        for (int i = 0; i < res.length; i++)
            res[i] = stripes[i].completedCnt;

        return res;
    }

    /**
     * @return Number of active tasks per stripe.
     */
    public boolean[] stripesActiveStatuses() {
        boolean[] res = new boolean[stripes()];

        for (int i = 0; i < res.length; i++)
            res[i] = stripes[i].active;

        return res;
    }

    /**
     * @return Number of active tasks.
     */
    public int activeStripesCount() {
        int res = 0;

        for (boolean status : stripesActiveStatuses()) {
            if (status)
                res++;
        }

        return res;
    }

    /**
     * @return Size of queue per stripe.
     */
    public int[] stripesQueueSizes() {
        int[] res = new int[stripes()];

        for (int i = 0; i < res.length; i++)
            res[i] = stripes[i].queueSize();

        return res;
    }

    /**
     * Operation not supported.
     */
    @NotNull @Override public <T> Future<T> submit(
        @NotNull Runnable task,
        T res
    ) {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation not supported.
     */
    @NotNull @Override public Future<?> submit(@NotNull Runnable task) {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation not supported.
     */
    @NotNull @Override public <T> Future<T> submit(@NotNull Callable<T> task) {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation not supported.
     */
    @NotNull @Override public <T> List<Future<T>> invokeAll(@NotNull Collection<? extends Callable<T>> tasks)
        throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation not supported.
     */
    @NotNull @Override public <T> List<Future<T>> invokeAll(
        @NotNull Collection<? extends Callable<T>> tasks,
        long timeout,
        @NotNull TimeUnit unit
    ) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation not supported.
     */
    @NotNull @Override public <T> T invokeAny(@NotNull Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException();
    }

    /**
     * Operation not supported.
     */
    @Override public <T> T invokeAny(
        @NotNull Collection<? extends Callable<T>> tasks,
        long timeout,
        @NotNull TimeUnit unit
    ) throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StripedExecutor.class, this);
    }

    /**
     *
     */
    private static class StripeSnap {
        /** */
        long ts;

        /** */
        int queueSize;

        /** */
        String msg;

        /**
         * @param ts Timestamp.
         * @param queueSize Queue size.
         * @param msg Message.
         */
        StripeSnap(long ts, int queueSize, String msg) {
            this.ts = ts;
            this.queueSize = queueSize;
            this.msg = msg;
        }

        /**
         * @return Timestamp.
         */
        public long timestamp() {
            return ts;
        }

        /**
         * @return Queue size.
         */
        public int queueSize() {
            return queueSize;
        }

        /**
         * @return Message.
         */
        public String message() {
            return msg;
        }
    }

    /**
     *
     */
    private static class StripeTaskSnap {
        /** */
        long ts;

        /** */
        long duration;

        /** */
        String msg;

        /**
         * @param ts Timestamp.
         * @param duration In nanos.
         * @param msg Message.
         */
        StripeTaskSnap(long ts, long duration, String msg) {
            this.ts = ts;
            this.duration = duration;
            this.msg = msg;
        }

        /**
         * @return Timestamp.
         */
        public long timestamp() {
            return ts;
        }

        /**
         * @return Duration in nanos.
         */
        public long duration() {
            return duration;
        }

        /**
         * @return Message.
         */
        public String message() {
            return msg;
        }
    }

    /**
     * Stripe.
     */
    private static abstract class Stripe extends GridWorker {
        private static final int IGNITE_STRIPE_QUEUE_THRESHOLD =
                IgniteSystemProperties.getInteger(
                        IgniteSystemProperties.IGNITE_STRIPE_QUEUE_THRESHOLD, 10);

        private static final int IGNITE_STRIPE_MAX_CONTENTION_HISTORY_SIZE =
                IgniteSystemProperties.getInteger(
                        IgniteSystemProperties.IGNITE_STRIPE_MAX_CONTENTION_HISTORY_SIZE, 1000);

        private static final long IGNITE_STRIPE_TASK_LONG_EXECUTION_THRESHOLD =
                IgniteSystemProperties.getInteger(
                        IgniteSystemProperties.IGNITE_STRIPE_TASK_LONG_EXECUTION_THRESHOLD, 1000) * 1_000_000L;

        private static final long IGNITE_STRIPE_HOT_KEYS_MAX_SIZE =
            IgniteSystemProperties.getInteger(
                IgniteSystemProperties.IGNITE_STRIPE_HOT_KEYS_MAX_SIZE, 10_000);

        /** */
        ConcurrentNavigableMap<Integer, List<StripeSnap>> contHist = new ConcurrentSkipListMap<>();

        /** */
        ConcurrentNavigableMap<Long, StripeTaskSnap> taskHist = new ConcurrentSkipListMap<>();

        /** Stripe stats. */
        Map<Class<? extends Message>, MessageStat> statMap = new HashMap<>();

        /** */
        private final String igniteInstanceName;

        /** */
        protected final int idx;

        /** */
        private final IgniteLogger log;

        /** */
        private volatile long completedCnt;

        /** */
        private volatile boolean active;

        /** Thread executing the loop. */
        protected Thread thread;

        /** Critical failure handler. */
        private IgniteInClosure<Throwable> errHnd;

        /**
         * @param igniteInstanceName Ignite instance name.
         * @param poolName Pool name.
         * @param idx Stripe index.
         * @param log Logger.
         * @param errHnd Exception handler.
         * @param gridWorkerLsnr listener to link with stripe worker.
         */
        public Stripe(
            String igniteInstanceName,
            String poolName,
            int idx,
            IgniteLogger log,
            IgniteInClosure<Throwable> errHnd,
            GridWorkerListener gridWorkerLsnr
        ) {
            super(igniteInstanceName, poolName + "-stripe-" + idx, log, gridWorkerLsnr);

            this.igniteInstanceName = igniteInstanceName;
            this.idx = idx;
            this.log = log;
            this.errHnd = errHnd;
        }

        /**
         * Starts the stripe.
         */
        void start() {
            thread = new IgniteThread(igniteInstanceName,
                name(),
                this,
                IgniteThread.GRP_IDX_UNASSIGNED,
                idx,
                GridIoPolicy.UNDEFINED);

            thread.start();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("NonAtomicOperationOnVolatileField")
        @Override public void body() {
            while (!isCancelled()) {
                Runnable cmd;

                try {
                    cmd = take();

                    if (cmd != null) {
                        active = true;

                        try {
                            long start = System.nanoTime();

                            cmd.run();

                            long duration = System.nanoTime() - start;

//                            if (duration >= IGNITE_STRIPE_TASK_LONG_EXECUTION_THRESHOLD) {
//                                taskHist.put(duration, new StripeTaskSnap(U.currentTimeMillis(), duration, cmd.toString()));
//
//                                if (taskHist.size() > IGNITE_STRIPE_MAX_CONTENTION_HISTORY_SIZE)
//                                    taskHist.remove(taskHist.firstKey());
//                            }

                            if (cmd instanceof GridIoManager.GridIoRunnable) {
                                GridIoManager.GridIoRunnable ioRunnable = (GridIoManager.GridIoRunnable)cmd;

                                GridIoMessage ioMsg = ioRunnable.message();

                                Message msg = ioMsg.message();

                                Class<? extends Message> msgCls = msg.getClass();

                                Map<Class<? extends Message>, MessageStat> statMap0;

                                synchronized (Stripe.this) {
                                    statMap0 = statMap;

                                    if (statMap0 == null)
                                        statMap0 = new HashMap<>();
                                }

                                MessageStat stat = statMap0.get(msgCls);

                                if (stat == null) {
                                    MessageStat tmp = new MessageStat();

                                    stat = statMap.putIfAbsent(msgCls, tmp);

                                    if (stat == null)
                                        stat = tmp;
                                }

                                stat.increment(duration);

                                if (msg instanceof GridNearSingleGetRequest) {
                                    GridNearSingleGetRequest req = (GridNearSingleGetRequest)msg;

                                    stat.addKey(req.key());
                                }
                            }
                        }
                        finally {
                            active = false;
                            completedCnt++;
                        }
                    }
                }
                catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();

                    break;
                }
                catch (Throwable e) {
                    if (e instanceof OutOfMemoryError)
                        errHnd.apply(e);

                    U.error(log, "Failed to execute runnable.", e);
                }
            }

            if (!isCancelled) {
                errHnd.apply(new IllegalStateException("Thread " + Thread.currentThread().getName() +
                    " is terminated unexpectedly"));
            }
        }

        /**
         * Execute the command.
         *
         * @param cmd Command.
         */
        void execute(Runnable cmd) {
            int qSz = queueSize();
            if (qSz >= IGNITE_STRIPE_QUEUE_THRESHOLD) {
                long snapTime = System.currentTimeMillis();

                contHist.computeIfAbsent(qSz, k -> Collections.synchronizedList(new ArrayList<>()))
                        .add(new StripeSnap(snapTime, qSz, makeSnapMessage()));

                if (contHist.size() > IGNITE_STRIPE_MAX_CONTENTION_HISTORY_SIZE)
                    contHist.remove(contHist.firstKey());
            }

            execute0(cmd);
        }

        /**
         * @param cmd Command.
         */
        abstract void execute0(Runnable cmd);

        /**
         *
         */
        abstract String makeSnapMessage();

        /**
         * @return Next runnable.
         * @throws InterruptedException If interrupted.
         */
        abstract Runnable take() throws InterruptedException;

        /**
         * @return Queue size.
         */
        abstract int queueSize();

        /**
         * @return Stripe's queue to string presentation.
         */
        abstract String queueToString();

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Stripe.class, this);
        }
    }

    /**
     * Stripe.
     */
    private static class StripeConcurrentQueue extends Stripe {
        /** */
        private static final int IGNITE_TASKS_STEALING_THRESHOLD =
            IgniteSystemProperties.getInteger(
                IgniteSystemProperties.IGNITE_DATA_STREAMING_EXECUTOR_SERVICE_TASKS_STEALING_THRESHOLD, 4);

        /** Queue. */
        private final Queue<Runnable> queue;

        /** */
        @GridToStringExclude
        private final Stripe[] others;

        /** */
        private volatile boolean parked;

        /**
         * @param igniteInstanceName Ignite instance name.
         * @param poolName Pool name.
         * @param idx Stripe index.
         * @param log Logger.
         * @param errHnd Critical failure handler.
         * @param gridWorkerLsnr listener to link with stripe worker.
         */
        StripeConcurrentQueue(
            String igniteInstanceName,
            String poolName,
            int idx,
            IgniteLogger log,
            IgniteInClosure<Throwable> errHnd,
            GridWorkerListener gridWorkerLsnr
        ) {
            this(igniteInstanceName, poolName, idx, log, null, errHnd, gridWorkerLsnr);
        }

        /**
         * @param igniteInstanceName Ignite instance name.
         * @param poolName Pool name.
         * @param idx Stripe index.
         * @param log Logger.
         * @param errHnd Critical failure handler.
         * @param gridWorkerLsnr listener to link with stripe worker.
         */
        StripeConcurrentQueue(
            String igniteInstanceName,
            String poolName,
            int idx,
            IgniteLogger log,
            Stripe[] others,
            IgniteInClosure<Throwable> errHnd,
            GridWorkerListener gridWorkerLsnr
        ) {
            super(
                igniteInstanceName,
                poolName,
                idx,
                log,
                errHnd,
                gridWorkerLsnr);

            this.others = others;

            this.queue = others == null ? new ConcurrentLinkedQueue<Runnable>() : new ConcurrentLinkedDeque<Runnable>();
        }

        /** {@inheritDoc} */
        @Override Runnable take() throws InterruptedException {
            Runnable r;

            for (int i = 0; i < 2048; i++) {
                r = queue.poll();

                if (r != null)
                    return r;
            }

            parked = true;

            try {
                for (;;) {
                    r = queue.poll();

                    if (r != null)
                        return r;

                    if(others != null) {
                        int len = others.length;
                        int init = ThreadLocalRandom.current().nextInt(len);
                        int cur = init;

                        while (true) {
                            if(cur != idx) {
                                Deque<Runnable> queue = (Deque<Runnable>) ((StripeConcurrentQueue) others[cur]).queue;

                                if(queue.size() > IGNITE_TASKS_STEALING_THRESHOLD && (r = queue.pollLast()) != null)
                                    return r;
                            }

                            if ((cur = (cur + 1) % len) == init)
                                break;
                        }
                    }

                    LockSupport.park();

                    if (Thread.interrupted())
                        throw new InterruptedException();
                }
            }
            finally {
                parked = false;
            }
        }

        /** {@inheritDoc} */
        @Override String makeSnapMessage() {
            return queue.stream().map(Runnable::toString).collect(Collectors.joining(","));
        }

        /** {@inheritDoc} */
        void execute0(Runnable cmd) {
            queue.add(cmd);

            if (parked)
                LockSupport.unpark(thread);

            if(others != null && queueSize() > IGNITE_TASKS_STEALING_THRESHOLD) {
                for (Stripe other : others) {
                    if(((StripeConcurrentQueue)other).parked)
                        LockSupport.unpark(other.thread);
                }
            }
        }

        /** {@inheritDoc} */
        @Override String queueToString() {
            return String.valueOf(queue);
        }

        /** {@inheritDoc} */
        @Override int queueSize() {
            return queue.size();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(StripeConcurrentQueue.class, this, super.toString());
        }
    }

    /**
     * Stripe.
     */
    private static class StripeConcurrentQueueNoPark extends Stripe {
        /** Queue. */
        private final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();

        /**
         * @param igniteInstanceName Ignite instance name.
         * @param poolName Pool name.
         * @param idx Stripe index.
         * @param log Logger.
         * @param errHnd Critical failure handler.
         * @param gridWorkerLsnr listener to link with stripe worker.
         */
        public StripeConcurrentQueueNoPark(
            String igniteInstanceName,
            String poolName,
            int idx,
            IgniteLogger log,
            IgniteInClosure<Throwable> errHnd,
            GridWorkerListener gridWorkerLsnr
        ) {
            super(igniteInstanceName,
                poolName,
                idx,
                log,
                errHnd,
                gridWorkerLsnr);
        }

        /** {@inheritDoc} */
        @Override Runnable take() {
            for (;;) {
                Runnable r = queue.poll();

                if (r != null)
                    return r;
            }
        }

        /** {@inheritDoc} */
        void execute0(Runnable cmd) {
            queue.add(cmd);
        }

        /** {@inheritDoc} */
        @Override String makeSnapMessage() {
            return queue.stream().map(Runnable::toString).collect(Collectors.joining(","));
        }

        /** {@inheritDoc} */
        @Override int queueSize() {
            return queue.size();
        }

        /** {@inheritDoc} */
        @Override String queueToString() {
            return String.valueOf(queue);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(StripeConcurrentQueueNoPark.class, this, super.toString());
        }
    }

    /**
     * Stripe.
     */
    private static class StripeConcurrentBlockingQueue extends Stripe {
        /** Queue. */
        private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();

        /**
         * @param igniteInstanceName Ignite instance name.
         * @param poolName Pool name.
         * @param idx Stripe index.
         * @param log Logger.
         * @param errHnd Critical failure handler.
         * @param gridWorkerLsnr listener to link with stripe worker.
         */
        public StripeConcurrentBlockingQueue(
            String igniteInstanceName,
            String poolName,
            int idx,
            IgniteLogger log,
            IgniteInClosure<Throwable> errHnd,
            GridWorkerListener gridWorkerLsnr
        ) {
            super(igniteInstanceName,
                poolName,
                idx,
                log,
                errHnd,
                gridWorkerLsnr);
        }

        /** {@inheritDoc} */
        @Override Runnable take() throws InterruptedException {
            return queue.take();
        }

        /** {@inheritDoc} */
        void execute0(Runnable cmd) {
            queue.add(cmd);
        }

        /** {@inheritDoc} */
        @Override String makeSnapMessage() {
            return queue.stream().map(Runnable::toString).collect(Collectors.joining(","));
        }

        /** {@inheritDoc} */
        @Override int queueSize() {
            return queue.size();
        }

        /** {@inheritDoc} */
        @Override String queueToString() {
            return String.valueOf(queue);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(StripeConcurrentBlockingQueue.class, this, super.toString());
        }
    }

    /** */
    private static class MessageStat {
        /** */
        final long[] buckets = new long[10];

        /** */
        final Map<KeyCacheObject, Long> hotKeys = new HashMap<>();

        /** */
        long rollingAvg;

        /** */
        long total;

        /**
         * @param duration Duration.
         */
        public void increment(long duration) {
            if (duration <= 50 * 1_000_000)
                buckets[0]++;
            else if (duration <= 100 * 1_000_000)
                buckets[1]++;
            else if (duration <= 200 * 1_000_000)
                buckets[2]++;
            else if (duration <= 400 * 1_000_000)
                buckets[3]++;
            else if (duration <= 600 * 1_000_000)
                buckets[4]++;
            else if (duration <= 800 * 1_000_000)
                buckets[5]++;
            else if (duration <= 1000 * 1_000_000)
                buckets[6]++;
            else if (duration <= 1200 * 1_000_000)
                buckets[7]++;
            else if (duration <= 1500 * 1_000_000)
                buckets[8]++;
            else
                buckets[9]++;

            rollingAvg = (rollingAvg * total + duration)/(total + 1);

            total++;
        }

        /** */
        public void merge(MessageStat dst, int n) {
            Arrays.setAll(dst.buckets, value -> dst.buckets[value] + buckets[value]);

            dst.rollingAvg = (dst.rollingAvg * (n - 1) + rollingAvg) / n;

            dst.total += total;

            // Collect hottest keys first.
            hotKeys.entrySet().stream().sorted((o1, o2) -> o2.getValue().compareTo(o1.getValue())).forEach(entry -> {
                Long cnt = dst.hotKeys.get(entry.getKey());

                if (cnt == null)
                    cnt = 0L;

                cnt += entry.getValue();

                if (dst.hotKeys.size() < Stripe.IGNITE_STRIPE_HOT_KEYS_MAX_SIZE * 5)
                    hotKeys.put(entry.getKey(), cnt);
            });
        }

        /**
         * @param key Key.
         */
        public void addKey(KeyCacheObject key) {
            if (hotKeys.size() >= Stripe.IGNITE_STRIPE_HOT_KEYS_MAX_SIZE)
                return;

            Long cnt = hotKeys.get(key);

            if (cnt == null)
                cnt = 0L;

            cnt++;

            hotKeys.put(key, cnt);
        }

        public static void main(String[] args) {
            MessageStat stat1 = new MessageStat();

            for (int i = 0; i < 10; i++)
                stat1.addKey(new KeyCacheObjectImpl("k" + i, new byte[0], 0));

            stat1.increment(50);
            stat1.increment(100);
            stat1.increment(200);

            MessageStat stat2 = new MessageStat();

            stat2.increment(100);
            stat2.increment(100);
            stat2.increment(200);

            System.out.println(stat1.rollingAvg);
            System.out.println(stat1.total);
            System.out.println(stat1.hotKeys);

            System.out.println(stat2.rollingAvg);
            System.out.println(stat2.total);

            MessageStat merge = new MessageStat();

            stat1.merge(merge, 1);
            stat2.merge(merge, 2);

            System.out.println(merge.rollingAvg);
            System.out.println(merge.total);
        }
    }

    /** Needed for correct rolling average calculation. */
    Map<Class<? extends Message>, Integer> avgCntMap = new HashMap<>();

    /** */
    private Map<Class<? extends Message>, MessageStat> mergeMap = new HashMap<>();

    /** */
    public void dumpProcessedMessagesStats() {
        GridStringBuilder sb = new GridStringBuilder();

        sb.a(">>> Processed messages statistics: [");

        for (int i = 0; i < stripes.length; i++) {
            Stripe stripe = stripes[i];

            Map<Class<? extends Message>, MessageStat> statMap0;

            synchronized (stripes[i]) {
                statMap0 = stripe.statMap;

                stripe.statMap = null;
            }

            if (statMap0 == null)
                continue;

            Set<Map.Entry<Class<? extends Message>, MessageStat>> entries = statMap0.entrySet();

            for (Map.Entry<Class<? extends Message>, MessageStat> entry : entries) {
                MessageStat res = mergeMap.get(entry.getKey());

                if (res == null) {
                    mergeMap.put(entry.getKey(), (res = new MessageStat()));

                    avgCntMap.put(entry.getKey(), 1);

                    entry.getValue().merge(res, 1);
                }
                else {
                    Integer cnt = avgCntMap.get(entry.getKey());

                    cnt++;

                    avgCntMap.put(entry.getKey(), cnt);

                    entry.getValue().merge(res, cnt);
                }
            }
        }

        List<Map.Entry<Class<? extends Message>, MessageStat>> top = mergeMap.entrySet().stream().sorted((o1,
            o2) -> Long.compare(o2.getValue().total, o1.getValue().total)).collect(Collectors.toList());

        long total = 0;

        Iterator<Map.Entry<Class<? extends Message>, MessageStat>> it = top.iterator();

        while (it.hasNext()) {
            Map.Entry<Class<? extends Message>, MessageStat> entry = it.next();

            sb.a("[msg=").a(entry.getKey().getSimpleName()).a(", total=").a(entry.getValue().total);

            sb.a(',').a(" buckets=[");

            for (int i = 0; i < entry.getValue().buckets.length; i++) {
                long t = entry.getValue().buckets[i];

                sb.a(t);

                if (i < entry.getValue().buckets.length - 1)
                    sb.a(", ");
            }

            sb.a(']');

            if (entry.getKey().equals(GridNearSingleGetRequest.class)) {
                sb.a(", hotkeys=").a(entry.getValue().hotKeys.entrySet().stream().sorted((o1,
                    o2) -> o2.getValue().compareTo(o1.getValue())).limit(10).collect(Collectors.toList()));
            }

            sb.a(']');

            total += entry.getValue().total;

            if (it.hasNext())
                sb.a(", ");
        }

        sb.a("], total=").a(total).a("]").a(U.nl());

        U.warn(log, sb.toString());
    }
}
