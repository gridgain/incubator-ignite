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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.SEQUENTIAL_CP_PAGE_COMPARATOR;

/**
 * Asynchronous Checkpointer, encapsulates thread pool functionality and allows
 */
public class AsyncCheckpointer {
    /** Checkpoint runner thread name prefix. */
    public static final String CHECKPOINT_RUNNER = "checkpoint-runner";

    /** Checkpoint runner thread pool. If null tasks are to be run in single thread */
    @Nullable private ThreadPoolExecutor asyncRunner;

    /**  Number of checkpoint threads. */
    private int checkpointThreads;

    /** Logger. */
    private IgniteLogger log;

    /**
     * @param checkpointThreads Number of checkpoint threads.
     * @param igniteInstanceName Ignite instance name.
     * @param log Logger.
     */
    public AsyncCheckpointer(int checkpointThreads, String igniteInstanceName, IgniteLogger log) {
        this.checkpointThreads = checkpointThreads;
        this.log = log;

        asyncRunner = new IgniteThreadPoolExecutor(
            CHECKPOINT_RUNNER,
            igniteInstanceName,
            checkpointThreads,
            checkpointThreads,
            30_000,
            new LinkedBlockingQueue<Runnable>()
        );
    }

    /**
     * Close async checkpointer, stops all thread from pool
     */
    public void shutdownCheckpointer() {
        asyncRunner.shutdownNow();

        try {
            asyncRunner.awaitTermination(2, TimeUnit.MINUTES);
        }
        catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Executes the given runnable in thread pool.
     *
     * @param runnable task to run.
     */
    private void execute(Runnable runnable) {
        try {
            asyncRunner.execute(runnable);
        }
        catch (RejectedExecutionException ignore) {
            // Run the task synchronously.
            runnable.run();
        }
    }

    /**
     * Executes the given runnable in thread pool.
     *
     * @param task task to run.
     * @param doneReportFut Count down future to report this runnable completion.
     */
    public void execute(Callable<Void> task, CountDownFuture doneReportFut) {
        execute(wrapRunnableWithDoneReporting(task, doneReportFut));
    }

    /**
     * @param task actual callable performing required action.
     * @param doneReportFut Count down future to report this runnable completion.
     * @return wrapper runnable which will report result to {@code doneReportFut}
     */
    private static Runnable wrapRunnableWithDoneReporting(final Callable<Void> task,
        final CountDownFuture doneReportFut) {
        return new Runnable() {
            @Override public void run() {
                try {
                    task.call();

                    doneReportFut.onDone((Void)null); // success
                }
                catch (Throwable t) {
                    doneReportFut.onDone(t); //reporting error
                }
            }
        };
    }

    /**
     * @param cpScope Checkpoint scope, contains unsorted collections.
     * @param taskFactory write pages task factory. Should provide callable to write given pages array.
     * @return future will be completed when background writing is done.
     */
    public CountDownFuture quickSortAndWritePages(CheckpointScope cpScope,
        final IgniteClosure<FullPageId[], Callable<Void>> taskFactory) {
        return quickSortAndWritePages(cpScope.toBuffer(), taskFactory);
    }

    /**
     * @param pageIds Checkpoint scope, contains unsorted collections.
     * @param taskFactory write pages task factory. Should provide callable to write given pages array.
     * @return future will be completed when background writing is done.
     */
    public CountDownFuture quickSortAndWritePages(FullPageIdsBuffer pageIds,
        final IgniteClosure<FullPageId[], Callable<Void>> taskFactory) {

        // init counter 1 protects here from premature completing
        final CountDownDynamicFuture cntDownDynamicFut = new CountDownDynamicFuture(1);

        //shared counter with running task from factory (payload)
        final AtomicInteger runningWriters = new AtomicInteger();

        Callable<Void> task = new QuickSortRecursiveTask(pageIds,
            SEQUENTIAL_CP_PAGE_COMPARATOR,
            wrapFactoryForCounting(taskFactory, runningWriters),
            new IgniteInClosure<Callable<Void>>() {
                @Override public void apply(Callable<Void> call) {
                    fork(call, cntDownDynamicFut);
                }
            },
            checkpointThreads,
            new ForkNowForkLaterStrategy() {
                @Override public boolean forkNow() {
                    if (asyncRunner.getActiveCount() < checkpointThreads) {
                        if (log.isTraceEnabled())
                            log.trace("Need to fill pool by computing tasks, fork now");

                        return true; // need to fill the pool
                    }

                    if(runningWriters.get() < checkpointThreads / 2) {
                        if (log.isTraceEnabled())
                            log.trace("Need to give a priority to payload tasks, fork later");

                        return false; // low writers, need to provide priority to writers and avoid forking
                    }

                    // fork later for this case, pool is busy, and not sufficient checkpoint writers running
                    return true;
                }
            });

        fork(task, cntDownDynamicFut);

        cntDownDynamicFut.onDone((Void)null); //submit of all tasks completed

        return cntDownDynamicFut;
    }

    /**
     * @param taskFactory task factory producing tasks to track.
     * @param runningWriters shared counter.
     * @return wrapped factory which allows to count running tasks.
     */
    private IgniteClosure<FullPageId[], Callable<Void>> wrapFactoryForCounting(
        final IgniteClosure<FullPageId[], Callable<Void>> taskFactory, final AtomicInteger runningWriters) {

        return new IgniteClosure<FullPageId[], Callable<Void>>() {
            @Override public Callable<Void> apply(FullPageId[] ids) {
                return wrapCallableForCounting(taskFactory.apply(ids), runningWriters);
            }
        };
    }

    /**
     * @param task payload task.
     * @param runningWriters counter.
     * @return wrapped callable which perform runs counting
     */
    @NotNull private Callable<Void> wrapCallableForCounting(final Callable<Void> task,
        final AtomicInteger runningWriters) {

        return new Callable<Void>() {
            @Override public Void call() throws Exception {
                try {
                    runningWriters.incrementAndGet();

                    return task.call();
                }
                finally {
                    runningWriters.decrementAndGet();
                }
            }
        };
    }

    /**
     * Executes the given runnable in thread pool, registers future to be waited.
     *
     * @param task task to run.
     * @param cntDownDynamicFut Count down future to register job and then report this runnable completion.
     */
    private void fork(Callable<Void> task, CountDownDynamicFuture cntDownDynamicFut) {
        cntDownDynamicFut.incrementTasksCount(); // for created task about to be forked

        execute(task, cntDownDynamicFut);
    }
}
