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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistry;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.processors.query.calcite.message.CalciteMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.message.TestIoManager;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME;

/**
 *
 */
@RunWith(Parameterized.class)
public class AbstractExecutionTest extends GridCommonAbstractTest {
    /** Last parameter number. */
    protected static final int LAST_PARAM_NUM = 0;

    /** Params string. */
    protected static final String PARAMS_STRING = "Execution strategy = {0}";

    /** */
    private Throwable lastE;

    /** */
    private Map<UUID, QueryTaskExecutorImpl> taskExecutors;

    /** */
    private Map<UUID, ExchangeServiceImpl> exchangeServices;

    /** */
    private Map<UUID, MailboxRegistryImpl> mailboxRegistries;

    /** */
    private List<UUID> nodes;

    /** */
    protected int nodesCnt = 3;

    /** */
    enum ExecutionStrategy {
        /** */
        FIFO {
            @Override public T2<Runnable, Integer> nextTask(Deque<T2<Runnable, Integer>> tasks) {
                return tasks.pollFirst();
            }
        },

        /** */
        LIFO {
            @Override public T2<Runnable, Integer> nextTask(Deque<T2<Runnable, Integer>> tasks) {
                return tasks.pollLast();
            }
        },

        /** */
        RANDOM {
            @Override public T2<Runnable, Integer> nextTask(Deque<T2<Runnable, Integer>> tasks) {
                return ThreadLocalRandom.current().nextBoolean() ? tasks.pollLast() : tasks.pollFirst();
            }
        };

        /**
         * Returns a next task according to the strategy.
         *
         * @param tasks Task list.
         * @return Next task.
         */
        public T2<Runnable, Integer> nextTask(Deque<T2<Runnable, Integer>> tasks) {
            throw new UnsupportedOperationException();
        }
    }

    /** */
    @Parameterized.Parameters(name = PARAMS_STRING)
    public static List<Object[]> parameters() {
        return Stream.of(ExecutionStrategy.values()).map(s -> new Object[]{s}).collect(Collectors.toList());
    }

    /** Execution direction. */
    @Parameterized.Parameter(LAST_PARAM_NUM)
    public ExecutionStrategy execStgy;

    /** */
    @Before
    public void setup() throws Exception {
        nodes = IntStream.range(0, nodesCnt)
            .mapToObj(i -> UUID.randomUUID()).collect(Collectors.toList());

        taskExecutors = new HashMap<>(nodes.size());
        exchangeServices = new HashMap<>(nodes.size());
        mailboxRegistries = new HashMap<>(nodes.size());

        TestIoManager mgr = new TestIoManager();

        for (UUID uuid : nodes) {
            GridTestKernalContext kernal = newContext();

            QueryTaskExecutorImpl taskExecutor = new QueryTaskExecutorImpl(kernal);
            taskExecutor.stripedThreadPoolExecutor(new IgniteTestStripedThreadPoolExecutor(
                execStgy,
                kernal.config().getQueryThreadPoolSize(),
                kernal.igniteInstanceName(),
                "calciteQry",
                this::handle,
                true,
                DFLT_THREAD_KEEP_ALIVE_TIME
            ));
            taskExecutors.put(uuid, taskExecutor);

            MailboxRegistryImpl mailboxRegistry = new MailboxRegistryImpl(kernal);

            mailboxRegistries.put(uuid, mailboxRegistry);

            MessageServiceImpl msgSvc = new TestMessageServiceImpl(kernal, mgr);
            msgSvc.localNodeId(uuid);
            msgSvc.taskExecutor(taskExecutor);
            mgr.register(msgSvc);

            ExchangeServiceImpl exchangeSvc = new ExchangeServiceImpl(kernal);
            exchangeSvc.taskExecutor(taskExecutor);
            exchangeSvc.messageService(msgSvc);
            exchangeSvc.mailboxRegistry(mailboxRegistry);
            exchangeSvc.init();

            exchangeServices.put(uuid, exchangeSvc);
        }
    }

    /** Task reordering executor. */
    private static class IgniteTestStripedThreadPoolExecutor extends org.apache.ignite.thread.IgniteStripedThreadPoolExecutor {
        /** */
        final Deque<T2<Runnable, Integer>> tasks = new ArrayDeque<>();

        /** Internal stop flag. */
        AtomicBoolean stop = new AtomicBoolean();

        /** Inner execution service. */
        ExecutorService exec = Executors.newWorkStealingPool();

        /** {@inheritDoc} */
        public IgniteTestStripedThreadPoolExecutor(
            final ExecutionStrategy execStgy,
            int concurrentLvl,
            String igniteInstanceName,
            String threadNamePrefix,
            Thread.UncaughtExceptionHandler eHnd,
            boolean allowCoreThreadTimeOut,
            long keepAliveTime
        ) {
            super(concurrentLvl, igniteInstanceName, threadNamePrefix, eHnd, allowCoreThreadTimeOut, keepAliveTime);

            GridTestUtils.runAsync(() -> {
                while (!stop.get()) {
                    synchronized (tasks) {
                        while (!tasks.isEmpty()) {
                            T2<Runnable, Integer> r = execStgy.nextTask(tasks);

                            exec.execute(() -> super.execute(r.getKey(), r.getValue()));
                        }
                    }

                    LockSupport.parkNanos(ThreadLocalRandom.current().nextLong(1_000, 10_000));
                }
            });
        }

        /** {@inheritDoc} */
        @Override public void execute(Runnable task, int idx) {
            synchronized (tasks) {
                tasks.add(new T2<>(task, idx));
            }
        }

        /** {@inheritDoc} */
        @Override public void shutdown() {
            stop.set(true);

            super.shutdown();
        }

        /** {@inheritDoc} */
        @Override public List<Runnable> shutdownNow() {
            stop.set(true);

            return super.shutdownNow();
        }
    }

    /** */
    @After
    public void tearDown() {
        taskExecutors.values().forEach(QueryTaskExecutorImpl::tearDown);

        if (lastE != null)
            throw new AssertionError(lastE);
    }

    /** */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    protected List<UUID> nodes() {
        return nodes;
    }

    /** */
    protected ExchangeService exchangeService(UUID nodeId) {
        return exchangeServices.get(nodeId);
    }

    /** */
    protected MailboxRegistry mailboxRegistry(UUID nodeId) {
        return mailboxRegistries.get(nodeId);
    }

    /** */
    protected QueryTaskExecutor taskExecutor(UUID nodeId) {
        return taskExecutors.get(nodeId);
    }

    /** */
    protected ExecutionContext<Object[]> executionContext(UUID nodeId, UUID qryId, long fragmentId) {
        FragmentDescription fragmentDesc = new FragmentDescription(fragmentId, null, null, null);
        return new ExecutionContext<>(
            taskExecutor(nodeId),
            PlanningContext.builder()
                .localNodeId(nodeId)
                .logger(log())
                .build(),
            qryId, fragmentDesc, ArrayRowHandler.INSTANCE, ImmutableMap.of());
    }

    /** */
    private void handle(Thread t, Throwable ex) {
        log().error(ex.getMessage(), ex);
        lastE = ex;
    }

    /** */
    private static class TestMessageServiceImpl extends MessageServiceImpl {
        /** */
        private final TestIoManager mgr;

        /** */
        private TestMessageServiceImpl(GridKernalContext kernal, TestIoManager mgr) {
            super(kernal);
            this.mgr = mgr;
        }

        /** {@inheritDoc} */
        @Override public void send(UUID nodeId, CalciteMessage msg) {
            mgr.send(localNodeId(), nodeId, msg);
        }

        /** {@inheritDoc} */
        @Override public boolean alive(UUID nodeId) {
            return true;
        }

        /** {@inheritDoc} */
        @Override protected void prepareMarshal(Message msg) {
            // No-op;
        }

        /** {@inheritDoc} */
        @Override protected void prepareUnmarshal(Message msg) {
            // No-op;
        }
    }
}
