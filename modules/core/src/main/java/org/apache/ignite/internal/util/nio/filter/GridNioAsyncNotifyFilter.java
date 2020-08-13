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

package org.apache.ignite.internal.util.nio.filter;

import java.util.concurrent.Executor;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.internal.util.worker.GridWorkerPool;

/**
 * Enables multithreaded notification of session opened, message received and session closed events.
 */
public class GridNioAsyncNotifyFilter extends GridAbstractNioFilter {
    /** Logger. */
    private final IgniteLogger log;

    /** Worker pool. */
    private final GridWorkerPool workerPool;

    /** Ignite instance name. */
    private final String igniteInstanceName;

    /**
     * Assigns filter name to a filter.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param exec Executor.
     * @param log Logger.
     */
    public GridNioAsyncNotifyFilter(String igniteInstanceName, Executor exec, IgniteLogger log) {
        this.igniteInstanceName = igniteInstanceName;
        this.log = log;

        workerPool = new GridWorkerPool(exec, log);
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        workerPool.join(false);
    }

    /** {@inheritDoc} */
    @Override public void onSessionOpened(final GridNioSession ses) throws IgniteCheckedException {
        onSessionOpened(ses, null);
    }

    /** {@inheritDoc} */
    @Override public void onSessionClosed(final GridNioSession ses) throws IgniteCheckedException {
        onSessionClosed(ses, null);
    }

    /** {@inheritDoc} */
    @Override public void onMessageReceived(final GridNioSession ses, final Object msg) throws IgniteCheckedException {
        onMessageReceived(ses, msg, null);
    }

    /**
     * Invoked when a new session was created.
     *
     * @param ses Opened session.
     * @param opC Operation finish closure.
     * @throws IgniteCheckedException If GridNioException occurred while handling event.
     */
    protected void onSessionOpened(final GridNioSession ses, Runnable opC) throws IgniteCheckedException {
        workerPool.execute(new GridWorker(igniteInstanceName, "session-opened-notify", log) {
            @Override protected void body() {
                try {
                    proceedSessionOpened(ses);
                }
                catch (IgniteCheckedException e) {
                    handleException(ses, e);
                }
                finally {
                    onFinish(ses, opC);
                }
            }
        });
    }

    /**
     * Invoked after session get closed.
     *
     * @param ses Closed session.
     * @param opC Operation finish closure.
     * @throws IgniteCheckedException If GridNioException occurred while handling event.
     */
    protected void onSessionClosed(final GridNioSession ses, Runnable opC) throws IgniteCheckedException {
        workerPool.execute(new GridWorker(igniteInstanceName, "session-closed-notify", log) {
            @Override protected void body() {
                try {
                    proceedSessionClosed(ses);
                }
                catch (IgniteCheckedException e) {
                    handleException(ses, e);
                }
                finally {
                    onFinish(ses, opC);
                }
            }
        });
    }

    /**
     * Invoked when a new messages received.
     *
     * @param ses Session on which message was received.
     * @param msg Received message.
     * @param opC Operation finish closure.
     * @throws IgniteCheckedException If IgniteCheckedException occurred while handling event.
     */
    protected void onMessageReceived(final GridNioSession ses, final Object msg, Runnable opC) throws IgniteCheckedException {
        workerPool.execute(new GridWorker(igniteInstanceName, "message-received-notify", log) {
            @Override protected void body() {
                try {
                    proceedMessageReceived(ses, msg);
                }
                catch (IgniteCheckedException e) {
                    handleException(ses, e);
                }
                finally {
                    onFinish(ses, opC);
                }
            }
        });
    }

    /**
     * @param ses Session.
     * @param opC Operation finish closure.
     */
    private void onFinish(GridNioSession ses, Runnable opC) {
        try {
            if (opC != null)
                opC.run();
        } catch (Throwable e) {
            handleException(ses, U.cast(e));
        }
    }

    /**
     * @param ses Session.
     * @param ex Exception.
     */
    private void handleException(GridNioSession ses, IgniteCheckedException ex) {
        try {
            proceedExceptionCaught(ses, ex);
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Failed to forward exception to the underlying filter (will ignore) [ses=" + ses + ", " +
                "originalEx=" + ex + ", ex=" + e + ']');
        }
    }
}
