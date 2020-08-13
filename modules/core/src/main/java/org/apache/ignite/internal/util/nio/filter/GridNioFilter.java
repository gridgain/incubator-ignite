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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.nio.GridNioException;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

/**
 * This interface defines the general element in transformation chain between the nio server and
 * application.
 */
public interface GridNioFilter {
    /**
     * Beginning of a filter lifecycle, invoked on server start. It is guaranteed that this method will be invoked
     * prior to any of the event-related methods.
     *
     */
    public default void start() {
        // No-op.
    }

    /**
     * End of a filter lifecycle, invoked on server stop. It is guaranteed that this method will be invoked after all
     * events are processed, no more event-related methods will be invoked after this method called.
     */
    public default void stop() {
        // No-op.
    }

    /**
     * Gets next filter in filter chain.
     *
     * @return Next filter (in order from application to network layer).
     */
    public GridNioFilter nextFilter();

    /**
     * Sets next filter in filter chain. In filter chain next filter would be more close
     * to the network layer.
     *
     * @param filter Next filter in filter chain.
     */
    public void nextFilter(GridNioFilter filter);

    /**
     * Gets previous filter in filter chain.
     *
     * @return Previous filter (in order from application to network layer).
     */
    public GridNioFilter previousFilter();

    /**
     * Sets previous filter in filter chain. In filter chain previous filter would be more close
     * to application layer.
     *
     * @param filter Previous filter in filter chain.
     */
    public void previousFilter(GridNioFilter filter);

    /**
     * Invoked when a new session was created.
     *
     * @param ses Opened session.
     * @throws IgniteCheckedException If GridNioException occurred while handling event.
     */
    public default void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionOpened(ses);
    }

    /**
     * Invoked after session get closed.
     *
     * @param ses Closed session.
     * @throws IgniteCheckedException If GridNioException occurred while handling event.
     */
    public default void onSessionClosed(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionClosed(ses);
    }

    /**
     * Invoked when exception is caught in filter processing.
     *
     * @param ses Session that caused IgniteCheckedException.
     * @param ex GridNioException instance.
     * @throws IgniteCheckedException If IgniteCheckedException occurred while handling event.
     */
    public default void onExceptionCaught(GridNioSession ses, IgniteCheckedException ex) throws IgniteCheckedException {
        proceedExceptionCaught(ses, ex);
    }

    /**
     * Invoked when a write request is performed on a session.
     *
     * @param ses Session on which message should be written.
     * @param msg Message being written.
     * @param fut {@code True} if write future should be created.
     * @param ackC Closure invoked when message ACK is received.
     * @return Write future or {@code null}.
     * @throws GridNioException If GridNioException occurred while handling event.
     */
    public default GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg, boolean fut,
        IgniteInClosure<IgniteException> ackC) throws IgniteCheckedException {
        return proceedSessionWrite(ses, msg, fut, ackC);
    }

    /**
     * Invoked when a new messages received.
     *
     * @param ses Session on which message was received.
     * @param msg Received message.
     * @throws IgniteCheckedException If IgniteCheckedException occurred while handling event.
     */
    public default void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
        proceedMessageReceived(ses, msg);
    }

    /**
     * Invoked when a messages sent.
     *
     * @param ses Session on which message was received.
     * @param msg Received message.
     * @throws IgniteCheckedException If IgniteCheckedException occurred while handling event.
     */
    public default void onMessageSent(GridNioSession ses, Object msg) throws IgniteCheckedException {
        proceedMessageSent(ses, msg);
    }

    /**
     * Invoked when a session close request is performed on session.
     *
     * @param ses Session to close.
     * @param cause Optional close cause.
     * @return Close future.
     * @throws IgniteCheckedException If IgniteCheckedException occurred while handling event.
     */
    public default GridNioFuture<Boolean> onSessionClose(GridNioSession ses, @Nullable IgniteCheckedException cause) throws IgniteCheckedException {
        return proceedSessionClose(ses, cause);
    }

    /**
     * Called on session idle.
     *
     * @param ses Session that is idle.
     * @throws IgniteCheckedException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public default void onSessionIdle(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionIdle(ses);
    }

    /**
     * Called when session is idle for longer time that is
     * allowed by NIO server.
     *
     * @param ses Session that is idle.
     * @throws IgniteCheckedException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public default void onSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionIdleTimeout(ses);
    }

    /**
     * Called when session has not empty write buffer that has not been fully
     * flushed during max timeout allowed by NIO server.
     *
     * @param ses Session that has timed out writes.
     * @throws IgniteCheckedException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public default void onSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionWriteTimeout(ses);
    }

    /**
     * Pauses reads for session.
     *
     * @param ses Session.
     * @return Future for operation.
     * @throws IgniteCheckedException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public default GridNioFuture<?> onPauseReads(GridNioSession ses) throws IgniteCheckedException {
        return proceedPauseReads(ses);
    }

    /**
     * Resumes reads for session.
     *
     * @param ses Session.
     * @return Future for operation.
     * @throws IgniteCheckedException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    public default GridNioFuture<?> onResumeReads(GridNioSession ses) throws IgniteCheckedException {
        return proceedResumeReads(ses);
    }

    /**
     * Forwards session opened event to the next logical filter in filter chain.
     *
     * @param ses Opened session.
     * @throws IgniteCheckedException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    default void proceedSessionOpened(GridNioSession ses) throws IgniteCheckedException {
        checkPrevious(this);

        previousFilter().onSessionOpened(ses);
    }

    /**
     * Forwards session closed event to the next logical filter in filter chain.
     *
     * @param ses Closed session.
     * @throws IgniteCheckedException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    default void proceedSessionClosed(GridNioSession ses) throws IgniteCheckedException {
        checkPrevious(this);

        previousFilter().onSessionClosed(ses);
    }

    /**
     * Forwards GridNioException event to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @param e GridNioException instance.
     * @throws IgniteCheckedException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    default void proceedExceptionCaught(GridNioSession ses, IgniteCheckedException e) throws IgniteCheckedException {
        checkPrevious(this);

        previousFilter().onExceptionCaught(ses, e);
    }

    /**
     * Forwards received message to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @param msg Received message.
     * @throws IgniteCheckedException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    default void proceedMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
        checkPrevious(this);

        previousFilter().onMessageReceived(ses, msg);
    }

    /**
     * Forwards received message to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @param msg Received message.
     * @throws IgniteCheckedException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    default void proceedMessageSent(GridNioSession ses, Object msg) throws IgniteCheckedException {
        checkPrevious(this);

        previousFilter().onMessageSent(ses, msg);
    }

    /**
     * Forwards write request to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @param msg Message to send.
     * @param fut {@code True} if write future should be created.
     * @param ackC Closure invoked when message ACK is received.
     * @return Write future or {@code null}.
     * @throws IgniteCheckedException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    default GridNioFuture<?> proceedSessionWrite(GridNioSession ses, Object msg, boolean fut,
        IgniteInClosure<IgniteException> ackC) throws IgniteCheckedException {
        checkNext(this);

        return nextFilter().onSessionWrite(ses, msg, fut, ackC);
    }

    /**
     * Forwards session close request to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @param cause
     * @return Close future.
     * @throws IgniteCheckedException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    default GridNioFuture<Boolean> proceedSessionClose(GridNioSession ses, @Nullable IgniteCheckedException cause) throws IgniteCheckedException {
        checkNext(this);

        return nextFilter().onSessionClose(ses, cause);
    }

    /**
     * Forwards session idle notification to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @throws IgniteCheckedException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    default void proceedSessionIdle(GridNioSession ses) throws IgniteCheckedException {
        checkNext(this);

        nextFilter().onSessionIdle(ses);
    }

    /**
     * Forwards session idle notification to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @throws IgniteCheckedException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    default void proceedSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
        checkPrevious(this);

        previousFilter().onSessionIdleTimeout(ses);
    }

    /**
     * Forwards session write timeout notification to the next logical filter in filter chain.
     *
     * @param ses Session instance.
     * @throws IgniteCheckedException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    default void proceedSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
        checkPrevious(this);

        previousFilter().onSessionWriteTimeout(ses);
    }

    /**
     * Pauses reads for session.
     *
     * @param ses Session.
     * @return Future for operation.
     * @throws IgniteCheckedException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    default GridNioFuture<?> proceedPauseReads(GridNioSession ses) throws IgniteCheckedException {
        checkNext(this);

        return nextFilter().onPauseReads(ses);
    }

    /**
     * Resumes reads for session.
     *
     * @param ses Session.
     * @return Future for operation.
     * @throws IgniteCheckedException If filter is not in chain or GridNioException occurred in the underlying filter.
     */
    default GridNioFuture<?> proceedResumeReads(GridNioSession ses) throws IgniteCheckedException {
        checkNext(this);

        return nextFilter().onResumeReads(ses);
    }

    /**
     * Checks that next filter is set.
     *
     * @throws GridNioException If next filter is not set.
     */
    static void checkNext(GridNioFilter filter) throws GridNioException {
        if (filter.nextFilter() == null)
            throw new GridNioException("Failed to proceed with filter call since next filter is not set " +
                "(do you use filter outside the filter chain?): " + filter.getClass().getName());
    }

    /**
     * Checks that next filter is set.
     *
     * @throws GridNioException If next filter is not set.
     */
    static void checkPrevious(GridNioFilter filter) throws GridNioException {
        if (filter.previousFilter() == null)
            throw new GridNioException("Failed to proceed with filter call since previous filter is not set " +
                "(do you use filter outside the filter chain?): " + filter.getClass().getName());
    }
}
