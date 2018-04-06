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

package org.apache.ignite.internal;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridSpinReadWriteLock;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
@GridToStringExclude
public class GridKernalGatewayImpl implements GridKernalGateway, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringExclude
    private final GridSpinReadWriteLock rwLock = new GridSpinReadWriteLock();
    private final IgniteLogger log;

    /** */
    @GridToStringExclude
    private volatile IgniteFutureImpl<?> reconnectFut;

    /** */
    private final AtomicReference<GridKernalState> _state = new AtomicReference<>(GridKernalState.STOPPED);

    /** */
    @GridToStringExclude
    private final String gridName;

    /**
     * User stack trace.
     *
     * Intentionally uses non-volatile variable for optimization purposes.
     */
    private String stackTrace;

    /**
     * @param gridName Grid name.
     */
    public GridKernalGatewayImpl(String gridName, GridKernalContext ctx) {
        log = ctx.log(this.getClass());
        this.gridName = gridName;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased", "BusyWait"})
    @Override public void readLock() throws IllegalStateException {
        if (stackTrace == null)
            stackTrace = stackTrace();

        rwLock.readLock();

        GridKernalState state = this._state.get();

        if (state != GridKernalState.STARTED) {
            // Unlock just acquired lock.
            rwLock.readUnlock();

            if (state == GridKernalState.DISCONNECTED) {
                assert reconnectFut != null;

                U.dumpStack(log, "org.apache.ignite.internal.GridKernalGatewayImpl.readLock()(state == GridKernalState.DISCONNECTED)");
                throw new IgniteClientDisconnectedException(reconnectFut, "Client node disconnected: " + gridName);
            }

            throw illegalState();
        }
    }

    /** {@inheritDoc} */
    @Override public void readLockAnyway() {
        if (stackTrace == null)
            stackTrace = stackTrace();

        rwLock.readLock();

        if (_state.get() == GridKernalState.DISCONNECTED) {
            U.dumpStack(log, "org.apache.ignite.internal.GridKernalGatewayImpl.readLockAnyway()(_state.get() == GridKernalState.DISCONNECTED)");
            throw new IgniteClientDisconnectedException(reconnectFut, "Client node disconnected: " + gridName);
        }
    }

    /** {@inheritDoc} */
    @Override public void readUnlock() {
        rwLock.readUnlock();
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"BusyWait"})
    @Override public void writeLock() {
        if (stackTrace == null)
            stackTrace = stackTrace();

        boolean interrupted = false;

        // Busy wait is intentional.
        while (true)
            try {
                if (rwLock.tryWriteLock(200, TimeUnit.MILLISECONDS))
                    break;
                else
                    Thread.sleep(200);
            }
            catch (InterruptedException ignore) {
                // Preserve interrupt status & ignore.
                // Note that interrupted flag is cleared.
                interrupted = true;
            }

        if (interrupted)
            Thread.currentThread().interrupt();
    }

    /** {@inheritDoc} */
    @Override public boolean tryWriteLock(long timeout) throws InterruptedException {
        boolean acquired = rwLock.tryWriteLock(timeout, TimeUnit.MILLISECONDS);

        if (acquired) {
            if (stackTrace == null)
                stackTrace = stackTrace();

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public GridFutureAdapter<?> onDisconnected() {
        U.dumpStack(log, "org.apache.ignite.internal.GridKernalGatewayImpl.onDisconnected");
        GridKernalState current = _state.get();
        log.info(String.format("org.apache.ignite.internal.GridKernalGatewayImpl.onDisconnected state = [%s]", current));
        if (current == GridKernalState.DISCONNECTED) {
            assert reconnectFut != null;

            return (GridFutureAdapter<?>)reconnectFut.internalFuture();
        }

        GridFutureAdapter<?> fut = new GridFutureAdapter<>();

        reconnectFut = new IgniteFutureImpl<>(fut);

        log.info(String.format("trying... _state.compareAndSet(GridKernalState.STARTED, GridKernalState.DISCONNECTED)"));
        if (!_state.compareAndSet(GridKernalState.STARTED, GridKernalState.DISCONNECTED)) {
            log.info(String.format("!_state.compareAndSet(GridKernalState.STARTED, GridKernalState.DISCONNECTED) [Node stopped.]"));
            ((GridFutureAdapter<?>)reconnectFut.internalFuture()).onDone(new IgniteCheckedException("Node stopped."));

            return null;
        }

        log.info(String.format("DONE _state.compareAndSet(GridKernalState.STARTED, GridKernalState.DISCONNECTED)"));

        return fut;
    }

    /** {@inheritDoc} */
    @Override public void onReconnected() {
        U.dumpStack(log, "org.apache.ignite.internal.GridKernalGatewayImpl.onReconnected");
        log.info(String.format("trying... _state.compareAndSet(GridKernalState.DISCONNECTED, GridKernalState.STARTED)"));
        if (_state.compareAndSet(GridKernalState.DISCONNECTED, GridKernalState.STARTED)) {
            log.info(String.format("DONE _state.compareAndSet(GridKernalState.DISCONNECTED, GridKernalState.STARTED)"));
            ((GridFutureAdapter<?>) reconnectFut.internalFuture()).onDone();
        }
    }

    /**
     * Retrieves user stack trace.
     *
     * @return User stack trace.
     */
    private static String stackTrace() {
        StringWriter sw = new StringWriter();

        new Throwable().printStackTrace(new PrintWriter(sw));

        return sw.toString();
    }

    /**
     * Creates new illegal state exception.
     *
     * @return Newly created exception.
     */
    private IllegalStateException illegalState() {
        return new IllegalStateException("Grid is in invalid state to perform this operation. " +
            "It either not started yet or has already being or have stopped [gridName=" + gridName +
            ", state=" + _state + ']');
    }

    /** {@inheritDoc} */
    @Override public void writeUnlock() {
        rwLock.writeUnlock();
    }

    /** {@inheritDoc} */
    @Override public void setState(GridKernalState state) {
        assert state != null;

        log.info("org.apache.ignite.internal.GridKernalGatewayImpl.setState " +
                "rwLock.writeLockedByCurrentThread(): " +
                rwLock.writeLockedByCurrentThread());

        U.dumpStack(log, String.format("org.apache.ignite.internal.GridKernalGatewayImpl.setState " +
                "old state: [%s] new state: [%s]", _state.get(), state));

        // NOTE: this method should always be called within write lock.
        this._state.set(state);

        if (reconnectFut != null) {
            log.info("org.apache.ignite.internal.GridKernalGatewayImpl.setState " +
                    "reconnectFut != null ");
            ((GridFutureAdapter<?>) reconnectFut.internalFuture()).onDone(new IgniteCheckedException("Node stopped."));
        }
    }

    /** {@inheritDoc} */
    @Override public GridKernalState getState() {
        GridKernalState gridKernalState = _state.get();
        U.dumpStack(log, String.format("org.apache.ignite.internal.GridKernalGatewayImpl.getState state = [%s]",
                gridKernalState));

        return gridKernalState;
    }

    /** {@inheritDoc} */
    @Override public String userStackTrace() {
        return stackTrace;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridKernalGatewayImpl.class, this);
    }
}