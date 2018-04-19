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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridSpinReadWriteLock;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;

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
    public GridKernalGatewayImpl(String gridName, IgniteLogger log) {
        this.log = log;
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

                dump("readLock()(state == DISCONNECTED)");
                IgniteInternalFuture igniteInternalFuture = ((IgniteFutureImpl) reconnectFut).internalFuture();
                logMsg(String.format("readLock() throw DisconnectedException reconnectFut = [%s] internal = [%s]",
                        reconnectFut.hashCode(),
                        igniteInternalFuture == null ? null : igniteInternalFuture.hashCode()));

                throw new IgniteClientDisconnectedException(proxy(reconnectFut), "Client node disconnected: " + gridName);
            }

            throw illegalState();
        }
    }

    private IgniteFuture<?> proxy(final IgniteFuture<?> rf) {
        return new IgniteFuture() {

            @Override
            public Object get() throws IgniteException {
                logMsg(String.format("proxy future get reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));
                return rf.get();
            }

            @Override
            public Object get(long timeout) throws IgniteException {
                logMsg(String.format("proxy future get(%s) reconFut = [%s] internal = [%s]",
                        timeout,
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));
                return rf.get(timeout);
            }

            @Override
            public Object get(long timeout, TimeUnit unit) throws IgniteException {
                logMsg(String.format("proxy future get(%s, %s) reconFut = [%s] internal = [%s]",
                        timeout, unit,
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));
                return rf.get(timeout, unit);
            }

            @Override
            public boolean cancel() throws IgniteException {
                logMsg(String.format("proxy future cancel() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                return rf.cancel();
            }

            @Override
            public boolean isCancelled() {
                logMsg(String.format("proxy future isCancelled() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                return rf.isCancelled();
            }

            @Override
            public boolean isDone() {
                logMsg(String.format("proxy future isDone() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                return rf.isDone();
            }

            @Override
            public long startTime() {
                logMsg(String.format("proxy future startTime() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                return rf.startTime();
            }

            @Override
            public long duration() {
                logMsg(String.format("proxy future duration() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                return rf.duration();
            }

            @Override
            public IgniteFuture chainAsync(IgniteClosure doneCb, Executor exec) {
                logMsg(String.format("proxy future chainAsync() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                return rf.chainAsync(doneCb, exec);
            }

            @Override
            public IgniteFuture chain(IgniteClosure doneCb) {
                logMsg(String.format("proxy future chain() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                return rf.chain(doneCb);
            }

            @Override
            public void listenAsync(IgniteInClosure lsnr, Executor exec) {
                logMsg(String.format("proxy future listenAsync() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                rf.listenAsync(lsnr, exec);
            }

            @Override
            public void listen(IgniteInClosure lsnr) {
                logMsg(String.format("proxy future listen() reconFut = [%s] internal = [%s]",
                        rf.hashCode(),
                        ((IgniteFutureImpl)rf).internalFuture().hashCode()));

                rf.listen(lsnr);

            }

            @Override
            public int hashCode() {
                return rf.hashCode();
            }
        };
    }

    private void dump(String msg) {
        U.dumpStack(log, "[KG][" + Thread.currentThread().getName() + "]" + msg);
    }

    private void logMsg(String msg) {
        log.info("[KG][" + Thread.currentThread().getName() + "]" + msg);
    }

    /** {@inheritDoc} */
    @Override public void readLockAnyway() {
        if (stackTrace == null)
            stackTrace = stackTrace();

        rwLock.readLock();

        if (_state.get() == GridKernalState.DISCONNECTED) {
            dump("readLockAnyway()(_state.get() == DISCONNECTED)");
            IgniteInternalFuture igniteInternalFuture = ((IgniteFutureImpl) reconnectFut).internalFuture();
            logMsg(String.format("readLockAnyway() throw DisconnectedException reconnectFut = [%s] internal = [%s]",
                    reconnectFut.hashCode(),
                    igniteInternalFuture == null ? null : igniteInternalFuture.hashCode()));
            throw new IgniteClientDisconnectedException(proxy(reconnectFut), "Client node disconnected: " + gridName);
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
        U.dumpStack(log, "onDisconnected");

        GridKernalState current = _state.get();

        logMsg(String.format("onDisconnected state = [%s]", current));

        if (current == GridKernalState.DISCONNECTED) {
            assert reconnectFut != null;

            return (GridFutureAdapter<?>)reconnectFut.internalFuture();
        }

        GridFutureAdapter<?> fut = new GridFutureAdapter<>();

        reconnectFut = new IgniteFutureImpl<>(fut);

        logMsg(String.format("onDisconnected reconnectFut = [%s] internal = [%s]", reconnectFut.hashCode(), fut.hashCode()));

        logMsg("trying... _state.compareAndSet(STARTED, DISCONNECTED)");
        if (!_state.compareAndSet(GridKernalState.STARTED, GridKernalState.DISCONNECTED)) {
            logMsg("!_state.compareAndSet(STARTED, DISCONNECTED) [Node stopped.]");
            ((GridFutureAdapter<?>) reconnectFut.internalFuture()).onDone(new IgniteCheckedException("Node stopped."));

            return null;
        }

        logMsg("DONE _state.compareAndSet(STARTED, DISCONNECTED)");

        return fut;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onReconnected() {
        dump("onReconnected");
        logMsg("trying... _state.compareAndSet(DISCONNECTED, STARTED)");
        if (_state.compareAndSet(GridKernalState.DISCONNECTED, GridKernalState.STARTED)) {
            logMsg("DONE _state.compareAndSet(DISCONNECTED, STARTED)");
            logMsg("onDone fo reconnectFut.iFut: " + reconnectFut.internalFuture().hashCode());
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

        logMsg("setState " +
                "rwLock.writeLockedByCurrentThread(): " +
                rwLock.writeLockedByCurrentThread());

        dump(String.format("setState " +
                "old state: [%s] new state: [%s]", _state.get(), state));

        // NOTE: this method should always be called within write lock.
        this._state.set(state);

        if (reconnectFut != null) {
            logMsg("setState " +
                    "reconnectFut != null ");
            ((GridFutureAdapter<?>) reconnectFut.internalFuture()).onDone(new IgniteCheckedException("Node stopped."));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GridKernalState getState() {
        GridKernalState gridKernalState = _state.get();
        dump(String.format("getState state = [%s]",
                gridKernalState));

        return gridKernalState;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String userStackTrace() {
        return stackTrace;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return S.toString(GridKernalGatewayImpl.class, this);
    }
}