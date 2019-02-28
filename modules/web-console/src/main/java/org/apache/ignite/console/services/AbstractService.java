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

package org.apache.ignite.console.services;

import java.util.Collection;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.DataObject;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.lang.IgniteAsyncSupport;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Base class for routers.
 */
public abstract class AbstractService {
    /** */
    protected final Ignite ignite;

    /** */
    private volatile boolean ready;

    /**
     * @param ignite Ignite.
     */
    protected AbstractService(Ignite ignite) {
        this.ignite = ignite;
    }

    /**
     * Initialize caches.
     */
    protected abstract void initialize();

    /**
     * Start transaction.
     *
     * @return Transaction.
     */
    protected Transaction txStart() {
        if (!ready) {
            initialize();

            ready = true;
        }

        IgniteTransactions txs = ignite.transactions();

        Transaction tx = txs.tx();

        return tx == null ? txs.txStart(PESSIMISTIC, REPEATABLE_READ) : new NestedTransaction(tx);
    }

    /**
     * Ensure that transaction was started explicitly.
     */
    protected void ensureTx() {
        if (ignite.transactions().tx() == null)
            throw new IllegalStateException("Transaction was not started explicitly");
    }

    /**
     * Load short list of DTOs.
     *
     * @param ownerId Owner ID.
     * @param ownerIdx Index with DTOs IDs.
     * @param tbl Table with DTOs.
     */
    protected Collection<? extends DataObject> loadList(
        UUID ownerId,
        OneToManyIndex ownerIdx,
        Table<? extends DataObject> tbl
    ) {
        try (Transaction ignored = txStart()) {
            TreeSet<UUID> ids = ownerIdx.load(ownerId);

            return tbl.loadAll(ids);
        }
    }

    /**
     * Nested transaction.
     */
    @SuppressWarnings("deprecation")
    private static class NestedTransaction implements Transaction {
        /** */
        private final Transaction delegate;

        /**
         * @param delegate Real transaction.
         */
        NestedTransaction(Transaction delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid xid() {
            return delegate.xid();
        }

        /** {@inheritDoc} */
        @Override public UUID nodeId() {
            return delegate.nodeId();
        }

        /** {@inheritDoc} */
        @Override public long threadId() {
            return delegate.threadId();
        }

        /** {@inheritDoc} */
        @Override public long startTime() {
            return delegate.startTime();
        }

        /** {@inheritDoc} */
        @Override public TransactionIsolation isolation() {
            return delegate.isolation();
        }

        /** {@inheritDoc} */
        @Override public TransactionConcurrency concurrency() {
            return delegate.concurrency();
        }

        /** {@inheritDoc} */
        @Override public boolean implicit() {
            return delegate.implicit();
        }

        /** {@inheritDoc} */
        @Override public boolean isInvalidate() {
            return delegate.isInvalidate();
        }

        /** {@inheritDoc} */
        @Override public TransactionState state() {
            return delegate.state();
        }

        /** {@inheritDoc} */
        @Override public long timeout() {
            return delegate.timeout();
        }

        /** {@inheritDoc} */
        @Override public long timeout(long timeout) {
            return delegate.timeout(timeout);
        }

        /** {@inheritDoc} */
        @Override public boolean setRollbackOnly() {
            return delegate.setRollbackOnly();
        }

        /** {@inheritDoc} */
        @Override public boolean isRollbackOnly() {
            return delegate.isRollbackOnly();
        }

        /** {@inheritDoc} */
        @Override public void commit() throws IgniteException {
            // Nested transaction do nothing.
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<Void> commitAsync() throws IgniteException {
            return new IgniteFinishedFutureImpl<>();
        }

        /** {@inheritDoc} */
        @Override public void close() throws IgniteException {
            // Nested transaction do nothing.
        }

        /** {@inheritDoc} */
        @Override public void rollback() throws IgniteException {
            // Nested transaction do nothing.
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<Void> rollbackAsync() throws IgniteException {
            return new IgniteFinishedFutureImpl<>();
        }

        /** {@inheritDoc} */
        @Override public void resume() throws IgniteException {
            // Nested transaction do nothing.
        }

        /** {@inheritDoc} */
        @Override public void suspend() throws IgniteException {
            // Nested transaction do nothing.
        }

        /** {@inheritDoc} */
        @Override public @Nullable String label() {
            return delegate.label();
        }

        /** {@inheritDoc} */
        @Override public IgniteAsyncSupport withAsync() {
            return delegate.withAsync();
        }

        /** {@inheritDoc} */
        @Override public boolean isAsync() {
            return delegate.isAsync();
        }

        /** {@inheritDoc} */
        @Override public <R> IgniteFuture<R> future() {
            return delegate.future();
        }
    }
}
