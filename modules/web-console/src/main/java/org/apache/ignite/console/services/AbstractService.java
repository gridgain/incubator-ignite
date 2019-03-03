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
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.console.db.OneToManyIndex;
import org.apache.ignite.console.db.Table;
import org.apache.ignite.console.dto.DataObject;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
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
public abstract class AbstractService extends AbstractVerticle {
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
     *
     * @param msg JSON message.
     * @param key Property name.
     * @return JSON for specified property.
     * @throws IllegalStateException If property not found.
     */
    protected JsonObject getProperty(Message<JsonObject> msg, String key) {
        JsonObject json = msg.body().getJsonObject(key);

        if (json == null)
            throw new IllegalStateException("Message does not contain property: " + key);

        return json;
    }

    /**
     * @param json JSON object.
     * @return ID or {@code null} if object has no ID.
     */
    @Nullable protected UUID getId(JsonObject json) {
        String s = json.getString("_id");

        return F.isEmpty(s) ? null : UUID.fromString(s);
    }

    /**
     * @param msg JSON message.
     * @return User ID.
     * @throws IllegalStateException If user ID not found.
     */
    protected UUID getUserId(Message<JsonObject> msg) {
        JsonObject user = getProperty(msg, "user");

        UUID userId = getId(user);

        if (userId == null)
            throw new IllegalStateException("User ID not found");

        return userId;
    }

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
     * @param rows Number of rows.
     * @return JSON with number of affected rows.
     */
    protected JsonObject rowsAffected(int rows) {
        return new JsonObject()
            .put("rowsAffected", rows);
    }

    /**
     * Nested transaction.
     */
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
            throw new UnsupportedOperationException("Resume is not supported for nested transaction");
        }

        /** {@inheritDoc} */
        @Override public void suspend() throws IgniteException {
            throw new UnsupportedOperationException("Suspend is not supported for nested transaction");
        }

        /** {@inheritDoc} */
        @Override public @Nullable String label() {
            return delegate.label();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("deprecation")
        @Override public IgniteAsyncSupport withAsync() {
            return delegate.withAsync();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("deprecation")
        @Override public boolean isAsync() {
            return delegate.isAsync();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("deprecation")
        @Override public <R> IgniteFuture<R> future() {
            return delegate.future();
        }
    }
}
