/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxQueryResultsEnlistResponse;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapterEx;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;

/**
 * Future processing transaction enlisting and locking of entries
 * produces by complex DML queries with reduce step.
 */
public class GridDhtTxQueryResultsEnlistFuture
    extends GridDhtTxQueryEnlistAbstractFuture<GridNearTxQueryResultsEnlistResponse> {
    /** */
    private Collection<Object> rows;

    /** */
    private GridCacheOperation op;

    /**
     * @param nearNodeId Near node ID.
     * @param nearLockVer Near lock version.
     * @param topVer Topology version.
     * @param mvccSnapshot Mvcc snapshot.
     * @param threadId Thread ID.
     * @param nearFutId Near future id.
     * @param nearMiniId Near mini future id.
     * @param tx Transaction.
     * @param timeout Lock acquisition timeout.
     * @param cctx Cache context.
     * @param rows Collection of rows.
     * @param op Cache operation.
     */
    public GridDhtTxQueryResultsEnlistFuture(UUID nearNodeId,
        GridCacheVersion nearLockVer,
        AffinityTopologyVersion topVer,
        MvccSnapshot mvccSnapshot,
        long threadId,
        IgniteUuid nearFutId,
        int nearMiniId,
        GridDhtTxLocalAdapter tx,
        long timeout,
        GridCacheContext<?, ?> cctx,
        Collection<Object> rows,
        GridCacheOperation op) {
        super(nearNodeId,
            nearLockVer,
            topVer,
            mvccSnapshot,
            threadId,
            nearFutId,
            nearMiniId,
            null,
            tx,
            timeout,
            cctx);

        this.rows = rows;
        this.op = op;
    }

    /** {@inheritDoc} */
    @Override protected UpdateSourceIterator<?> createIterator() throws IgniteCheckedException {
        return new UpdateResultsIterator<>(op, rows);
    }

    /** {@inheritDoc} */
    @NotNull @Override public GridNearTxQueryResultsEnlistResponse createResponse(@NotNull Throwable err) {
        return new GridNearTxQueryResultsEnlistResponse(cctx.cacheId(), nearFutId, nearMiniId, nearLockVer, 0, err);
    }

    /** {@inheritDoc} */
    @NotNull @Override public GridNearTxQueryResultsEnlistResponse createResponse(long res, boolean removeMapping) {
        return new GridNearTxQueryResultsEnlistResponse(cctx.cacheId(), nearFutId, nearMiniId, nearLockVer, res, null);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridDhtTxQueryResultsEnlistFuture future = (GridDhtTxQueryResultsEnlistFuture)o;

        return Objects.equals(futId, future.futId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return futId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxQueryResultsEnlistFuture.class, this);
    }

    /** */
    private class UpdateResultsIterator<T>
        extends GridCloseableIteratorAdapterEx<T> implements UpdateSourceIterator<T>  {
        /** */
        private static final long serialVersionUID = 0L;
        /** */
        private final GridCacheOperation op;
        /** */
        private final Iterator<T> it;

        /**
         * @param op Cache operation.
         * @param rows Rows.
         */
        private UpdateResultsIterator(GridCacheOperation op, Collection<T> rows) {
            this.op = op;

            it = rows.iterator();
        }

        /** {@inheritDoc} */
        @Override public void beforeDetach() {
            //No-op.
        }

        /** {@inheritDoc} */
        @Override public GridCacheOperation operation() {
            return op;
        }

        /** {@inheritDoc} */
        @Override protected T onNext() throws IgniteCheckedException {
            return it.next();
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws IgniteCheckedException {
            return it.hasNext();
        }

        /** {@inheritDoc} */
        @Override protected void onRemove() throws IgniteCheckedException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws IgniteCheckedException {
            // No-op
        }
    }
}
