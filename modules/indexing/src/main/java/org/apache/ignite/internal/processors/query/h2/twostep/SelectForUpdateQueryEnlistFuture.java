package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryEnlistAbstractFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxQueryEnlistResponse;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.LockingOperationSourceIterator;
import org.apache.ignite.lang.IgniteUuid;
import org.h2.value.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Future to process a single page of SELECT FOR UPDATE result.
 */
public class SelectForUpdateQueryEnlistFuture extends GridDhtTxQueryEnlistAbstractFuture<GridNearTxQueryEnlistResponse> {
    /** Rows to process. */
    private final List<Value[]> rows;

    /**
     * @param nearNodeId   Near node ID.
     * @param nearLockVer  Near lock version.
     * @param topVer       Topology version.
     * @param mvccSnapshot Mvcc snapshot.
     * @param threadId     Thread ID.
     * @param nearFutId    Near future id.
     * @param nearMiniId   Near mini future id.
     * @param parts        Partitions.
     * @param tx           Transaction.
     * @param timeout      Lock acquisition timeout.
     * @param cctx         Cache context.
     */
    SelectForUpdateQueryEnlistFuture(UUID nearNodeId, GridCacheVersion nearLockVer, AffinityTopologyVersion topVer,
        MvccSnapshot mvccSnapshot, long threadId, IgniteUuid nearFutId, int nearMiniId, @Nullable int[] parts,
        GridDhtTxLocalAdapter tx, long timeout, GridCacheContext<?, ?> cctx, List<Value[]> rows) {
        super(nearNodeId, nearLockVer, topVer, mvccSnapshot, threadId, nearFutId, nearMiniId, parts, tx, timeout, cctx);

        this.rows = rows;
    }

    /** {@inheritDoc} */
    @Override protected LockingOperationSourceIterator<?> createIterator() throws IgniteCheckedException {
        return new SelectForUpdateResultsIterator(rows.iterator());
    }

    /** {@inheritDoc} */
    @Override public GridNearTxQueryEnlistResponse createResponse(@NotNull Throwable err) {
        return new GridNearTxQueryEnlistResponse(cctx.cacheId(), null, 0, null, 0, err);
    }

    /** {@inheritDoc} */
    @Override public GridNearTxQueryEnlistResponse createResponse() {
        return null;
    }
}
