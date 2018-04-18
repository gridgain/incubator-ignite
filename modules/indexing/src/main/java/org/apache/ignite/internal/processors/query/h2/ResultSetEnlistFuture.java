package org.apache.ignite.internal.processors.query.h2;

import java.sql.ResultSet;
import java.util.UUID;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryEnlistAbstractFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxQueryEnlistResponse;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.LockingOperationSourceIterator;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Future to process whole local result set of SELECT FOR UPDATE query.
 */
public class ResultSetEnlistFuture extends GridDhtTxQueryEnlistAbstractFuture<GridNearTxQueryEnlistResponse> {
    /** Dummy response. */
    private final static GridNearTxQueryEnlistResponse RESP = new GridNearTxQueryEnlistResponse();

    /** Rows to process. */
    private final ResultSet rs;

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
    ResultSetEnlistFuture(UUID nearNodeId, GridCacheVersion nearLockVer, AffinityTopologyVersion topVer,
        MvccSnapshot mvccSnapshot, long threadId, IgniteUuid nearFutId, int nearMiniId, @Nullable int[] parts,
        GridDhtTxLocalAdapter tx, long timeout, GridCacheContext<?, ?> cctx, ResultSet rs) {
        super(nearNodeId, nearLockVer, topVer, mvccSnapshot, threadId, nearFutId, nearMiniId, parts, tx, timeout, cctx);

        this.rs = rs;
    }

    /**
     * @return Transaction adapter.
     */
    public GridDhtTxLocalAdapter tx() {
        return tx;
    }

    /** {@inheritDoc} */
    @Override protected LockingOperationSourceIterator<?> createIterator() {
        return new SelectForUpdateResultSetIterator(rs);
    }

    /** {@inheritDoc} */
    @Override public GridNearTxQueryEnlistResponse createResponse(@NotNull Throwable err) {
        return new GridNearTxQueryEnlistResponse(cctx.cacheId(), null, 0, null, 0, err);
    }

    /** {@inheritDoc} */
    @Override public GridNearTxQueryEnlistResponse createResponse() {
        return RESP;
    }


}
