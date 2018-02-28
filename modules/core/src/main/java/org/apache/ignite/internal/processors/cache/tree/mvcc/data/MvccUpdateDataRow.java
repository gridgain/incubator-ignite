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

package org.apache.ignite.internal.processors.cache.tree.mvcc.data;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.tree.RowLinkIO;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.assertMvccVersionValid;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.hasNewMvccVersion;

/**
 *
 */
public class MvccUpdateDataRow extends MvccDataRow implements BPlusTree.TreeRowClosure<CacheSearchRow, CacheDataRow> {
    /** */
    private final CacheGroupContext grp;

    /** */
    private UpdateResult res;

    /** */
    private boolean canCleanup;

    /** */
    private GridLongList activeTxs;

    /** */
    private List<MvccLinkAwareSearchRow> cleanupRows;

    /** */
    private final MvccSnapshot mvccSnapshot;

    /** */
    private final boolean needOld;

    /** */
    private CacheDataRow oldRow;

    /**
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @param expireTime Expire time.
     * @param mvccSnapshot MVCC snapshot.
     * @param needOld {@code True} if need previous value.
     * @param part Partition.
     * @param cacheId Cache ID.
     * @param grp Group context.
     */
    public MvccUpdateDataRow(
        KeyCacheObject key,
        CacheObject val,
        GridCacheVersion ver,
        long expireTime,
        MvccSnapshot mvccSnapshot,
        MvccVersion newVer,
        boolean needOld,
        int part,
        int cacheId,
        CacheGroupContext grp) {
        super(key, val, ver, part, expireTime, cacheId, mvccSnapshot.coordinatorVersion(), mvccSnapshot.counter(), newVer);

        this.mvccSnapshot = mvccSnapshot;
        this.needOld = needOld;
        this.grp = grp;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(BPlusTree<CacheSearchRow, CacheDataRow> tree,
        BPlusIO<CacheSearchRow> io,
        long pageAddr,
        int idx)
        throws IgniteCheckedException
    {
        RowLinkIO rowIo = (RowLinkIO)io;

        // Assert version grows.
        assert assertVersion(rowIo, pageAddr, idx);

        boolean checkActive = mvccSnapshot.activeTransactions().size() > 0;

        boolean txActive = false;

        long rowCrdVer = rowIo.getMvccCoordinatorVersion(pageAddr, idx);

        long crdVer = mvccCoordinatorVersion();

        boolean isFirstRmvd = false;

        if (res == null) {
            int cmp = Long.compare(crdVer, rowCrdVer);

            if (cmp == 0)
                cmp = Long.compare(mvccSnapshot.counter(), rowIo.getMvccCounter(pageAddr, idx));

            if (cmp == 0)
                res = UpdateResult.VERSION_FOUND;
            else {
                isFirstRmvd = hasNewMvccVersion(grp, rowIo.getLink(pageAddr, idx));

                if (isFirstRmvd)
                    res = UpdateResult.PREV_NULL;
                else
                    res = UpdateResult.PREV_NOT_NULL;

                if (needOld)
                    oldRow = tree.getRow(io, pageAddr, idx, CacheDataRowAdapter.RowData.NO_KEY);
                else
                    oldRow = tree.getRow(io, pageAddr, idx, RowData.LINK_WITH_HEADER);
            }
        }

        // Suppose transactions on previous coordinator versions are done.
        if (checkActive && crdVer == rowCrdVer) {
            long rowMvccCntr = rowIo.getMvccCounter(pageAddr, idx);

            long activeTx = isFirstRmvd ? oldRow.newMvccCounter() : rowMvccCntr;

            if (mvccSnapshot.activeTransactions().contains(activeTx)) {
                txActive = true;

                if (activeTxs == null)
                    activeTxs = new GridLongList();

                activeTxs.add(activeTx);
            }
        }

        if (!txActive) {
            assert Long.compare(crdVer, rowCrdVer) >= 0;

            int cmp;

            long rowCntr = rowIo.getMvccCounter(pageAddr, idx);

            if (crdVer == rowCrdVer)
                cmp = Long.compare(mvccSnapshot.cleanupVersion(), rowCntr);
            else
                cmp = 1;

            if (cmp >= 0) {
                // Do not cleanup oldest version.
                if (canCleanup) {
                    assert assertMvccVersionValid(rowCrdVer, rowCntr);

                    // Should not be possible to cleanup active tx.
                    assert rowCrdVer != crdVer || !mvccSnapshot.activeTransactions().contains(rowCntr);

                    if (cleanupRows == null)
                        cleanupRows = new ArrayList<>();

                    cleanupRows.add(new MvccLinkAwareSearchRow(cacheId, key, rowCrdVer, rowCntr, rowIo.getLink(pageAddr, idx)));
                }
                else
                    canCleanup = true;
            }
        }

        return true;
    }

    /**
     * @return Old row.
     */
    public CacheDataRow oldRow() {
        return oldRow;
    }

    /**
     * @return {@code True} if previous value was non-null.
     */
    public UpdateResult updateResult() {
        return res == null ? UpdateResult.PREV_NULL : res;
    }

    /**
     * @return Active transactions to wait for.
     */
    @Nullable public GridLongList activeTransactions() {
        return activeTxs;
    }

    /**
     * @return Rows which are safe to cleanup.
     */
    public List<MvccLinkAwareSearchRow> cleanupRows() {
        return cleanupRows;
    }

    /**
     * @param io IO.
     * @param pageAddr Page address.
     * @param idx Item index.
     * @return Always {@code true}.
     */
    private boolean assertVersion(RowLinkIO io, long pageAddr, int idx) {
        long rowCrdVer = io.getMvccCoordinatorVersion(pageAddr, idx);
        long rowCntr = io.getMvccCounter(pageAddr, idx);

        int cmp = Long.compare(mvccCoordinatorVersion(), rowCrdVer);

        if (cmp == 0)
            cmp = Long.compare(mvccSnapshot.counter(), rowCntr);

        // Can be equals if execute update on backup and backup already rebalanced value updated on primary.
        assert cmp >= 0 : "[updCrd=" + mvccCoordinatorVersion() +
            ", updCntr=" + mvccSnapshot.counter() +
            ", rowCrd=" + rowCrdVer +
            ", rowCntr=" + rowCntr + ']';

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccUpdateDataRow.class, this, "super", super.toString());
    }

    /**
     *
     */
    public enum UpdateResult {
        /** */
        VERSION_FOUND,
        /** */
        PREV_NULL,
        /** */
        PREV_NOT_NULL
    }
}
