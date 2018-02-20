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

package org.apache.ignite.internal.processors.query.h2.database;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.MvccDataPageIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2RowLinkIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2SearchRow;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;

import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor.assertMvccVersionValid;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor.unmaskCoordinatorVersion;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.MvccDataPageIO.MVCC_INFO_SIZE;

/**
 *
 */
public class H2TreeFilterClosure implements H2Tree.TreeRowClosure<GridH2SearchRow, GridH2Row> {
    /** */
    private final MvccSnapshot mvccSnapshot;

    /** */
    private final IndexingQueryCacheFilter filter;

    /** */
    private final GridCacheContext cctx;

    /**
     * @param filter Cache filter.
     * @param mvccSnapshot MVCC snapshot.
     * @param cctx Cache context.
     * @param lsnr Page lock listener.
     */
    public H2TreeFilterClosure(IndexingQueryCacheFilter filter, MvccSnapshot mvccSnapshot, GridCacheContext cctx) {
        assert (filter != null || mvccSnapshot != null) && cctx != null ;

        this.filter = filter;
        this.mvccSnapshot = mvccSnapshot;
        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(BPlusTree<GridH2SearchRow, GridH2Row> tree, BPlusIO<GridH2SearchRow> io,
        long pageAddr, int idx)  throws IgniteCheckedException {

        return (filter  == null || applyFilter((H2RowLinkIO)io, pageAddr, idx))
            && (mvccSnapshot == null || applyMvcc((H2RowLinkIO)io, pageAddr, idx));
    }

    /**
     * @param io Row IO.
     * @param pageAddr Page address.
     * @param idx Item index.
     * @return {@code True} if row passes the filter.
     */
    private boolean applyFilter(H2RowLinkIO io, long pageAddr, int idx) {
        assert filter != null;

        return filter.applyPartition(PageIdUtils.partId(pageId(io.getLink(pageAddr, idx))));
    }

    /**
     * @param io Row IO.
     * @param pageAddr Page address.
     * @param idx Item index.
     * @return {@code True} if row passes the filter.
     */
    private boolean applyMvcc(H2RowLinkIO io, long pageAddr, int idx) throws IgniteCheckedException {
        assert io.storeMvccInfo() : io;

        long rowCrdVer = io.getMvccCoordinatorVersion(pageAddr, idx);

        assert unmaskCoordinatorVersion(rowCrdVer) == rowCrdVer : rowCrdVer;
        assert rowCrdVer > 0 : rowCrdVer;

        int cmp = Long.compare(mvccSnapshot.coordinatorVersion(), rowCrdVer);

        if (cmp == 0) {
            long rowCntr = io.getMvccCounter(pageAddr, idx);

            cmp = Long.compare(mvccSnapshot.counter(), rowCntr);

            return cmp >= 0 &&
                !newVersionAvailable(io, pageAddr, idx) &&
                !mvccSnapshot.activeTransactions().contains(rowCntr);
        }
        else
            return cmp > 0;
    }

    /**
     * @param rowIo Row IO.
     * @param pageAddr Page address.
     * @param idx Item index.
     * @return {@code True} if a new version is available.
     */
    private boolean newVersionAvailable(H2RowLinkIO rowIo, long pageAddr, int idx) throws IgniteCheckedException {
        long newCrd;
        long newCntr;

        PageMemory pageMem = cctx.dataRegion().pageMemory();
        int grpId = cctx.groupId();

        long link = rowIo.getLink(pageAddr, idx);
        long pageId = pageId(link);
        long page = pageMem.acquirePage(grpId, pageId);
        // TODO Move to MvccIO
        try {
            long dataPageAddr = pageMem.readLock(grpId, pageId, page);

            try{
                MvccDataPageIO dataIo = MvccDataPageIO.VERSIONS.forPage(dataPageAddr);

                DataPagePayload data = dataIo.readPayload(dataPageAddr, itemId(link), pageMem.pageSize());

                assert data.payloadSize() >= MVCC_INFO_SIZE : "MVCC info should be fit on the very first data page.";

                long addr = dataPageAddr + data.offset();

                // Skip xid_min.
                addr += 16;

                newCrd = PageUtils.getLong(addr, 0);
                newCntr = PageUtils.getLong(addr, 8);
            }
            finally {
                pageMem.readUnlock(grpId, pageId, page);
            }
        }
        finally {
            pageMem.releasePage(grpId, pageId, page);
        }

        if (newCrd == 0)
            return false;

        int cmp = Long.compare(mvccSnapshot.coordinatorVersion(), newCrd);

        if (cmp == 0) {
            assert assertMvccVersionValid(newCrd, newCntr);

            return newCntr <= mvccSnapshot.counter() && !mvccSnapshot.activeTransactions().contains(newCntr);
        }
        else
            return cmp < 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2TreeFilterClosure.class, this);
    }
}
