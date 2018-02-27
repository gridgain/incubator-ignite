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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor.MVCC_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.assertMvccVersionValid;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.MVCC_INFO_SIZE;

/**
 *
 */
public class MvccDataRow extends DataRow {
    /** Mvcc coordinator version. */
    @GridToStringInclude
    private long mvccCrd;

    /** Mvcc counter. */
    @GridToStringInclude
    private long mvccCntr;

    /** New mvcc coordinator version. */
    @GridToStringInclude
    private long newMvccCrd;

    /** New mvcc counter. */
    @GridToStringInclude
    private long newMvccCntr;

    /**
     * @param link Link.
     */
    public MvccDataRow(long link) {
        super(link);
    }

    /**
     * @param grp Context.
     * @param hash Key hash.
     * @param link Link.
     * @param part Partition number.
     * @param rowData Data.
     * @param crdVer Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     */
    public MvccDataRow(CacheGroupContext grp, int hash, long link, int part, RowData rowData, long crdVer, long mvccCntr) {
        super(grp, hash, link, part, rowData);

        assertMvccVersionValid(crdVer, mvccCntr);

        assert rowData == RowData.LINK_ONLY || this.mvccCrd == crdVer && this.mvccCntr == mvccCntr;

        if (rowData == RowData.LINK_ONLY) {
            this.mvccCrd = crdVer;
            this.mvccCntr = mvccCntr;
        }
    }

    /** {@inheritDoc} */
    @Override protected int readHeader(long addr, int off) {
        // xid_min.
        mvccCrd = PageUtils.getLong(addr, off);
        mvccCntr = PageUtils.getLong(addr, off + 8);

        // xid_max.
        newMvccCrd = PageUtils.getLong(addr, off + 16);
        newMvccCntr = PageUtils.getLong(addr, off + 24);

        assert mvccCrd > 0 && mvccCntr > MVCC_COUNTER_NA;

        return MVCC_INFO_SIZE;
    }

    /** {@inheritDoc} */
    @Override public long mvccCoordinatorVersion() {
        return mvccCrd;
    }

    /** {@inheritDoc} */
    @Override public long mvccCounter() {
        return mvccCntr;
    }

    /** {@inheritDoc} */
    @Override public void mvccVersion(long crdVer, long mvccCntr) {
        this.mvccCrd = crdVer;
        this.mvccCntr = mvccCntr;
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        assert (newMvccCrd > 0) == (newMvccCntr > MVCC_COUNTER_NA);

        return newMvccCrd > 0;
    }

    /** {@inheritDoc} */
    @Override public long newMvccCoordinatorVersion() {
        return newMvccCrd;
    }

    /** {@inheritDoc} */
    @Override public long newMvccCounter() {
        return newMvccCntr;
    }

    /** {@inheritDoc} */
    @Override public int size() throws IgniteCheckedException {
        assert mvccCoordinatorVersion() > 0 && mvccCounter() > MVCC_COUNTER_NA;

        return super.size() + MVCC_INFO_SIZE;
    }

    /** {@inheritDoc} */
    @Override public int headerSize() {
        assert mvccCoordinatorVersion() > 0 && mvccCounter() > MVCC_COUNTER_NA;

        return MVCC_INFO_SIZE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccDataRow.class, this, "super", super.toString());
    }
}
