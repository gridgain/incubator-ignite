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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * Data page with MVCC xid_min and xid_max fields.
 */
public class MvccDataPageIO extends AbstractDataPageIO<CacheDataRow> {
    /** */
    public static final int MVCC_INFO_SIZE = 4 * 8;

    /** */
    public static final IOVersions<MvccDataPageIO> VERSIONS = new IOVersions<>(
        new MvccDataPageIO(1)
    );

    /**
     * @param ver Page format version.
     */
    protected MvccDataPageIO(int ver) {
        super(T_MVCC_DATA, ver);
    }

    /** {@inheritDoc} */
    @Override protected void writeFragmentData(CacheDataRow row, ByteBuffer buf, int rowOff,
        int payloadSize) throws IgniteCheckedException {
        DataPageIOUtils.writeFragmentData(row, buf, rowOff, payloadSize, true);
    }

    /** {@inheritDoc} */
    @Override protected void writeRowData(long pageAddr, int dataOff, int payloadSize, CacheDataRow row,
        boolean newRow) throws IgniteCheckedException {
        DataPageIOUtils.writeRowData(pageAddr, dataOff, payloadSize, row, newRow, true);
    }

    /** {@inheritDoc} */
    @Override public int getRowSize(CacheDataRow row) throws IgniteCheckedException {
        return DataPageIO.getRowSize(row, row.cacheId() != 0) + MVCC_INFO_SIZE;
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        // TODO Add xid_min/xid_max to layout.
        sb.a("MvccDataPageIO [\n");
        printPageLayout(addr, pageSize, sb);
        sb.a("\n]");
    }

    /** {@inheritDoc} */
    @Override public int getFreeSpace(long pageAddr) {
        int freeSpace = super.getFreeSpace(pageAddr) - MVCC_INFO_SIZE;

        return freeSpace < 0 ? 0 : freeSpace;
    }

}
