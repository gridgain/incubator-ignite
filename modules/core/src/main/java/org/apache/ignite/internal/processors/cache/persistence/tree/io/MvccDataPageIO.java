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
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.GridStringBuilder;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor.MVCC_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIOUtils.EntryPart.CACHE_ID;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIOUtils.EntryPart.EXPIRE_TIME;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIOUtils.EntryPart.KEY;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIOUtils.EntryPart.MVCC_INFO;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIOUtils.writeCacheIdFragment;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIOUtils.writeExpireTimeFragment;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIOUtils.writeVersionFragment;

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

    /**
     * Marks row as removed by new version.
     *
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @param newVer New version.
     */
    public void markRemoved(long pageAddr, int dataOff, MvccVersion newVer) {
        long addr = pageAddr + dataOff;

        // Skip xid_min.
        addr += 16;

        // TODO uncomment when IGNITE-7764 is implemented.
        //long prevCrd = PageUtils.getLong(addr, 0);
        //long prevCntr =  PageUtils.getLong(addr, 8);

        //assert prevCrd == 0 && prevCntr == MVCC_COUNTER_NA;

        PageUtils.putLong(addr, 0, newVer.coordinatorVersion());
        PageUtils.putLong(addr, 8, newVer.counter());
    }

    /**
     * Returns MVCC coordinator number.
     *
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @return MVCC coordinator number.
     */
    public long mvccCoordinator(long pageAddr, int dataOff) {
        long addr = pageAddr + dataOff;

        return PageUtils.getLong(addr, 0);
    }

    /**
     * Returns MVCC counter value.
     *
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @return MVCC counter value.
     */
    public long mvccCounter(long pageAddr, int dataOff) {
        long addr = pageAddr + dataOff;

        return PageUtils.getLong(addr, 8);
    }

    /**
     * Returns new MVCC coordinator number.
     *
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @return New MVCC coordinator number.
     */
    public long newMvccCoordinator(long pageAddr, int dataOff) {
        long addr = pageAddr + dataOff;

        // Skip xid_min.
        addr += 16;

        return PageUtils.getLong(addr, 0);
    }

    /**
     * Returns new MVCC counter value.
     *
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @return New MVCC counter value.
     */
    public long newMvccCounter(long pageAddr, int dataOff) {
        long addr = pageAddr + dataOff;

        // Skip xid_min.
        addr += 16;

        return PageUtils.getLong(addr, 8);
    }

    /** {@inheritDoc} */
    @Override protected void writeRowData(long pageAddr, int dataOff, int payloadSize, CacheDataRow row,
        boolean newRow) throws IgniteCheckedException {
        assert row.mvccCounter() != MVCC_COUNTER_NA && row.mvccCoordinatorVersion() != 0;

        long addr = pageAddr + dataOff;

        int cacheIdSize = row.cacheId() != 0 ? 4 : 0;

        if (newRow) {
            PageUtils.putShort(addr, 0, (short)payloadSize);
            addr += 2;

            // xid_min.
            PageUtils.putLong(addr, 0, row.mvccCoordinatorVersion());
            PageUtils.putLong(addr, 8, row.mvccCounter());

            // empty xid_max.
            PageUtils.putLong(addr, 16, row.newMvccCoordinatorVersion());
            PageUtils.putLong(addr, 24, row.newMvccCounter());

            addr += MVCC_INFO_SIZE;

            if (cacheIdSize != 0) {
                PageUtils.putInt(addr, 0, row.cacheId());

                addr += cacheIdSize;
            }

            addr += row.key().putValue(addr);
        }
        else
            addr += (2 + MVCC_INFO_SIZE + cacheIdSize  + row.key().valueBytesLength(null));

        addr += row.value().putValue(addr);

        CacheVersionIO.write(addr, row.version(), false);
        addr += CacheVersionIO.size(row.version(), false);

        PageUtils.putLong(addr, 0, row.expireTime());
    }

    /** {@inheritDoc} */
    @Override protected void writeFragmentData(CacheDataRow row, ByteBuffer buf, int rowOff,
        int payloadSize) throws IgniteCheckedException {
        assert row.mvccCounter() != MVCC_COUNTER_NA && row.mvccCoordinatorVersion() != 0;

        final int keySize = row.key().valueBytesLength(null);

        final int valSize = row.value().valueBytesLength(null);

        int written = writeFragment(row, buf, rowOff, payloadSize,
            MVCC_INFO, keySize, valSize);

        written += writeFragment(row, buf, rowOff + written, payloadSize - written, CACHE_ID, keySize, valSize);


        written += writeFragment(row, buf, rowOff + written, payloadSize - written,
            KEY, keySize, valSize);

        written += writeFragment(row, buf, rowOff + written, payloadSize - written,
            EXPIRE_TIME, keySize, valSize);
        written += writeFragment(row, buf, rowOff + written, payloadSize - written,
            DataPageIOUtils.EntryPart.VALUE, keySize, valSize);
        written += writeFragment(row, buf, rowOff + written, payloadSize - written,
            DataPageIOUtils.EntryPart.VERSION, keySize, valSize);

        assert written == payloadSize;
    }

    /**
     * Try to write fragment data.
     *
     * @param row Row.
     * @param buf Byte buffer.
     * @param rowOff Offset in row data bytes.
     * @param payloadSize Data length that should be written in this fragment.
     * @param type Type of the part of entry.
     * @param keySize Key size.
     * @param valSize Value size.
     * @return Actually written data.
     * @throws IgniteCheckedException If fail.
     */
    private int writeFragment(
        final CacheDataRow row,
        final ByteBuffer buf,
        final int rowOff,
        final int payloadSize,
        final DataPageIOUtils.EntryPart type,
        final int keySize,
        final int valSize
    ) throws IgniteCheckedException {
        if (payloadSize == 0)
            return 0;

        final int prevLen;
        final int curLen;

        int cacheIdSize = row.cacheId() == 0 ? 0 : 4;

        switch (type) {
            case MVCC_INFO:

                prevLen = 0;
                curLen = MVCC_INFO_SIZE;

                break;

            case CACHE_ID:
                prevLen = MVCC_INFO_SIZE;
                curLen = MVCC_INFO_SIZE + cacheIdSize;

                break;

            case KEY:
                prevLen = MVCC_INFO_SIZE + cacheIdSize;
                curLen = MVCC_INFO_SIZE + cacheIdSize + keySize;

                break;

            case EXPIRE_TIME:
                prevLen = MVCC_INFO_SIZE + cacheIdSize + keySize;
                curLen = MVCC_INFO_SIZE + cacheIdSize + keySize + 8;

                break;

            case VALUE:
                prevLen = MVCC_INFO_SIZE + cacheIdSize + keySize + 8;
                curLen = MVCC_INFO_SIZE + cacheIdSize + keySize + valSize + 8;

                break;

            case VERSION:
                prevLen = MVCC_INFO_SIZE + cacheIdSize + keySize + valSize + 8;
                curLen = MVCC_INFO_SIZE + cacheIdSize + keySize + valSize + CacheVersionIO.size(row.version(), false) + 8;

                break;

            default:
                throw new IllegalArgumentException("Unknown entry part type: " + type);
        }

        if (curLen <= rowOff)
            return 0;

        final int len = Math.min(curLen - rowOff, payloadSize);

        if (type == EXPIRE_TIME)
            writeExpireTimeFragment(buf, row.expireTime(), rowOff, len, prevLen);
        else if (type == CACHE_ID)
            writeCacheIdFragment(buf, row.cacheId(), rowOff, len, prevLen);
        else if (type == MVCC_INFO)
            writeMvccInfoFragment(buf, row.mvccCoordinatorVersion(), row.mvccCounter(),
                row.newMvccCoordinatorVersion(), row.newMvccCounter(), len);
        else if (type != DataPageIOUtils.EntryPart.VERSION) {
            // Write key or value.
            final CacheObject co = type == KEY ? row.key() : row.value();

            co.putValue(buf, rowOff - prevLen, len);
        }
        else
            writeVersionFragment(buf, row.version(), rowOff, len, prevLen);

        return len;
    }

    /**
     * @param buf Byte buffer.
     * @param mvccCrd Coordinator version.
     * @param mvccCnt Counter.
     * @param newMvccCrd New coordinator version.
     * @param newMvccCnt New counter version.
     * @param len Length.
     */
    private static void writeMvccInfoFragment(ByteBuffer buf, long mvccCrd, long mvccCnt, long newMvccCrd,
        long newMvccCnt, int len) {

        assert len >= MVCC_INFO_SIZE : "Mvcc info should fit on the one page!";

        // xid_min.
        buf.putLong(mvccCrd);
        buf.putLong(mvccCnt);

        // xid_max.
        buf.putLong(newMvccCrd);
        buf.putLong(newMvccCnt);
    }

    /** {@inheritDoc} */
    @Override public int getRowSize(CacheDataRow row) throws IgniteCheckedException {
        return DataPageIOUtils.getRowSize(row, row.cacheId() != 0);
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        sb.a("MvccDataPageIO [\n");
        printPageLayout(addr, pageSize, sb);
        sb.a("\n]");
    }

    /** {@inheritDoc} */
    @Override public int getHeaderSize() {
        return MVCC_INFO_SIZE;
    }
}
