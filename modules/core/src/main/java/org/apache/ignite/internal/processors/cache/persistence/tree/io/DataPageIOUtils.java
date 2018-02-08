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
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor.MVCC_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.MvccDataPageIO.MVCC_INFO_SIZE;

/**
 * Utils for data page IO.
 */
class DataPageIOUtils {

    /**
     * Private constructor.
     */
    private DataPageIOUtils() {
    }

    /**
     * Writes full row to data page.
     *
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @param payloadSize Payload size.
     * @param row Data row.
     * @param newRow {@code False} if existing cache entry is updated, in this case skip key data write.
     * @param writeMvcc {@code True} if need to write MVCC xid_min and xid_max.
     * @throws IgniteCheckedException If failed.
     */
    static void writeRowData(
        long pageAddr,
        int dataOff,
        int payloadSize,
        CacheDataRow row,
        boolean newRow,
        final boolean writeMvcc
    ) throws IgniteCheckedException {
        assert !writeMvcc || (row.mvccCounter() != MVCC_COUNTER_NA && row.mvccCoordinatorVersion() != 0);

        long addr = pageAddr + dataOff;

        int cacheIdSize = row.cacheId() != 0 ? 4 : 0;
        int mvccInfoSize = writeMvcc ? MVCC_INFO_SIZE : 0;

        if (newRow) {
            PageUtils.putShort(addr, 0, (short)payloadSize);
            addr += 2;

            if (writeMvcc) {
                // xid_min.
                PageUtils.putLong(addr, 0, row.mvccCoordinatorVersion());
                PageUtils.putLong(addr, 8, row.mvccCounter());

                // empty xid_max.
                PageUtils.putLong(addr, 16, 0);
                PageUtils.putLong(addr, 24, MVCC_COUNTER_NA);

                addr += mvccInfoSize;
            }

            if (cacheIdSize != 0) {
                PageUtils.putInt(addr, 0, row.cacheId());

                addr += cacheIdSize;
            }

            addr += row.key().putValue(addr);

            if (row.removed())
                return;
        }
        else {
            assert !row.removed() : row;

            addr += (2 + mvccInfoSize + cacheIdSize  + row.key().valueBytesLength(null));
        }

        addr += row.value().putValue(addr);

        CacheVersionIO.write(addr, row.version(), false);
        addr += CacheVersionIO.size(row.version(), false);

        PageUtils.putLong(addr, 0, row.expireTime());
    }

    /**
     * Writes row fragment to data page.
     *
     * @param row Data row.
     * @param buf Byte buffer.
     * @param rowOff Offset in row data bytes.
     * @param payloadSize Data length that should be written in this fragment.
     * @param writeMvcc {@code True} if need to write MVCC xid_min and xid_max.
     * @throws IgniteCheckedException If fail.
     */
    static void writeFragmentData(
        final CacheDataRow row,
        final ByteBuffer buf,
        final int rowOff,
        final int payloadSize,
        final boolean writeMvcc
    ) throws IgniteCheckedException {
        assert !writeMvcc || (row.mvccCounter() != MVCC_COUNTER_NA && row.mvccCoordinatorVersion() != 0);

        final int keySize = row.key().valueBytesLength(null);

        boolean rmvd = row.removed();

        final int valSize = rmvd ? 0 : row.value().valueBytesLength(null);

        int written = writeFragment(row, buf, rowOff, payloadSize,
            EntryPart.CACHE_ID, keySize, valSize, writeMvcc);

        if (writeMvcc)
            written += writeFragment(row, buf, rowOff + written, payloadSize - written,
                EntryPart.MVCC_INFO, keySize, valSize, writeMvcc);

        written += writeFragment(row, buf, rowOff + written, payloadSize - written,
            EntryPart.KEY, keySize, valSize, writeMvcc);

        if (!rmvd) {
            written += writeFragment(row, buf, rowOff + written, payloadSize - written,
                EntryPart.EXPIRE_TIME, keySize, valSize, writeMvcc);
            written += writeFragment(row, buf, rowOff + written, payloadSize - written,
                EntryPart.VALUE, keySize, valSize, writeMvcc);
            written += writeFragment(row, buf, rowOff + written, payloadSize - written,
                EntryPart.VERSION, keySize, valSize, writeMvcc);
        }

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
     * @param writeMvcc {@code True} if need to write MVCC xid_min and xid_max.
     * @return Actually written data.
     * @throws IgniteCheckedException If fail.
     */
    private static int writeFragment(
        final CacheDataRow row,
        final ByteBuffer buf,
        final int rowOff,
        final int payloadSize,
        final EntryPart type,
        final int keySize,
        final int valSize,
        final boolean writeMvcc
    ) throws IgniteCheckedException {
        if (payloadSize == 0)
            return 0;

        final int prevLen;
        final int curLen;

        int cacheIdSize = row.cacheId() == 0 ? 0 : 4;
        int mvccInfoSize = writeMvcc ? MVCC_INFO_SIZE : 0;

        switch (type) {
            case MVCC_INFO:
                assert writeMvcc;

                prevLen = 0;//cacheIdSize;
                curLen = mvccInfoSize;//cacheIdSize + mvccInfoSize;

                break;

            case CACHE_ID:
                prevLen = mvccInfoSize;
                curLen = mvccInfoSize + cacheIdSize;

                break;

            case KEY:
                prevLen = mvccInfoSize + cacheIdSize;
                curLen = mvccInfoSize + cacheIdSize + keySize;

                break;

            case EXPIRE_TIME:
                prevLen = mvccInfoSize + cacheIdSize + keySize;
                curLen = cacheIdSize + mvccInfoSize + keySize + 8;

                break;

            case VALUE:
                prevLen = mvccInfoSize + cacheIdSize + keySize + 8;
                curLen = mvccInfoSize + cacheIdSize + keySize + valSize + 8;

                break;

            case VERSION:
                prevLen = mvccInfoSize + cacheIdSize + keySize + valSize + 8;
                curLen = mvccInfoSize + cacheIdSize + keySize + valSize + CacheVersionIO.size(row.version(), false) + 8;

                break;

            default:
                throw new IllegalArgumentException("Unknown entry part type: " + type);
        }

        if (curLen <= rowOff)
            return 0;

        final int len = Math.min(curLen - rowOff, payloadSize);

        if (type == EntryPart.EXPIRE_TIME)
            writeExpireTimeFragment(buf, row.expireTime(), rowOff, len, prevLen);
        else if (type == EntryPart.CACHE_ID)
            writeCacheIdFragment(buf, row.cacheId(), rowOff, len, prevLen);
        else if (type == EntryPart.MVCC_INFO)
            writeMvccInfoFragment(buf, row.mvccCoordinatorVersion(), row.mvccCounter(), rowOff, len, prevLen);
        else if (type != EntryPart.VERSION) {
            // Write key or value.
            final CacheObject co = type == EntryPart.KEY ? row.key() : row.value();

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
     * @param rowOff Row offset.
     * @param len Length.
     * @param prevLen Previous length.
     */
    private static void writeMvccInfoFragment(ByteBuffer buf, long mvccCrd, long mvccCnt, int rowOff, int len, int prevLen) {
        int size = MVCC_INFO_SIZE;

        if (size <= len) {
            // xid_min.
            buf.putLong(mvccCrd);
            buf.putLong(mvccCnt);

            // empty xid_max.
            buf.putLong(0);
            buf.putLong(MVCC_COUNTER_NA);
        }
        else {
            ByteBuffer mvccBuf = ByteBuffer.allocate(size);

            mvccBuf.order(buf.order());

            // xid_min.
            mvccBuf.putLong(mvccCrd);
            mvccBuf.putLong(mvccCnt);

            // empty xid_max.
            mvccBuf.putLong(0);
            mvccBuf.putLong(MVCC_COUNTER_NA);

            buf.put(mvccBuf.array(), rowOff - prevLen, len);
        }
    }

    /**
     * @param buf Byte buffer.
     * @param ver Version.
     * @param rowOff Row offset.
     * @param len Length.
     * @param prevLen previous length.
     */
    private static void writeVersionFragment(ByteBuffer buf, GridCacheVersion ver, int rowOff, int len, int prevLen) {
        int verSize = CacheVersionIO.size(ver, false);

        assert len <= verSize: len;

        if (verSize == len) { // Here we check for equality but not <= because version is the last.
            // Here we can write version directly.
            CacheVersionIO.write(buf, ver, false);
        }
        else {
            // We are in the middle of cache version.
            ByteBuffer verBuf = ByteBuffer.allocate(verSize);

            verBuf.order(buf.order());

            CacheVersionIO.write(verBuf, ver, false);

            buf.put(verBuf.array(), rowOff - prevLen, len);
        }
    }

    /**
     * @param buf Byte buffer.
     * @param expireTime Expire time.
     * @param rowOff Row offset.
     * @param len Length.
     * @param prevLen previous length.
     */
    private static void writeExpireTimeFragment(ByteBuffer buf, long expireTime, int rowOff, int len, int prevLen) {
        int size = 8;
        if (size <= len)
            buf.putLong(expireTime);
        else {
            ByteBuffer timeBuf = ByteBuffer.allocate(size);

            timeBuf.order(buf.order());

            timeBuf.putLong(expireTime);

            buf.put(timeBuf.array(), rowOff - prevLen, len);
        }
    }

    /**
     * @param buf Buffer.
     * @param cacheId Cache ID.
     * @param rowOff Row offset.
     * @param len Length.
     * @param prevLen Prev length.
     */
    private static void writeCacheIdFragment(ByteBuffer buf, int cacheId, int rowOff, int len, int prevLen) {
        if (cacheId == 0)
            return;

        int size = 4;

        if (size <= len)
            buf.putInt(cacheId);
        else {
            ByteBuffer cacheIdBuf = ByteBuffer.allocate(size);

            cacheIdBuf.order(buf.order());

            cacheIdBuf.putInt(cacheId);

            buf.put(cacheIdBuf.array(), rowOff - prevLen, len);
        }
    }

    /**
     *
     */
    enum EntryPart {
        /** */
        KEY,

        /** */
        VALUE,

        /** */
        VERSION,

        /** */
        EXPIRE_TIME,

        /** */
        CACHE_ID,

        /** */
        MVCC_INFO
    }
}
