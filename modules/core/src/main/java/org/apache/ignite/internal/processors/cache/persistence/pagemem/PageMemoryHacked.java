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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.InitNewPageRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointLockStateChecker;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.*;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.lang.GridInClosure3X;
import org.apache.ignite.internal.util.offheap.GridOffHeapOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.ignite.internal.util.GridUnsafe.wrapPointer;

/**
 * Page header structure is described by the following diagram.
 *
 * When page is not allocated (in a free list):
 * <pre>
 * +--------+------------------------------------------------------+
 * |8 bytes |         PAGE_SIZE + PAGE_OVERHEAD - 8 bytes          |
 * +--------+------------------------------------------------------+
 * |Next ptr|                      Page data                       |
 * +--------+------------------------------------------------------+
 * </pre>
 * <p/>
 * When page is allocated and is in use:
 * <pre>
 * +------------------+--------+--------+----+----+--------+--------+----------------------+
 * |     8 bytes      |8 bytes |8 bytes |4 b |4 b |8 bytes |8 bytes |       PAGE_SIZE      |
 * +------------------+--------+--------+----+----+--------+--------+----------------------+
 * | Marker/Timestamp |Rel ptr |Page ID |C ID|PIN | LOCK   |TMP BUF |       Page data      |
 * +------------------+--------+--------+----+----+--------+--------+----------------------+
 * </pre>
 *
 * Note that first 8 bytes of page header are used either for page marker or for next relative pointer depending
 * on whether the page is in use or not.
 */
@SuppressWarnings({"LockAcquiredButNotSafelyReleased", "FieldAccessedSynchronizedAndUnsynchronized"})
public class PageMemoryHacked implements PageMemoryEx {
    /** */
    public static final long PAGE_MARKER = 0x0000000000000001L;

    /** Relative pointer chunk index mask. */
    private static final long SEGMENT_INDEX_MASK = 0xFFFFFF0000000000L;

    /** Full relative pointer mask. */
    private static final long RELATIVE_PTR_MASK = 0xFFFFFFFFFFFFFFL;

    /** Dirty flag. */
    private static final long DIRTY_FLAG = 0x0100000000000000L;

    /** Invalid relative pointer value. */
    private static final long INVALID_REL_PTR = RELATIVE_PTR_MASK;

    /** Address mask to avoid ABA problem. */
    private static final long ADDRESS_MASK = 0xFFFFFFFFFFFFFFL;

    /** Counter mask to avoid ABA problem. */
    private static final long COUNTER_MASK = ~ADDRESS_MASK;

    /** Counter increment to avoid ABA problem. */
    private static final long COUNTER_INC = ADDRESS_MASK + 1;

    /** Page relative pointer. Does not change once a page is allocated. */
    public static final int RELATIVE_PTR_OFFSET = 8;

    /** Page ID offset */
    public static final int PAGE_ID_OFFSET = 16;

    /** Page cache group ID offset. */
    public static final int PAGE_CACHE_ID_OFFSET = 24;

    /** Page pin counter offset. */
    public static final int PAGE_PIN_CNT_OFFSET = 28;

    /** Page lock offset. */
    public static final int PAGE_LOCK_OFFSET = 32;

    /** Page temp copy buffer relative pointer offset. */
    public static final int PAGE_TMP_BUF_OFFSET = 40;

    /**
     * 8b Marker/timestamp
     * 8b Relative pointer
     * 8b Page ID
     * 4b Cache group ID
     * 4b Pin count
     * 8b Lock
     * 8b Temporary buffer
     */
    public static final int PAGE_OVERHEAD = 48;

    /** Tracking io. */
    private static final TrackingPageIO trackingIO = TrackingPageIO.VERSIONS.latest();

    /** Checkpoint pool overflow error message. */
    public static final String CHECKPOINT_POOL_OVERFLOW_ERROR_MSG = "Failed to allocate temporary buffer for checkpoint " +
        "(increase checkpointPageBufferSize configuration property)";

    /** Page size. */
    private final int sysPageSize;

    /** Shared context. */
    private final GridCacheSharedContext<?, ?> ctx;

    /** Checkpoint lock state provider. */
    private final CheckpointLockStateChecker stateChecker;

    /** Number of used pages in checkpoint buffer. */
    private final AtomicInteger cpBufPagesCntr = new AtomicInteger(0);

    /** */
    private ExecutorService asyncRunner = new ThreadPoolExecutor(
        0,
        Runtime.getRuntime().availableProcessors(),
        30L,
        TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(Runtime.getRuntime().availableProcessors()));

    /** Page store manager. */
    private IgnitePageStoreManager storeMgr;

    /** */
    private IgniteWriteAheadLogManager walMgr;

    /** */
    private final IgniteLogger log;

    /** Direct memory allocator. */
    private final DirectMemoryProvider directMemoryProvider;

    /** Segments array. */
    private Segment[] segments;

    /** */
    private PagePool checkpointPool;

    /** */
    private OffheapReadWriteLock rwLock;

    /**
     * Callback invoked to track changes in pages.
     * {@code Null} if page tracking functionality is disabled
     * */
    @Nullable private final GridInClosure3X<Long, FullPageId, PageMemoryEx> changeTracker;

    /** Write throttle type. */
    private ThrottlingPolicy throttlingPlc;

    /** */
    private long[] sizes;

    /** Memory metrics to track dirty pages count and page replace rate. */
    private DataRegionMetricsImpl memMetrics;

    /**
     * @param directMemoryProvider Memory allocator to use.
     * @param sizes segments sizes, last is checkpoint pool size.
     * @param ctx Cache shared context.
     * @param pageSize Page size.
     * @param changeTracker Callback invoked to track changes in pages.
     * @param stateChecker Checkpoint lock state provider. Used to ensure lock is held by thread, which modify pages.
     * @param memMetrics Memory metrics to track dirty pages count and page replace rate.
     * @param throttlingPlc Write throttle enabled and its type. Null equal to none.
     */
    public PageMemoryHacked(
        DirectMemoryProvider directMemoryProvider,
        long[] sizes,
        GridCacheSharedContext<?, ?> ctx,
        int pageSize,
        @Nullable GridInClosure3X<Long, FullPageId, PageMemoryEx> changeTracker,
        CheckpointLockStateChecker stateChecker,
        DataRegionMetricsImpl memMetrics,
        @Nullable ThrottlingPolicy throttlingPlc
    ) {
        assert ctx != null;
        assert pageSize > 0;

        log = ctx.logger(PageMemoryHacked.class);

        this.ctx = ctx;
        this.directMemoryProvider = directMemoryProvider;
        this.sizes = sizes;

        this.changeTracker = changeTracker;
        this.stateChecker = stateChecker;
        this.throttlingPlc = throttlingPlc != null ? throttlingPlc : ThrottlingPolicy.CHECKPOINT_BUFFER_ONLY;

        storeMgr = ctx.pageStore();
        walMgr = ctx.wal();

        assert storeMgr != null;
        assert walMgr != null;

        sysPageSize = pageSize + PAGE_OVERHEAD;

        rwLock = new OffheapReadWriteLock(128);

        this.memMetrics = memMetrics;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        directMemoryProvider.initialize(sizes);

        List<DirectMemoryRegion> regions = new ArrayList<>(sizes.length);

        while (true) {
            DirectMemoryRegion reg = directMemoryProvider.nextRegion();

            if (reg == null)
                break;

            regions.add(reg);
        }

        int regs = regions.size();

        segments = new Segment[regs - 1];

        DirectMemoryRegion cpReg = regions.get(regs - 1);

        checkpointPool = new PagePool(regs - 1, cpReg, cpBufPagesCntr);

        long checkpointBuf = cpReg.size();

        long totalAllocated = 0;
        int pages = 0;

        for (int i = 0; i < regs - 1; i++) {
            assert i < segments.length;

            DirectMemoryRegion reg = regions.get(i);

            totalAllocated += reg.size();

            segments[i] = new Segment(i, regions.get(i), checkpointPool.pages() / segments.length, throttlingPlc);

            segments[i].init();

            pages += segments[i].pages();
        }

        if (log.isInfoEnabled())
            log.info("Started page memory [memoryAllocated=" + U.readableSize(totalAllocated, false) +
                ", pages=" + pages +
                ", checkpointBuffer=" + U.readableSize(checkpointBuf, false) +
                ']');
    }

    /** {@inheritDoc} */
    @SuppressWarnings("OverlyStrongTypeCast")
    @Override public void stop() throws IgniteException {
        if (log.isDebugEnabled())
            log.debug("Stopping page memory.");

        U.shutdownNow(getClass(), asyncRunner, log);

        if (segments != null) {
            for (Segment seg : segments)
                seg.close();
        }

        directMemoryProvider.shutdown();
    }

    /** {@inheritDoc} */
    @Override public void releasePage(int grpId, long pageId, long page) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public long readLock(int grpId, long pageId, long page) {
        return readLockPage(page, new FullPageId(pageId, grpId), false);
    }

    /** {@inheritDoc} */
    @Override public void readUnlock(int grpId, long pageId, long page) {
        readUnlockPage(page);
    }

    /** {@inheritDoc} */
    @Override public long writeLock(int grpId, long pageId, long page) {
        return writeLock(grpId, pageId, page, false);
    }

    /** {@inheritDoc} */
    @Override public long writeLock(int grpId, long pageId, long page, boolean restore) {
        return writeLockPage(page, new FullPageId(pageId, grpId), !restore);
    }

    /** {@inheritDoc} */
    @Override public long tryWriteLock(int grpId, long pageId, long page) {
        return tryWriteLockPage(page, new FullPageId(pageId, grpId), true);
    }

    /** {@inheritDoc} */
    @Override public void writeUnlock(int grpId, long pageId, long page, Boolean walPlc,
        boolean dirtyFlag) {
        writeUnlock(grpId, pageId, page, walPlc, dirtyFlag, false);
    }

    /** {@inheritDoc} */
    @Override public void writeUnlock(int grpId, long pageId, long page, Boolean walPlc,
        boolean dirtyFlag, boolean restore) {
        writeUnlockPage(page, new FullPageId(pageId, grpId), walPlc, dirtyFlag, restore);
    }

    /** {@inheritDoc} */
    @Override public boolean isDirty(int grpId, long pageId, long page) {
        return isDirty(page);
    }

    /** {@inheritDoc} */
    @Override public long allocatePage(int grpId, int partId, byte flags) throws IgniteCheckedException {
        assert flags == PageIdAllocator.FLAG_DATA && partId <= PageIdAllocator.MAX_PARTITION_ID ||
            flags == PageIdAllocator.FLAG_IDX && partId == PageIdAllocator.INDEX_PARTITION :
            "flags = " + flags + ", partId = " + partId;

        assert stateChecker.checkpointLockIsHeldByThread();

        if (partId >= segments.length)
            throw new IllegalArgumentException("Unexpected [part=" + partId +
                ", parts=" + segments.length + ']');

        long pageId = storeMgr.allocatePage(grpId, partId, flags);

        assert PageIdUtils.pageIndex(pageId) > 0; //it's crucial for tracking pages (zero page is super one)

        // We need to allocate page in memory for marking it dirty to save it in the next checkpoint.
        // Otherwise it is possible that on file will be empty page which will be saved at snapshot and read with error
        // because there is no crc inside them.
        Segment seg = segment(grpId, pageId);

        long absPtr = seg.absolute(pageId);

        assert seg.empty(absPtr);

        FullPageId fullId = new FullPageId(pageId, grpId);

        boolean isTrackingPage = changeTracker != null && trackingIO.trackingPageFor(pageId, pageSize()) == pageId;

        rwLock.writeLock(absPtr + PAGE_LOCK_OFFSET, OffheapReadWriteLock.TAG_LOCK_ALWAYS);

        try {
            GridUnsafe.setMemory(absPtr + PAGE_OVERHEAD, pageSize(), (byte)0);

            PageHeader.fullPageId(absPtr, fullId);
            PageHeader.writeTimestamp(absPtr, U.currentTimeMillis());
            setDirty(fullId, absPtr, true, true);

            if (isTrackingPage) {
                long pageAddr = absPtr + PAGE_OVERHEAD;

                // We are inside segment write lock, so no other thread can pin this tracking page yet.
                // We can modify page buffer directly.
                if (PageIO.getType(pageAddr) == 0) {
                    trackingIO.initNewPage(pageAddr, pageId, pageSize());

                    if (!ctx.wal().disabled(fullId.groupId()))
                        if (!ctx.wal().isAlwaysWriteFullPages())
                            ctx.wal().log(
                                new InitNewPageRecord(
                                    grpId,
                                    pageId,
                                    trackingIO.getType(),
                                    trackingIO.getVersion(), pageId
                                )
                            );
                        else
                            ctx.wal().log(new PageSnapshot(fullId, absPtr + PAGE_OVERHEAD, pageSize()));
                }
            }

        }
        finally {
            rwLock.writeUnlock(absPtr + PAGE_LOCK_OFFSET, OffheapReadWriteLock.TAG_LOCK_ALWAYS);
        }

        //we have allocated 'tracking' page, we need to allocate regular one
        return isTrackingPage ? allocatePage(grpId, partId, flags) : pageId;
    }

    /**
     * @return Data region configuration.
     */
    private DataRegionConfiguration getDataRegionConfiguration() {
        DataStorageConfiguration memCfg = ctx.kernalContext().config().getDataStorageConfiguration();

        assert memCfg != null;

        String dataRegionName = memMetrics.getName();

        if (memCfg.getDefaultDataRegionConfiguration().getName().equals(dataRegionName))
            return memCfg.getDefaultDataRegionConfiguration();

        DataRegionConfiguration[] dataRegions = memCfg.getDataRegionConfigurations();

        if (dataRegions != null) {
            for (DataRegionConfiguration reg : dataRegions) {
                if (reg != null && reg.getName().equals(dataRegionName))
                    return reg;
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer pageBuffer(long pageAddr) {
        return wrapPointer(pageAddr, pageSize());
    }

    /** {@inheritDoc} */
    @Override public boolean freePage(int grpId, long pageId) {
        assert false : "Free page should be never called directly when persistence is enabled.";

        return false;
    }

    /** {@inheritDoc} */
    @Override public long metaPageId(int grpId) {
        return storeMgr.metaPageId(grpId);
    }

    /** {@inheritDoc} */
    @Override public long partitionMetaPageId(int grpId, int partId) {
        return PageIdUtils.pageId(partId, PageIdAllocator.FLAG_DATA, 0);
    }

    /** {@inheritDoc} */
    @Override public long acquirePage(int grpId, long pageId) throws IgniteCheckedException {
        return acquirePage(grpId, pageId, false);
    }

    /** {@inheritDoc} */
    @Override public long acquirePage(int grpId, long pageId, boolean restore) throws IgniteCheckedException {
        FullPageId fullId = new FullPageId(pageId, grpId);

        int partId = PageIdUtils.partId(pageId);

        if (partId >= segments.length)
            throw new IllegalArgumentException("Unexpected [part=" + partId +
                ", parts=" + segments.length + ']');

        Segment seg = segment(grpId, pageId);

        long absPtr = seg.absolute(pageId);

        if (seg.empty(absPtr)) {
            rwLock.writeLock(absPtr + PAGE_LOCK_OFFSET, OffheapReadWriteLock.TAG_LOCK_ALWAYS);

            long actualPageId = 0;

            try {
                if (!seg.empty(absPtr))
                    return absPtr;

                long pageAddr = absPtr + PAGE_OVERHEAD;

                ByteBuffer buf = wrapPointer(pageAddr, pageSize());

                try {
                    storeMgr.read(grpId, pageId, buf);

                    actualPageId = PageIO.getPageId(buf);

                    memMetrics.onPageRead();
                }
                catch (IgniteDataIntegrityViolationException ignore) {
                    U.warn(log, "Failed to read page (data integrity violation encountered, will try to " +
                        "restore using existing WAL) [fullPageId=" + fullId + ']');

                    buf.rewind();

                    tryToRestorePage(fullId, buf);

                    memMetrics.onPageRead();
                }

                seg.setInitialized(absPtr);
            }
            finally {
                rwLock.writeUnlock(absPtr + PAGE_LOCK_OFFSET,
                    actualPageId == 0 ? OffheapReadWriteLock.TAG_LOCK_ALWAYS : PageIdUtils.tag(actualPageId));
            }
        }

        return absPtr;
    }

    /**
     * Restores page from WAL page snapshot & delta records.
     *
     * @param fullId Full page ID.
     * @param buf Destination byte buffer. Note: synchronization to provide ByteBuffer safety should be done outside
     * this method.
     *
     * @throws IgniteCheckedException If failed to start WAL iteration, if incorrect page type observed in data, etc.
     * @throws StorageException If it was not possible to restore page, page not found in WAL.
     */
    private void tryToRestorePage(FullPageId fullId, ByteBuffer buf) throws IgniteCheckedException {
        Long tmpAddr = null;
        try {
            ByteBuffer curPage = null;
            ByteBuffer lastValidPage = null;

            try (WALIterator it = walMgr.replay(null)) {
                for (IgniteBiTuple<WALPointer, WALRecord> tuple : it) {
                    switch (tuple.getValue().type()) {
                        case PAGE_RECORD:
                            PageSnapshot snapshot = (PageSnapshot)tuple.getValue();

                            if (snapshot.fullPageId().equals(fullId)) {
                                if (tmpAddr == null) {
                                    assert snapshot.pageData().length <= pageSize() : snapshot.pageData().length;

                                    tmpAddr = GridUnsafe.allocateMemory(pageSize());
                                }

                                if (curPage == null)
                                    curPage = wrapPointer(tmpAddr, pageSize());

                                PageUtils.putBytes(tmpAddr, 0, snapshot.pageData());
                            }

                            break;

                        case CHECKPOINT_RECORD:
                            CheckpointRecord rec = (CheckpointRecord)tuple.getValue();

                            assert !rec.end();

                            if (curPage != null) {
                                lastValidPage = curPage;
                                curPage = null;
                            }

                            break;

                        case MEMORY_RECOVERY: // It means that previous checkpoint was broken.
                            curPage = null;

                            break;

                        default:
                            if (tuple.getValue() instanceof PageDeltaRecord) {
                                PageDeltaRecord deltaRecord = (PageDeltaRecord)tuple.getValue();

                                if (curPage != null
                                    && deltaRecord.pageId() == fullId.pageId()
                                    && deltaRecord.groupId() == fullId.groupId()) {
                                    assert tmpAddr != null;

                                    deltaRecord.applyDelta(this, tmpAddr);
                                }
                            }
                    }
                }
            }

            ByteBuffer restored = curPage == null ? lastValidPage : curPage;

            if (restored == null)
                throw new StorageException(String.format(
                    "Page is broken. Can't restore it from WAL. (grpId = %d, pageId = %X).",
                    fullId.groupId(), fullId.pageId()
                ));

            buf.put(restored);
        }
        finally {
            if (tmpAddr != null)
                GridUnsafe.freeMemory(tmpAddr);
        }
    }

    /** {@inheritDoc} */
    @Override public int pageSize() {
        return sysPageSize - PAGE_OVERHEAD;
    }

    /** {@inheritDoc} */
    @Override public int systemPageSize() {
        return sysPageSize;
    }

    /** {@inheritDoc} */
    @Override public boolean safeToUpdate() {
        if (segments != null) {
            for (Segment segment : segments)
                if (!segment.safeToUpdate())
                    return false;
        }

        return true;
    }

    /**
     * @param dirtyRatioThreshold Throttle threshold.
     */
    boolean shouldThrottle(double dirtyRatioThreshold) {
        for (Segment segment : segments) {
            if (segment.shouldThrottle(dirtyRatioThreshold))
                return true;
        }

        return false;
    }

    /**
     * @return Max dirty ratio from the segments.
     */
    double getDirtyPagesRatio() {
        double res = 0;

        for (Segment segment : segments) {
            res = Math.max(res, segment.getDirtyPagesRatio());
        }

        return res;
    }

    /**
     * @return Total pages can be placed in all segments.
     */
    public long totalPages() {
        long res = 0;

        for (Segment segment : segments) {
            res += segment.pages();
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public GridMultiCollectionWrapper<FullPageId> beginCheckpoint() throws IgniteException {
        if (segments == null)
            return new GridMultiCollectionWrapper<>(Collections.<FullPageId>emptyList());

        Collection[] collections = new Collection[segments.length];

        for (int i = 0; i < segments.length; i++) {
            Segment seg = segments[i];

            if (seg.segCheckpointPages != null)
                throw new IgniteException("Failed to begin checkpoint (it is already in progress).");

            collections[i] = seg.segCheckpointPages = seg.dirtyPages;

            seg.dirtyPages = new GridConcurrentHashSet<>();
        }

        memMetrics.resetDirtyPages();

        return new GridMultiCollectionWrapper<>(collections);
    }

    /**
     * @return {@code True} if throttling is enabled.
     */
    private boolean isThrottlingEnabled() {
        return throttlingPlc != ThrottlingPolicy.CHECKPOINT_BUFFER_ONLY && throttlingPlc != ThrottlingPolicy.DISABLED;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "TooBroadScope"})
    @Override public void finishCheckpoint() {
        if (segments == null)
            return;

        for (Segment seg : segments)
            seg.segCheckpointPages = null;
    }

    /** {@inheritDoc} */
    @Override public Integer getForCheckpoint(FullPageId fullId, ByteBuffer outBuf, CheckpointMetricsTracker tracker) {
        assert outBuf.remaining() == pageSize();

        Segment seg = segment(fullId.groupId(), fullId.pageId());

        long absPtr = seg.absolute(fullId.pageId());

        rwLock.readLock(absPtr + PAGE_LOCK_OFFSET, OffheapReadWriteLock.TAG_LOCK_ALWAYS);

        try {
            copyInBuffer(absPtr + PAGE_OVERHEAD, outBuf);
        }
        finally {
            rwLock.readUnlock(absPtr + PAGE_LOCK_OFFSET);
        }

        return 0;
    }

    /**
     * @param absPtr Absolute ptr.
     * @param buf Tmp buffer.
     */
    private void copyInBuffer(long absPtr, ByteBuffer buf) {
        if (buf.isDirect()) {
            long tmpPtr = GridUnsafe.bufferAddress(buf);

            GridUnsafe.copyMemory(absPtr + PAGE_OVERHEAD, tmpPtr, pageSize());

            assert GridUnsafe.getInt(absPtr + PAGE_OVERHEAD + 4) == 0; //TODO GG-11480
            assert GridUnsafe.getInt(tmpPtr + 4) == 0; //TODO GG-11480
        }
        else {
            byte[] arr = buf.array();

            assert arr != null;
            assert arr.length == pageSize();

            GridUnsafe.copyMemory(null, absPtr + PAGE_OVERHEAD, arr, GridUnsafe.BYTE_ARR_OFF, pageSize());
        }
    }

    /** {@inheritDoc} */
    @Override public int invalidate(int grpId, int partId) {
        int tag = 0;

        for (Segment seg : segments) {
            seg.writeLock().lock();

            try {
                int newTag = seg.incrementPartGeneration(grpId, partId);

                if (tag == 0)
                    tag = newTag;

                assert tag == newTag;
            }
            finally {
                seg.writeLock().unlock();
            }
        }

        return tag;
    }

    /** {@inheritDoc} */
    @Override public void onCacheGroupDestroyed(int grpId) {
        for (Segment seg : segments) {
            seg.writeLock().lock();

            try {
                seg.resetGroupPartitionsGeneration(grpId);
            }
            finally {
                seg.writeLock().unlock();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Void> clearAsync(
        LoadedPagesMap.KeyPredicate pred,
        boolean cleanDirty
    ) {
        return new GridFinishedFuture<>();
    }

    /** {@inheritDoc} */
    @Override public long loadedPages() {
        return 0;
    }

    /**
     * @return Total number of acquired pages.
     */
    public long acquiredPages() {
        long total = 0;

        for (Segment seg : segments) {
            seg.readLock().lock();

            try {
                if (seg.closed)
                    continue;

                total += seg.acquiredPages();
            }
            finally {
                seg.readLock().unlock();
            }
        }

        return total;
    }

    /**
     * @param absPtr Absolute pointer to read lock.
     * @param fullId Full page ID.
     * @param force Force flag.
     * @return Pointer to the page read buffer.
     */
    private long readLockPage(long absPtr, FullPageId fullId, boolean force) {
        return readLockPage(absPtr, fullId, force, true);
    }

    /**
     * @param absPtr Absolute pointer to read lock.
     * @param fullId Full page ID.
     * @param force Force flag.
     * @param touch Update page timestamp.
     * @return Pointer to the page read buffer.
     */
    private long readLockPage(long absPtr, FullPageId fullId, boolean force, boolean touch) {
        int tag = force ? -1 : PageIdUtils.tag(fullId.pageId());

        boolean locked = rwLock.readLock(absPtr + PAGE_LOCK_OFFSET, tag);

        if (!locked)
            return 0;

        if (touch)
            PageHeader.writeTimestamp(absPtr, U.currentTimeMillis());

        assert GridUnsafe.getInt(absPtr + PAGE_OVERHEAD + 4) == 0; //TODO GG-11480

        return absPtr + PAGE_OVERHEAD;
    }

    /** {@inheritDoc} */
    @Override public long readLockForce(int grpId, long pageId, long page) {
        return readLockPage(page, new FullPageId(pageId, grpId), true);
    }

    /**
     * @param absPtr Absolute pointer to unlock.
     */
    void readUnlockPage(long absPtr) {
        rwLock.readUnlock(absPtr + PAGE_LOCK_OFFSET);
    }

    /**
     * Checks if a page has temp copy buffer.
     *
     * @param absPtr Absolute pointer.
     * @return {@code True} if a page has temp buffer.
     */
    public boolean hasTempCopy(long absPtr) {
        return PageHeader.tempBufferPointer(absPtr) != INVALID_REL_PTR;
    }

    /**
     * @param absPtr Absolute pointer.
     * @return Pointer to the page write buffer or {@code 0} if page was not locked.
     */
    long tryWriteLockPage(long absPtr, FullPageId fullId, boolean checkTag) {
        int tag = checkTag ? PageIdUtils.tag(fullId.pageId()) : OffheapReadWriteLock.TAG_LOCK_ALWAYS;

        if (!rwLock.tryWriteLock(absPtr + PAGE_LOCK_OFFSET, tag))
            return 0;

        return postWriteLockPage(absPtr, fullId);
    }

    /**
     * @param absPtr Absolute pointer.
     * @return Pointer to the page write buffer.
     */
    private long writeLockPage(long absPtr, FullPageId fullId, boolean checkTag) {
        int tag = checkTag ? PageIdUtils.tag(fullId.pageId()) : OffheapReadWriteLock.TAG_LOCK_ALWAYS;

        boolean locked = rwLock.writeLock(absPtr + PAGE_LOCK_OFFSET, tag);

        return locked ? postWriteLockPage(absPtr, fullId) : 0;
    }

    /**
     * @param absPtr Absolute pointer.
     * @return Pointer to the page write buffer.
     */
    private long postWriteLockPage(long absPtr, FullPageId fullId) {
        PageHeader.writeTimestamp(absPtr, U.currentTimeMillis());

        // Create a buffer copy if the page is scheduled for a checkpoint.
        if (isInCheckpoint(fullId) && PageHeader.tempBufferPointer(absPtr) == INVALID_REL_PTR) {
            long tmpRelPtr = checkpointPool.borrowOrAllocateFreePage(fullId.pageId());

            if (tmpRelPtr == INVALID_REL_PTR) {
                rwLock.writeUnlock(absPtr + PAGE_LOCK_OFFSET, OffheapReadWriteLock.TAG_LOCK_ALWAYS);

                throw new IgniteException(CHECKPOINT_POOL_OVERFLOW_ERROR_MSG + ": " + memMetrics.getName());
            }

            // Pin the page until checkpoint is not finished.
            PageHeader.acquirePage(absPtr);

            long tmpAbsPtr = checkpointPool.absolute(tmpRelPtr);

            GridUnsafe.copyMemory(
                null,
                absPtr + PAGE_OVERHEAD,
                null,
                tmpAbsPtr + PAGE_OVERHEAD,
                pageSize()
            );

            assert PageIO.getType(tmpAbsPtr + PAGE_OVERHEAD) != 0 : "Invalid state. Type is 0! pageId = " + U.hexLong(fullId.pageId());
            assert PageIO.getVersion(tmpAbsPtr + PAGE_OVERHEAD) != 0 : "Invalid state. Version is 0! pageId = " + U.hexLong(fullId.pageId());

            PageHeader.dirty(absPtr, false);
            PageHeader.tempBufferPointer(absPtr, tmpRelPtr);

            assert GridUnsafe.getInt(absPtr + PAGE_OVERHEAD + 4) == 0; //TODO GG-11480
            assert GridUnsafe.getInt(tmpAbsPtr + PAGE_OVERHEAD + 4) == 0; //TODO GG-11480
        }

        assert GridUnsafe.getInt(absPtr + PAGE_OVERHEAD + 4) == 0; //TODO GG-11480

        return absPtr + PAGE_OVERHEAD;
    }

    /**
     * @param page Page pointer.
     * @param fullId full page ID.
     * @param markDirty set dirty flag to page.
     */
    private void writeUnlockPage(
        long page,
        FullPageId fullId,
        Boolean walPlc,
        boolean markDirty,
        boolean restore
    ) {
        boolean wasDirty = isDirty(page);

        //if page is for restore, we shouldn't mark it as changed
        if (!restore && markDirty && !wasDirty && changeTracker != null)
            changeTracker.apply(page, fullId, this);

        boolean pageWalRec = markDirty && walPlc != FALSE && (walPlc == TRUE || !wasDirty);

        assert GridUnsafe.getInt(page + PAGE_OVERHEAD + 4) == 0; //TODO GG-11480

        if (markDirty)
            setDirty(fullId, page, markDirty, false);

        beforeReleaseWrite(fullId, page + PAGE_OVERHEAD, pageWalRec);

        long pageId = PageIO.getPageId(page + PAGE_OVERHEAD);

        assert pageId != 0 : U.hexLong(PageHeader.readPageId(page));
        assert PageIO.getVersion(page + PAGE_OVERHEAD) != 0 : U.hexLong(pageId);
        assert PageIO.getType(page + PAGE_OVERHEAD) != 0 : U.hexLong(pageId);

        try {
            rwLock.writeUnlock(page + PAGE_LOCK_OFFSET, PageIdUtils.tag(pageId));
        }
        catch (AssertionError ex) {
            U.error(log, "Failed to unlock page [fullPageId=" + fullId + ", binPage=" + U.toHexString(page, systemPageSize()) + ']');

            throw ex;
        }
    }

    /**
     * @param absPtr Absolute pointer to the page.
     * @return {@code True} if write lock acquired for the page.
     */
    boolean isPageWriteLocked(long absPtr) {
        return rwLock.isWriteLocked(absPtr + PAGE_LOCK_OFFSET);
    }

    /**
     * @param absPtr Absolute pointer to the page.
     * @return {@code True} if read lock acquired for the page.
     */
    boolean isPageReadLocked(long absPtr) {
        return rwLock.isReadLocked(absPtr + PAGE_LOCK_OFFSET);
    }

    /**
     * @param pageId Page ID to check if it was added to the checkpoint list.
     * @return {@code True} if it was added to the checkpoint list.
     */
    boolean isInCheckpoint(FullPageId pageId) {
        Segment seg = segment(pageId.groupId(), pageId.pageId());

        Collection<FullPageId> pages0 = seg.segCheckpointPages;

        return pages0 != null && pages0.contains(pageId);
    }

    /**
     * @param fullPageId Page ID to clear.
     * @return {@code True} if remove successfully.
     */
    boolean clearCheckpoint(FullPageId fullPageId) {
        Segment seg = segment(fullPageId.groupId(), fullPageId.pageId());

        Collection<FullPageId> pages0 = seg.segCheckpointPages;

        assert pages0 != null;

        return pages0.remove(fullPageId);
    }

    /**
     * @param absPtr Absolute pointer.
     * @return {@code True} if page is dirty.
     */
    boolean isDirty(long absPtr) {
        return PageHeader.dirty(absPtr);
    }

    /**
     * Gets the number of active pages across all segments. Used for test purposes only.
     *
     * @return Number of active pages.
     */
    public int activePagesCount() {
        int total = 0;

        for (Segment seg : segments)
            total += seg.acquiredPages();

        return total;
    }

    /** {@inheritDoc} */
    @Override public int checkpointBufferPagesCount() {
        return cpBufPagesCntr.get();
    }

    /**
     * Number of used pages in checkpoint buffer.
     */
    public int checkpointBufferPagesSize() {
        return checkpointPool.pages();
    }

    /**
     * This method must be called in synchronized context.
     *
     * @param pageId full page ID.
     * @param absPtr Absolute pointer.
     * @param dirty {@code True} dirty flag.
     * @param forceAdd If this flag is {@code true}, then the page will be added to the dirty set regardless whether the
     * old flag was dirty or not.
     */
    private void setDirty(FullPageId pageId, long absPtr, boolean dirty, boolean forceAdd) {
        boolean wasDirty = PageHeader.dirty(absPtr, dirty);

        if (dirty) {
            assert stateChecker.checkpointLockIsHeldByThread();

            if (!wasDirty || forceAdd) {
                boolean added = segment(pageId.groupId(), pageId.pageId()).dirtyPages.add(pageId);

                if (added)
                    memMetrics.incrementDirtyPages();
            }
        }
        else {
            boolean rmv = segment(pageId.groupId(), pageId.pageId()).dirtyPages.remove(pageId);

            if (rmv)
                memMetrics.decrementDirtyPages();
        }
    }

    /**
     *
     */
    void beforeReleaseWrite(FullPageId pageId, long ptr, boolean pageWalRec) {
        if (walMgr != null && (pageWalRec || walMgr.isAlwaysWriteFullPages()) && !walMgr.disabled(pageId.groupId())) {
            try {
                walMgr.log(new PageSnapshot(pageId, ptr, pageSize()));
            }
            catch (IgniteCheckedException e) {
                // TODO ignite-db.
                throw new IgniteException(e);
            }
        }
    }

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @return Segment.
     */
    private Segment segment(int grpId, long pageId) {
        int idx = segmentIndex(grpId, pageId, segments.length);

        return segments[idx];
    }

    /**
     * @param pageId Page ID.
     * @return Segment index.
     */
    public static int segmentIndex(int grpId, long pageId, int segments) {
        pageId = PageIdUtils.effectivePageId(pageId);

        // Take a prime number larger than total number of partitions.
        int hash = U.hash(pageId * 65537 + grpId);

        return U.safeAbs(hash) % segments;
    }

    /**
     *
     */
    private class PagePool {
        /** Segment index. */
        protected final int idx;

        /** Direct memory region. */
        protected final DirectMemoryRegion region;

        /** Pool pages counter. */
        protected final AtomicInteger pagesCntr;

        /** */
        protected long lastAllocatedIdxPtr;

        /** Pointer to the address of the free page list. */
        protected long freePageListPtr;

        /** Pages base. */
        protected long pagesBase;

        /**
         * @param idx Index.
         * @param region Region
         * @param pagesCntr Pages counter.
         */
        protected PagePool(int idx, DirectMemoryRegion region, AtomicInteger pagesCntr) {
            this.idx = idx;
            this.region = region;
            this.pagesCntr = pagesCntr;

            long base = (region.address() + 7) & ~0x7;

            freePageListPtr = base;

            base += 8;

            lastAllocatedIdxPtr = base;

            base += 8;

            // Align page start by
            pagesBase = base;

            GridUnsafe.putLong(freePageListPtr, INVALID_REL_PTR);
            GridUnsafe.putLong(lastAllocatedIdxPtr, 1L);
        }

        /**
         * Allocates a new free page.
         *
         * @param pageId Page ID to to initialize.
         * @return Relative pointer to the allocated page.
         * @throws GridOffHeapOutOfMemoryException If failed to allocate new free page.
         */
        private long borrowOrAllocateFreePage(long pageId) throws GridOffHeapOutOfMemoryException {
            if (pagesCntr != null)
                pagesCntr.getAndIncrement();

            long relPtr = borrowFreePage();

            return relPtr != INVALID_REL_PTR ? relPtr : allocateFreePage(pageId);
        }

        /**
         * @return Relative pointer to a free page that was borrowed from the allocated pool.
         */
        private long borrowFreePage() {
            while (true) {
                long freePageRelPtrMasked = GridUnsafe.getLong(freePageListPtr);

                long freePageRelPtr = freePageRelPtrMasked & ADDRESS_MASK;

                if (freePageRelPtr != INVALID_REL_PTR) {
                    long freePageAbsPtr = absolute(freePageRelPtr);

                    long nextFreePageRelPtr = GridUnsafe.getLong(freePageAbsPtr) & ADDRESS_MASK;

                    long cnt = ((freePageRelPtrMasked & COUNTER_MASK) + COUNTER_INC) & COUNTER_MASK;

                    if (GridUnsafe.compareAndSwapLong(null, freePageListPtr, freePageRelPtrMasked, nextFreePageRelPtr | cnt)) {
                        GridUnsafe.putLong(freePageAbsPtr, PAGE_MARKER);

                        return freePageRelPtr;
                    }
                }
                else
                    return INVALID_REL_PTR;
            }
        }

        /**
         * @param pageId Page ID.
         * @return Relative pointer of the allocated page.
         * @throws GridOffHeapOutOfMemoryException If failed to allocate new free page.
         */
        private long allocateFreePage(long pageId) throws GridOffHeapOutOfMemoryException {
            long limit = region.address() + region.size();

            while (true) {
                long lastIdx = GridUnsafe.getLong(lastAllocatedIdxPtr);

                // Check if we have enough space to allocate a page.
                if (pagesBase + (lastIdx + 1) * sysPageSize > limit)
                    return INVALID_REL_PTR;

                if (GridUnsafe.compareAndSwapLong(null, lastAllocatedIdxPtr, lastIdx, lastIdx + 1)) {
                    long absPtr = pagesBase + lastIdx * sysPageSize;

                    assert (lastIdx & SEGMENT_INDEX_MASK) == 0L;

                    long relative = relative(lastIdx);

                    assert relative != INVALID_REL_PTR;

                    PageHeader.initNew(absPtr, relative);

                    rwLock.init(absPtr + PAGE_LOCK_OFFSET, PageIdUtils.tag(pageId));

                    return relative;
                }
            }
        }

        /**
         * @param relPtr Relative pointer to free.
         */
        private void releaseFreePage(long relPtr) {
            long absPtr = absolute(relPtr);

            assert !PageHeader.isAcquired(absPtr) : "Release pinned page: " + PageHeader.fullPageId(absPtr);

            if (pagesCntr != null)
                pagesCntr.getAndDecrement();

            while (true) {
                long freePageRelPtrMasked = GridUnsafe.getLong(freePageListPtr);

                long freePageRelPtr = freePageRelPtrMasked & RELATIVE_PTR_MASK;

                GridUnsafe.putLong(absPtr, freePageRelPtr);

                if (GridUnsafe.compareAndSwapLong(null, freePageListPtr, freePageRelPtrMasked, relPtr))
                    return;
            }
        }

        /**
         * @return Absolute pointer.
         */
        long absolute(long pageIdx) {
            long off = pageIdx * sysPageSize;

            return pagesBase + off;
        }

        /**
         * @param pageIdx Page index in the pool.
         * @return Relative pointer.
         */
        private long relative(long pageIdx) {
            return pageIdx | ((long)idx) << 40;
        }

        /**
         * @return Max number of pages in the pool.
         */
        private int pages() {
            return (int)((region.size() - (pagesBase - region.address())) / sysPageSize);
        }
    }

    /**
     *
     */
    private class Segment extends ReentrantReadWriteLock {
        /** */
        private static final long serialVersionUID = 0L;

        /** Pointer to acquired pages integer counter. */
        private long acquiredPagesPtr;

        /** */
        private PagePool pool;

        /** Pages marked as dirty since the last checkpoint. */
        private Collection<FullPageId> dirtyPages = new GridConcurrentHashSet<>();

        /** */
        private volatile Collection<FullPageId> segCheckpointPages;

        /** */
        private final int maxDirtyPages;

        /** Initial partition generation. */
        private static final int INIT_PART_GENERATION = 1;

        /** Maps partition (grpId, partId) to its generation. Generation is 1-based incrementing partition counter. */
        private final Map<GroupPartitionId, Integer> partGenerationMap = new HashMap<>();

        /** */
        private boolean closed;

        /** */
        private int pages;

        /**
         * @param region Memory region.
         * @param throttlingPlc policy determine if write throttling enabled and its type.
         */
        private Segment(int idx, DirectMemoryRegion region, int cpPoolPages, ThrottlingPolicy throttlingPlc) {
            long totalMemory = region.size();

            pages = (int)(totalMemory / sysPageSize);

            acquiredPagesPtr = region.address();

            GridUnsafe.putIntVolatile(null, acquiredPagesPtr, 0);

            DirectMemoryRegion poolRegion = region.slice(0);

            pool = new PagePool(idx, poolRegion, null);

            maxDirtyPages = throttlingPlc != ThrottlingPolicy.DISABLED
                ? pool.pages() * 3 / 4
                : Math.min(pool.pages() * 2 / 3, cpPoolPages);
        }

        /**
         * Closes the segment.
         */
        private void close() {
            writeLock().lock();

            try {
                closed = true;
            }
            finally {
                writeLock().unlock();
            }
        }

        private void init() {
            for (int i = 0; i < pages; i++) {
                long absPtr = pool.absolute(i);

                GridUnsafe.setMemory(absPtr, sysPageSize, (byte)0);

                rwLock.init(absPtr + PAGE_LOCK_OFFSET, 0);
            }
        }

        /**
         *
         */
        private boolean safeToUpdate() {
            return dirtyPages.size() < maxDirtyPages;
        }

        /**
         * @param dirtyRatioThreshold Throttle threshold.
         */
        private boolean shouldThrottle(double dirtyRatioThreshold) {
            return getDirtyPagesRatio() > dirtyRatioThreshold;
        }

        /**
         * @return dirtyRatio to be compared with Throttle threshold.
         */
        private double getDirtyPagesRatio() {
            return ((double)dirtyPages.size()) / pages();
        }

        /**
         * @return Max number of pages this segment can allocate.
         */
        private int pages() {
            return pool.pages();
        }

        /**
         * @param absPtr Page absolute address to acquire.
         */
        private void acquirePage(long absPtr) {
            PageHeader.acquirePage(absPtr);

            updateAtomicInt(acquiredPagesPtr, 1);
        }

        /**
         * @param absPtr Page absolute address to release.
         */
        private void releasePage(long absPtr) {
            PageHeader.releasePage(absPtr);

            updateAtomicInt(acquiredPagesPtr, -1);
        }

        /**
         * @return Total number of acquired pages.
         */
        private int acquiredPages() {
            return GridUnsafe.getInt(acquiredPagesPtr);
        }

        /**
         * @param pageId Page ID.
         * @return Page relative pointer.
         */
        private long borrowOrAllocateFreePage(long pageId) {
            return pool.borrowOrAllocateFreePage(pageId);
        }

        /**
         * Prepares a page removal for page replacement, if needed.
         *
         * @param fullPageId Candidate page full ID.
         * @param absPtr Absolute pointer of the page to evict.
         * @param saveDirtyPage implementation to save dirty page to persistent storage.
         * @return {@code True} if it is ok to replace this page, {@code false} if another page should be selected.
         * @throws IgniteCheckedException If failed to write page to the underlying store during eviction.
         */
        private boolean preparePageRemoval(FullPageId fullPageId, long absPtr, ReplacedPageWriter saveDirtyPage) throws IgniteCheckedException {
            assert writeLock().isHeldByCurrentThread();

            // Do not evict cache meta pages.
            if (fullPageId.pageId() == storeMgr.metaPageId(fullPageId.groupId()))
                return false;

            if (PageHeader.isAcquired(absPtr))
                return false;

            Collection<FullPageId> cpPages = segCheckpointPages;

            clearRowCache(fullPageId, absPtr);

            if (isDirty(absPtr)) {
                // Can evict a dirty page only if should be written by a checkpoint.
                // These pages does not have tmp buffer.
                if (cpPages != null && cpPages.contains(fullPageId)) {
                    assert storeMgr != null;

                    memMetrics.updatePageReplaceRate(U.currentTimeMillis() - PageHeader.readTimestamp(absPtr));

                    saveDirtyPage.writePage(
                        fullPageId,
                        wrapPointer(absPtr + PAGE_OVERHEAD, pageSize()),
                        partGeneration(
                            fullPageId.groupId(),
                            PageIdUtils.partId(fullPageId.pageId())
                        )
                    );

                    setDirty(fullPageId, absPtr, false, true);

                    cpPages.remove(fullPageId);

                    return true;
                }

                return false;
            }
            else {
                memMetrics.updatePageReplaceRate(U.currentTimeMillis() - PageHeader.readTimestamp(absPtr));

                // Page was not modified, ok to evict.
                return true;
            }
        }

        /**
         * @param fullPageId Full page ID to remove all links placed on the page from row cache.
         * @param absPtr Absolute pointer of the page to evict.
         * @throws IgniteCheckedException On error.
         */
        private void clearRowCache(FullPageId fullPageId, long absPtr) throws IgniteCheckedException {
            assert writeLock().isHeldByCurrentThread();

            if (ctx.kernalContext().query() == null || !ctx.kernalContext().query().moduleEnabled())
                return;

            long pageAddr = readLockPage(absPtr, fullPageId, true, false);

            try {
                if (PageIO.getType(pageAddr) != PageIO.T_DATA)
                    return;

                final GridQueryRowCacheCleaner cleaner = ctx.kernalContext().query()
                    .getIndexing().rowCacheCleaner(fullPageId.groupId());

                if (cleaner == null)
                    return;

                DataPageIO io = DataPageIO.VERSIONS.forPage(pageAddr);

                io.forAllItems(pageAddr, new DataPageIO.CC<Void>() {
                    @Override public Void apply(long link) {
                        cleaner.remove(link);

                        return null;
                    }
                });
            }
            finally {
                readUnlockPage(absPtr);
            }
        }

        /**
         * @param absPageAddr Absolute page address
         * @return {@code True} if page is related to partition metadata, which is loaded in saveStoreMetadata().
         */
        private boolean isStoreMetadataPage(long absPageAddr) {
            try {
                long dataAddr = absPageAddr + PAGE_OVERHEAD;

                int type = PageIO.getType(dataAddr);
                int ver = PageIO.getVersion(dataAddr);

                PageIO io = PageIO.getPageIO(type, ver);

                return io instanceof PagePartitionMetaIO
                    || io instanceof PagesListMetaIO
                    || io instanceof PagePartitionCountersIO;
            }
            catch (IgniteCheckedException ignored) {
                return false;
            }
        }

        /**
         * Delegate to the corresponding page pool.
         *
         * @param pageId Page ID.
         * @return Absolute pointer.
         */
        private long absolute(long pageId) {
            return pool.absolute(PageIdUtils.pageIndex(pageId));
        }

        /**
         * @param grpId Cache group ID.
         * @param partId Partition ID.
         * @return Partition generation. Growing, 1-based partition version. Changed
         */
        private int partGeneration(int grpId, int partId) {
            assert getReadHoldCount() > 0 || getWriteHoldCount() > 0;

            Integer tag = partGenerationMap.get(new GroupPartitionId(grpId, partId));

            assert tag == null || tag >= 0 : "Negative tag=" + tag;

            return tag == null ? INIT_PART_GENERATION : tag;
        }

        /**
         * Increments partition generation due to partition invalidation (e.g. partition was rebalanced to other node
         * and evicted).
         *
         * @param grpId Cache group ID.
         * @param partId Partition ID.
         * @return New partition generation.
         */
        private int incrementPartGeneration(int grpId, int partId) {
            assert getWriteHoldCount() > 0;

            GroupPartitionId grpPart = new GroupPartitionId(grpId, partId);

            Integer gen = partGenerationMap.get(grpPart);

            if (gen == null)
                gen = INIT_PART_GENERATION;

            if (gen == Integer.MAX_VALUE) {
                U.warn(log, "Partition tag overflow [grpId=" + grpId + ", partId=" + partId + "]");

                partGenerationMap.put(grpPart, 0);

                return 0;
            }
            else {
                partGenerationMap.put(grpPart, gen + 1);

                return gen + 1;
            }
        }

        /**
         * @param grpId Cache group id.
         */
        private void resetGroupPartitionsGeneration(int grpId) {
            assert getWriteHoldCount() > 0;

            partGenerationMap.keySet().removeIf(grpPart -> grpPart.getGroupId() == grpId);
        }

        public boolean empty(long absPtr) {
            return !PageHeader.isAcquired(absPtr);
        }

        public void setInitialized(long absPtr) {
            PageHeader.acquirePage(absPtr);
        }
    }

    /**
     * Gets an estimate for the amount of memory required to store the given number of page IDs
     * in a segment table.
     *
     * @param pages Number of pages to store.
     * @return Memory size estimate.
     */
    private static long requiredSegmentTableMemory(int pages) {
        return FullPageIdTable.requiredMemory(pages) + 8;
    }

    /**
     * @param ptr Pointer to update.
     * @param delta Delta.
     */
    private static int updateAtomicInt(long ptr, int delta) {
        while (true) {
            int old = GridUnsafe.getInt(ptr);

            int updated = old + delta;

            if (GridUnsafe.compareAndSwapInt(null, ptr, old, updated))
                return updated;
        }
    }

    /**
     * @param ptr Pointer to update.
     * @param delta Delta.
     */
    private static long updateAtomicLong(long ptr, long delta) {
        while (true) {
            long old = GridUnsafe.getLong(ptr);

            long updated = old + delta;

            if (GridUnsafe.compareAndSwapLong(null, ptr, old, updated))
                return updated;
        }
    }

    /**
     *
     */
    private static class PageHeader {
        /**
         * @param absPtr Absolute pointer to initialize.
         * @param relative Relative pointer to write.
         */
        private static void initNew(long absPtr, long relative) {
            relative(absPtr, relative);

            tempBufferPointer(absPtr, INVALID_REL_PTR);

            GridUnsafe.putLong(absPtr, PAGE_MARKER);
            GridUnsafe.putInt(absPtr + PAGE_PIN_CNT_OFFSET, 0);
        }

        /**
         * @param absPtr Absolute pointer.
         * @return Dirty flag.
         */
        private static boolean dirty(long absPtr) {
            return flag(absPtr, DIRTY_FLAG);
        }

        /**
         * @param absPtr Page absolute pointer.
         * @param dirty Dirty flag.
         * @return Previous value of dirty flag.
         */
        private static boolean dirty(long absPtr, boolean dirty) {
            return flag(absPtr, DIRTY_FLAG, dirty);
        }

        /**
         * @param absPtr Absolute pointer.
         * @param flag Flag mask.
         * @return Flag value.
         */
        private static boolean flag(long absPtr, long flag) {
            assert (flag & 0xFFFFFFFFFFFFFFL) == 0;
            assert Long.bitCount(flag) == 1;

            long relPtrWithFlags = GridUnsafe.getLong(absPtr + RELATIVE_PTR_OFFSET);

            return (relPtrWithFlags & flag) != 0;
        }

        /**
         * Sets flag.
         *
         * @param absPtr Absolute pointer.
         * @param flag Flag mask.
         * @param set New flag value.
         * @return Previous flag value.
         */
        private static boolean flag(long absPtr, long flag, boolean set) {
            assert (flag & 0xFFFFFFFFFFFFFFL) == 0;
            assert Long.bitCount(flag) == 1;

            long relPtrWithFlags = GridUnsafe.getLong(absPtr + RELATIVE_PTR_OFFSET);

            boolean was = (relPtrWithFlags & flag) != 0;

            if (set)
                relPtrWithFlags |= flag;
            else
                relPtrWithFlags &= ~flag;

            GridUnsafe.putLong(absPtr + RELATIVE_PTR_OFFSET, relPtrWithFlags);

            return was;
        }

        /**
         * @param absPtr Page pointer.
         * @return If page is pinned.
         */
        private static boolean isAcquired(long absPtr) {
            return GridUnsafe.getInt(absPtr + PAGE_PIN_CNT_OFFSET) > 0;
        }

        /**
         * @param absPtr Absolute pointer.
         */
        private static void acquirePage(long absPtr) {
            updateAtomicInt(absPtr + PAGE_PIN_CNT_OFFSET, 1);
        }

        /**
         * @param absPtr Absolute pointer.
         */
        private static int releasePage(long absPtr) {
            return updateAtomicInt(absPtr + PAGE_PIN_CNT_OFFSET, -1);
        }

        /**
         * Reads relative pointer from the page at the given absolute position.
         *
         * @param absPtr Absolute memory pointer to the page header.
         * @return Relative pointer written to the page.
         */
        private static long readRelative(long absPtr) {
            return GridUnsafe.getLong(absPtr + RELATIVE_PTR_OFFSET) & RELATIVE_PTR_MASK;
        }

        /**
         * Writes relative pointer to the page at the given absolute position.
         *
         * @param absPtr Absolute memory pointer to the page header.
         * @param relPtr Relative pointer to write.
         */
        private static void relative(long absPtr, long relPtr) {
            GridUnsafe.putLong(absPtr + RELATIVE_PTR_OFFSET, relPtr & RELATIVE_PTR_MASK);
        }

        /**
         * Volatile write for current timestamp to page in {@code absAddr} address.
         *
         * @param absPtr Absolute page address.
         */
        private static void writeTimestamp(final long absPtr, long tstamp) {
            tstamp >>= 8;

            GridUnsafe.putLongVolatile(null, absPtr, (tstamp << 8) | 0x01);
        }

        /**
         * Read for timestamp from page in {@code absAddr} address.
         *
         * @param absPtr Absolute page address.
         * @return Timestamp.
         */
        private static long readTimestamp(final long absPtr) {
            long markerAndTs = GridUnsafe.getLong(absPtr);

            // Clear last byte as it is occupied by page marker.
            return markerAndTs & ~0xFF;
        }

        /**
         * Sets pointer to checkpoint buffer.
         *
         * @param absPtr Page absolute pointer.
         * @param tmpRelPtr Temp buffer relative pointer or {@link #INVALID_REL_PTR} if page is not copied to checkpoint
         * buffer.
         */
        private static void tempBufferPointer(long absPtr, long tmpRelPtr) {
            GridUnsafe.putLong(absPtr + PAGE_TMP_BUF_OFFSET, tmpRelPtr);
        }

        /**
         * Gets pointer to checkpoint buffer or {@link #INVALID_REL_PTR} if page is not copied to checkpoint buffer.
         *
         * @param absPtr Page absolute pointer.
         * @return Temp buffer relative pointer.
         */
        private static long tempBufferPointer(long absPtr) {
            return GridUnsafe.getLong(absPtr + PAGE_TMP_BUF_OFFSET);
        }

        /**
         * Reads page ID from the page at the given absolute position.
         *
         * @param absPtr Absolute memory pointer to the page header.
         * @return Page ID written to the page.
         */
        private static long readPageId(long absPtr) {
            return GridUnsafe.getLong(absPtr + PAGE_ID_OFFSET);
        }

        /**
         * Writes page ID to the page at the given absolute position.
         *
         * @param absPtr Absolute memory pointer to the page header.
         * @param pageId Page ID to write.
         */
        private static void pageId(long absPtr, long pageId) {
            GridUnsafe.putLong(absPtr + PAGE_ID_OFFSET, pageId);
        }

        /**
         * Reads cache group ID from the page at the given absolute pointer.
         *
         * @param absPtr Absolute memory pointer to the page header.
         * @return Cache group ID written to the page.
         */
        private static int readPageGroupId(final long absPtr) {
            return GridUnsafe.getInt(absPtr + PAGE_CACHE_ID_OFFSET);
        }

        /**
         * Writes cache group ID from the page at the given absolute pointer.
         *
         * @param absPtr Absolute memory pointer to the page header.
         * @param grpId Cache group ID to write.
         */
        private static void pageGroupId(final long absPtr, final int grpId) {
            GridUnsafe.putInt(absPtr + PAGE_CACHE_ID_OFFSET, grpId);
        }

        /**
         * Reads page ID and cache group ID from the page at the given absolute pointer.
         *
         * @param absPtr Absolute memory pointer to the page header.
         * @return Full page ID written to the page.
         */
        private static FullPageId fullPageId(final long absPtr) {
            return new FullPageId(readPageId(absPtr), readPageGroupId(absPtr));
        }

        /**
         * Writes page ID and cache group ID from the page at the given absolute pointer.
         *
         * @param absPtr Absolute memory pointer to the page header.
         * @param fullPageId Full page ID to write.
         */
        private static void fullPageId(final long absPtr, final FullPageId fullPageId) {
            pageId(absPtr, fullPageId.pageId());

            pageGroupId(absPtr, fullPageId.groupId());
        }
    }

    /**
     * Throttling enabled and its type enum.
     */
    public enum ThrottlingPolicy {
        /** All ways of throttling are disabled. */
        DISABLED,
        /** Only exponential throttling is used to protect from CP buffer overflow. */
        CHECKPOINT_BUFFER_ONLY,
        /** Target ratio based: CP progress is used as border. */
        TARGET_RATIO_BASED,
        /** Speed based. CP writting speed and estimated ideal speed are used as border */
        SPEED_BASED
    }
}
