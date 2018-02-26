/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointLockStateChecker;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.internal.util.lang.GridInClosure3X;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;

import static org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl.CHECKPOINT_POOL_OVERFLOW_ERROR_MSG;

/**
 *
 */
public class PageMemoryImplTest extends GridCommonAbstractTest {
    /** Mb. */
    private static final long MB = 1024 * 1024;

    /** Page size. */
    private static final int PAGE_SIZE = 1024;

    /**
     * @throws Exception if failed.
     */
    public void testThatAllocationTooMuchPagesCauseToOOMException() throws Exception {
        PageMemoryImpl memory = createPageMemory();

        try {
            while (!Thread.currentThread().isInterrupted())
                memory.allocatePage(1, PageIdAllocator.INDEX_PARTITION, PageIdAllocator.FLAG_IDX);
        }
        catch (IgniteOutOfMemoryException ignore) {
            //Success
        }

        assertFalse(memory.safeToUpdate());
    }

    /**
     *
     * @throws Exception if failed
     */
    public void testCheckpointBufferOverusageDontCauseWriteLockLeak() throws Exception {
        PageMemoryImpl memory = createPageMemory();

        List<FullPageId> pages = new ArrayList<>();

        try {
            while (!Thread.currentThread().isInterrupted()) {
                long pageId = memory.allocatePage(1, PageIdAllocator.INDEX_PARTITION, PageIdAllocator.FLAG_IDX);

                FullPageId fullPageId = new FullPageId(pageId, 1);

                pages.add(fullPageId);

                releaseWriteLock(memory, fullPageId); //to set page id, otherwise we would fail with assertion error
            }
        }
        catch (IgniteOutOfMemoryException ignore) {
            //Success
        }

        memory.beginCheckpoint();

        FullPageId lastPage = null;

        try {
            for (FullPageId fullPageId : pages) {
                lastPage = fullPageId;

                releaseWriteLock(memory, fullPageId);
            }
        }
        catch (Exception ex) {
            assertEquals(CHECKPOINT_POOL_OVERFLOW_ERROR_MSG, ex.getMessage());
        }

        memory.finishCheckpoint();


        releaseWriteLock(memory, lastPage); //we should be able get lock again
    }

    /**
     * @param memory Memory.
     * @param fullPageId Full page id.
     */
    private void releaseWriteLock(PageMemoryImpl memory, FullPageId fullPageId) throws IgniteCheckedException {
        long page = memory.acquirePage(1, fullPageId.pageId());

        long address = memory.writeLock(1, fullPageId.pageId(), page);

        PageIO.setPageId(address, fullPageId.pageId());

        PageIO.setType(address, PageIO.T_BPLUS_META);

        PageUtils.putShort(address, PageIO.VER_OFF, (short)1);

        memory.writeUnlock(1, fullPageId.pageId(), page, Boolean.FALSE, true);

        memory.releasePage(1, fullPageId.pageId(), page);
    }

    /**
     *
     */
    private PageMemoryImpl createPageMemory() throws Exception {
        long[] sizes = new long[5];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = 1024 * MB / 4;

        sizes[4] = 10 * MB;

        DirectMemoryProvider provider = new UnsafeMemoryProvider(log);

        IgniteConfiguration igniteCfg = new IgniteConfiguration();
        igniteCfg.setDataStorageConfiguration(new DataStorageConfiguration());

        GridTestKernalContext kernalCtx = new GridTestKernalContext(new GridTestLog4jLogger(), igniteCfg);
        kernalCtx.add(new IgnitePluginProcessor(kernalCtx, igniteCfg, Collections.<PluginProvider>emptyList()));

        GridCacheSharedContext<Object, Object> sharedCtx = new GridCacheSharedContext<>(
            kernalCtx,
            null,
            null,
            null,
            new NoOpPageStoreManager(),
            new NoOpWALManager(),
            null,
            new IgniteCacheDatabaseSharedManager(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        PageMemoryImpl mem = new PageMemoryImpl(
            provider,
            sizes,
            sharedCtx,
            PAGE_SIZE,
            (fullPageId, byteBuf, tag) -> {
                assert false : "No page replacement (rotation with disk) should happen during the test";
            },
            new GridInClosure3X<Long, FullPageId, PageMemoryEx>() {
                @Override public void applyx(Long page, FullPageId fullId, PageMemoryEx pageMem) {
                }
            }, new CheckpointLockStateChecker() {
                @Override public boolean checkpointLockIsHeldByThread() {
                    return true;
                }
            },
            new DataRegionMetricsImpl(igniteCfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration()),
            PageMemoryImpl.ThrottlingPolicy.NONE,
            null
        );

        mem.start();

        return mem;
    }
}
