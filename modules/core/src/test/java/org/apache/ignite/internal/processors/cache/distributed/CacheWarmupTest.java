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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;

/**
 *
 */
public class CacheWarmupTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 16));

        cfg.setCacheConfiguration(ccfg);

        cfg.setActiveOnStart(false);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setPageSize(4096)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(500 * 1024 * 1024))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /**
     * @throws Exception if failed.
     */
    public void testWarmup() throws Exception {
        try {
            startGrids(1);

            grid(0).cluster().active(true);

//            int total = 0, max = 500_000;
//
//            int k = 0;
//
//            // Put all in one partition.
//            while(total < max) {
//                IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);
//
//                if (grid(0).affinity(DEFAULT_CACHE_NAME).partition(k) == 0) {
//                    total++;
//
//                    cache.put(k, new byte[512]);
//
//                    if (total % 1000 == 0)
//                        log.info(String.format("Loaded %d of %d", total, max));
//                }
//
//                k++;
//            }
//
//            forceCheckpoint();

            long t0 = System.nanoTime();

            GridDhtCacheAdapter<Object, Object> dht = ((IgniteKernal)grid(0)).internalCache(DEFAULT_CACHE_NAME).context().dht();

            GridDhtLocalPartition part = dht.topology().localPartition(0);

            assertNotNull(part);

            assertTrue(part.state() == OWNING);

            preloadPartition(dht.context().group(), part);

            System.out.println(String.format("Finished preloading: duration=%f ms", (System.nanoTime() - t0) / 1000 / 1000.));

            t0 = System.nanoTime();

            ScanQuery<Integer, Integer> sq = new ScanQuery<>();
            sq.setLocal(true);
            sq.setPartition(0);

            QueryCursor<Cache.Entry<Integer, Integer>> query = grid(0).cache(DEFAULT_CACHE_NAME).query(sq);

            int i = 0;

            for (Cache.Entry<Integer, Integer> entry : query)
                i++;

            System.out.println(String.format("Finished reading: cnt=%d, duration=%f ms", i, (System.nanoTime() - t0) / 1000 / 1000.));
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * @param grpCtx Group context.
     * @param part Partition.
     */
    private void preloadPartition(CacheGroupContext grpCtx, GridDhtLocalPartition part) throws IgniteCheckedException {
        if (part.state() != OWNING)
            return;

        if (!part.reserve())
            throw new IgniteCheckedException("Reservation failed: [partId=" + part + ']');

        try {
            IgnitePageStoreManager pageStoreMgr = grpCtx.shared().pageStore();

            if (pageStoreMgr instanceof FilePageStoreManager) {
                FilePageStoreManager filePageStoreMgr = (FilePageStoreManager)pageStoreMgr;

                PageStore pageStore = filePageStoreMgr.getStore(grpCtx.groupId(), part.id());

                PageMemoryEx pageMemory = (PageMemoryEx)grpCtx.dataRegion().pageMemory();

                long pageId = pageMemory.partitionMetaPageId(grpCtx.groupId(), part.id());

                for (int pageNo = 0; pageNo < pageStore.pages(); pageId++, pageNo++) {
                    long pagePointer = -1;

                    try {
                        pagePointer = pageMemory.acquirePage(grpCtx.groupId(), pageId);
                    }
                    finally {
                        if (pagePointer != -1)
                            pageMemory.releasePage(grpCtx.groupId(), pageId, pagePointer);
                    }
                }
            }
        }
        finally {
            part.release();
        }
    }

    /**
     * @param ignite Ignite.
     * @param grpName Group name.
     * @param partId Partition id.
     */
    private void warmupPartition(IgniteEx ignite, String grpName, int partId) throws Exception {
        DynamicCacheDescriptor desc = ignite.context().cache().cacheDescriptor(grpName);

        if (desc == null)
            throw new Exception("Unknown cache");

        int grpId = desc.groupId();

        CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(grpId);

        if (grpCtx == null)
            throw new Exception("Can not find context");

        List<GridDhtLocalPartition> parts = grpCtx.topology().localPartitions();

        boolean hasPart = false;

        for (GridDhtLocalPartition part : parts) {
            if (part.id() == partId) {
                hasPart = true;
                preloadPartition(grpCtx, part);
            }
        }

        if (!hasPart)
            throw new Exception("The part " + partId + " in not local." );
    }
}
