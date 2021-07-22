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

package org.apache.ignite.internal.cache.query.index.sorted;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.IndexRenameRootPageRecord;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTaskResult;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIoResolver;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;

/**
 * Task for background cleaning of index trees.
 */
public class DurableBackgroundCleanupIndexTreeTaskV2 extends IgniteDataTransferObject implements
    DurableBackgroundTask<Long> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Index tree index factory. */
    public static InlineIndexTreeFactory IDX_TREE_FACTORY = new InlineIndexTreeFactory();

    /** Logger. */
    @Nullable private transient volatile IgniteLogger log;

    /** Unique id. */
    private String uid;

    /** Cache group name. */
    @Nullable private String grpName;

    /** Cache name. */
    private String cacheName;

    /** Index name. */
    private String idxName;

    /** Old name of underlying index tree name. */
    private String oldTreeName;

    /** New name of underlying index tree name. */
    private String newTreeName;

    /** Number of segments. */
    private int segments;

    /** Need to rename index root pages. */
    private transient volatile boolean needToRen;

    /** Index trees. Mapping: segment number -> index tree. */
    @Nullable private transient volatile Map<Integer, InlineIndexTree> trees;

    /** Worker cleaning index trees. */
    @Nullable private transient volatile GridWorker worker;

    /** Total number of pages recycled from index trees. */
    private final AtomicLong pageCnt = new AtomicLong();

    /**
     * Constructor.
     *
     * @param grpName Cache group name.
     * @param cacheName Cache name.
     * @param idxName Index name.
     * @param oldTreeName Old name of underlying index tree name.
     * @param newTreeName New name of underlying index tree name.
     * @param segments Number of segments.
     * @param trees Index trees.
     */
    public DurableBackgroundCleanupIndexTreeTaskV2(
        @Nullable String grpName,
        String cacheName,
        String idxName,
        String oldTreeName,
        String newTreeName,
        int segments,
        @Nullable InlineIndexTree[] trees
    ) {
        uid = UUID.randomUUID().toString();
        this.grpName = grpName;
        this.cacheName = cacheName;
        this.idxName = idxName;
        this.oldTreeName = oldTreeName;
        this.newTreeName = newTreeName;
        this.segments = segments;

        if (trees != null) {
            assert trees.length == segments :
                "Invalid number of index trees [trees=" + trees.length + ", segments=" + segments + ']';

            Map<Integer, InlineIndexTree> trees0 = new ConcurrentHashMap<>();

            for (int i = 0; i < trees.length; i++)
                trees0.put(i, trees[i]);

            this.trees = trees0;
        }

        needToRen = true;
    }

    /**
     * Default constructor for {@link Externalizable}.
     */
    public DurableBackgroundCleanupIndexTreeTaskV2() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeLongString(out, uid);
        U.writeLongString(out, grpName);
        U.writeLongString(out, cacheName);
        U.writeLongString(out, idxName);
        U.writeLongString(out, oldTreeName);
        U.writeLongString(out, newTreeName);
        out.writeInt(segments);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(
        byte protoVer,
        ObjectInput in
    ) throws IOException, ClassNotFoundException {
        uid = U.readLongString(in);
        grpName = U.readLongString(in);
        cacheName = U.readLongString(in);
        idxName = U.readLongString(in);
        oldTreeName = U.readLongString(in);
        newTreeName = U.readLongString(in);
        segments = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "drop-sql-index-" + cacheName + "-" + idxName + "-" + uid;
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        GridWorker w = worker;

        if (w != null) {
            worker = null;

            U.awaitForWorkersStop(singleton(w), true, log);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<DurableBackgroundTaskResult<Long>> executeAsync(GridKernalContext ctx) {
        assert worker == null;

        log = ctx.log(DurableBackgroundCleanupIndexTreeTaskV2.class);

        IgniteInternalFuture<DurableBackgroundTaskResult<Long>> outFut;

        CacheGroupContext grpCtx = ctx.cache().cacheGroup(CU.cacheGroupId(cacheName, grpName));

        if (grpCtx != null) {
            try {
                if (needToRen) {
                    renameIndexRootPages(grpCtx, cacheName, oldTreeName, newTreeName, segments);

                    needToRen = false;
                }

                Map<Integer, InlineIndexTree> trees = this.trees;

                if (trees == null) {
                    Map<Integer, RootPage> rootPages = findIndexRootPages(grpCtx, cacheName, newTreeName, segments);

                    Map<Integer, InlineIndexTree> trees0 = indexTrees(grpCtx, rootPages, newTreeName);

                    if (trees0 != null)
                        this.trees = trees = new ConcurrentHashMap<>(trees0);
                }

                if (!F.isEmpty(trees)) {
                    GridFutureAdapter<DurableBackgroundTaskResult<Long>> fut = new GridFutureAdapter<>();

                    Map<Integer, InlineIndexTree> finalTrees = trees;

                    GridWorker w = new GridWorker(
                        ctx.igniteInstanceName(),
                        "async-worker-" + name(),
                        log
                    ) {
                        /** {@inheritDoc} */
                        @Override protected void body() {
                            try {
                                Iterator<Map.Entry<Integer, InlineIndexTree>> it = finalTrees.entrySet().iterator();

                                while (it.hasNext()) {
                                    Map.Entry<Integer, InlineIndexTree> e = it.next();

                                    InlineIndexTree tree = e.getValue();
                                    Integer segment = e.getKey();

                                    long pages = destroyIndexTrees(grpCtx, tree, cacheName, newTreeName, segment);

                                    pageCnt.addAndGet(pages);

                                    it.remove();
                                }

                                fut.onDone(DurableBackgroundTaskResult.complete(pageCnt.get()));
                            }
                            catch (Throwable t) {
                                fut.onDone(DurableBackgroundTaskResult.restart(t));
                            }
                            finally {
                                worker = null;
                            }
                        }
                    };

                    new IgniteThread(w).start();

                    this.worker = w;

                    outFut = fut;
                }
                else
                    outFut = new GridFinishedFuture<>(DurableBackgroundTaskResult.complete());
            }
            catch (Throwable t) {
                outFut = new GridFinishedFuture<>(DurableBackgroundTaskResult.restart(t));
            }
        }
        else
            outFut = new GridFinishedFuture<>(DurableBackgroundTaskResult.complete());

        return outFut;
    }

    /**
     * Destroying index trees.
     *
     * @param grpCtx Cache group context.
     * @param tree Index tree.
     * @param cacheName Cache name.
     * @param treeName Name of underlying index tree name.
     * @param segment Segment number.
     * @return Total number of pages recycled from this tree.
     * @throws IgniteCheckedException If failed.
     */
    public static long destroyIndexTrees(
        CacheGroupContext grpCtx,
        InlineIndexTree tree,
        String cacheName,
        String treeName,
        int segment
    ) throws IgniteCheckedException {
        long pageCnt = 0;

        grpCtx.shared().database().checkpointReadLock();

        try {
            if (existMetaPage(grpCtx, tree.getMetaPageId())) {
                pageCnt += tree.destroy(null, true);

                if (grpCtx.offheap().dropRootPageForIndex(CU.cacheId(cacheName), treeName, segment) != null)
                    pageCnt++;
            }
        }
        finally {
            grpCtx.shared().database().checkpointReadUnlock();
        }

        return pageCnt;
    }

    /**
     * Checking the existence of a index tree meta page.
     *
     * @param grpCtx Cache group context.
     * @param metaPageId Meta page id.
     * @return {@code True} if exist.
     * @throws IgniteCheckedException If failed.
     */
    public static boolean existMetaPage(CacheGroupContext grpCtx, long metaPageId) throws IgniteCheckedException {
        PageMemory pageMemory = grpCtx.dataRegion().pageMemory();

        int grpId = grpCtx.groupId();

        long metaPage = pageMemory.acquirePage(grpId, metaPageId);

        try {
            long metaPageAddr = pageMemory.readLock(grpId, metaPageId, metaPage);

            try {
                return metaPageAddr != 0;
            }
            finally {
                if (metaPageAddr != 0)
                    pageMemory.readUnlock(grpId, metaPageId, metaPage);
            }
        }
        finally {
            pageMemory.releasePage(grpId, metaPageId, metaPage);
        }
    }

    /**
     * Creation of index trees.
     *
     * @param grpCtx Cache group context.
     * @param rootPages Index root pages. Mapping: segment number -> index root page.
     * @param treeName Name of underlying index tree name.
     * @return Index trees. Mapping: segment number -> index tree.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public static Map<Integer, InlineIndexTree> indexTrees(
        CacheGroupContext grpCtx,
        Map<Integer, RootPage> rootPages,
        String treeName
    ) throws IgniteCheckedException {
        if (rootPages.isEmpty())
            return null;
        else {
            Map<Integer, InlineIndexTree> trees = new HashMap<>(rootPages.size());

            for (Map.Entry<Integer, RootPage> e : rootPages.entrySet())
                trees.put(e.getKey(), IDX_TREE_FACTORY.create(grpCtx, e.getValue(), treeName));

            return trees;
        }
    }

    /**
     * Finding the root pages of the index.
     *
     * @param grpCtx Cache group context.
     * @param cacheName Cache name.
     * @param treeName Name of underlying index tree name.
     * @param segments Number of segments.
     * @return Index root pages. Mapping: segment number -> index root page.
     * @throws IgniteCheckedException If failed.
     */
    public static Map<Integer, RootPage> findIndexRootPages(
        CacheGroupContext grpCtx,
        String cacheName,
        String treeName,
        int segments
    ) throws IgniteCheckedException {
        Map<Integer, RootPage> rootPages = new HashMap<>();

        for (int i = 0; i < segments; i++) {
            RootPage rootPage = grpCtx.offheap().findRootPageForIndex(CU.cacheId(cacheName), treeName, i);

            if (rootPage != null)
                rootPages.put(i, rootPage);
        }

        return rootPages;
    }

    /**
     * Renaming the root index pages.
     *
     * @param grpCtx Cache group context.
     * @param cacheName Cache name.
     * @param oldTreeName Old name of underlying index tree name.
     * @param newTreeName New name of underlying index tree name.
     * @param segments Number of segments.
     * @throws IgniteCheckedException If failed.
     */
    public static void renameIndexRootPages(
        CacheGroupContext grpCtx,
        String cacheName,
        String oldTreeName,
        String newTreeName,
        int segments
    ) throws IgniteCheckedException {
        IgniteWriteAheadLogManager wal = grpCtx.shared().wal();

        int cacheId = CU.cacheId(cacheName);

        if (wal != null)
            wal.log(new IndexRenameRootPageRecord(cacheId, oldTreeName, newTreeName, segments));

        grpCtx.shared().database().checkpointReadLock();

        try {
            for (int i = 0; i < segments; i++)
                grpCtx.offheap().renameRootPageForIndex(cacheId, oldTreeName, newTreeName, i);
        }
        finally {
            grpCtx.shared().database().checkpointReadUnlock();
        }
    }

    /**
     * A do-nothing {@link InlineIndexRowHandlerFactory} implementation.
     */
    public static class NoopRowHandlerFactory implements InlineIndexRowHandlerFactory {
        /** {@inheritDoc} */
        @Override public InlineIndexRowHandler create(
            SortedIndexDefinition sdef,
            IndexKeyTypeSettings keyTypeSettings
        ) {
            return new InlineIndexRowHandler() {
                /** {@inheritDoc} */
                @Override public IndexKey indexKey(int idx, CacheDataRow row) {
                    return null;
                }

                /** {@inheritDoc} */
                @Override public List<InlineIndexKeyType> inlineIndexKeyTypes() {
                    return emptyList();
                }

                /** {@inheritDoc} */
                @Override public List<IndexKeyDefinition> indexKeyDefinitions() {
                    return emptyList();
                }

                @Override public IndexKeyTypeSettings indexKeyTypeSettings() {
                    return null;
                }

                /** {@inheritDoc} */
                @Override public int partition(CacheDataRow row) {
                    return 0;
                }

                /** {@inheritDoc} */
                @Override public Object cacheKey(CacheDataRow row) {
                    return null;
                }

                /** {@inheritDoc} */
                @Override public Object cacheValue(CacheDataRow row) {
                    return null;
                }
            };
        }
    }

    /**
     * Factory for creating index trees.
     */
    public static class InlineIndexTreeFactory {
        /**
         * Creation of an index tree.
         *
         * @param grpCtx Cache group context.
         * @param rootPage Index root page.
         * @param treeName Name of underlying index tree name.
         * @return New index tree.
         * @throws IgniteCheckedException If failed.
         */
        protected InlineIndexTree create(
            CacheGroupContext grpCtx,
            RootPage rootPage,
            String treeName
        ) throws IgniteCheckedException {
            return new InlineIndexTree(
                null,
                grpCtx,
                treeName,
                grpCtx.offheap(),
                grpCtx.offheap().reuseListForIndex(treeName),
                grpCtx.dataRegion().pageMemory(),
                PageIoResolver.DEFAULT_PAGE_IO_RESOLVER,
                rootPage.pageId().pageId(),
                false,
                0,
                0,
                new IndexKeyTypeSettings(),
                null,
                null,
                new NoopRowHandlerFactory(),
                null
            );
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DurableBackgroundCleanupIndexTreeTaskV2.class, this);
    }
}
