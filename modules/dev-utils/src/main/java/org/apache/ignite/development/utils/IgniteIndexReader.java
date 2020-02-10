/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.development.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.AllocatedPageTracker;
import org.apache.ignite.internal.processors.cache.persistence.IndexStorageImpl;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileVersionCheckingFactory;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageMetaIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2RowLinkIO;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static java.util.Collections.singletonList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdUtils.flag;
import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.pagemem.PageIdUtils.partId;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO.ITEMS_OFF;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getType;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getVersion;

public class IgniteIndexReader {
    private static final String META_TREE_NAME = "MetaTree";

    static {
        PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS);

        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();
    }

    private final int pageSize;
    private final int partCnt;
    private final File cacheWorkDir;
    private final DataStorageConfiguration dsCfg;
    private final FilePageStoreFactory storeFactory;
    private final AllocatedPageTracker allocatedTracker = AllocatedPageTracker.NO_OP;
    private final PrintStream outStream;
    private final PrintStream outErrStream;
    private final File idxFile;
    private final FilePageStore idxStore;
    private final FilePageStore[] partStores;
    private final long pagesNum;
    private final Set<Integer> missingPartitions = new HashSet<>();

    private PageIOProcessor innerPageIOProcessor = new InnerPageIOProcessor();
    private PageIOProcessor leafPageIOProcessor = new LeafPageIOProcessor();

    private final Map<Class, PageIOProcessor> ioProcessorsMap = new HashMap<Class, PageIOProcessor>() {{
        put(BPlusMetaIO.class, new MetaPageIOProcessor());
        put(BPlusInnerIO.class, innerPageIOProcessor);
        put(H2ExtrasInnerIO.class, innerPageIOProcessor);
        put(IndexStorageImpl.MetaStoreInnerIO.class, innerPageIOProcessor);
        put(BPlusLeafIO.class, leafPageIOProcessor);
        put(H2ExtrasLeafIO.class, leafPageIOProcessor);
        put(IndexStorageImpl.MetaStoreLeafIO.class, leafPageIOProcessor);
    }};

    public IgniteIndexReader(String cacheWorkDirPath, int pageSize, int partCnt, int filePageStoreVer, String outputFile)
        throws IgniteCheckedException {
        this.pageSize = pageSize;
        this.partCnt = partCnt;
        this.dsCfg = new DataStorageConfiguration().setPageSize(pageSize);
        this.cacheWorkDir = new File(cacheWorkDirPath);
        this.storeFactory = new FileVersionCheckingFactory(new AsyncFileIOFactory(), dsCfg, 0) {
            /** {@inheritDoc} */
            @Override public int latestVersion() {
                return filePageStoreVer;
            }
        };

        if (outputFile == null) {
            outStream = System.out;
            outErrStream = System.err;
        }
        else {
            try {
                this.outStream = new PrintStream(new FileOutputStream(outputFile));
                this.outErrStream = outStream;
            }
            catch (FileNotFoundException e) {
                throw new IgniteException(e.getMessage(), e);
            }
        }

        idxFile = getFile(INDEX_PARTITION);

        if (idxFile == null)
            throw new RuntimeException("index.bin file not found");

        idxStore = storeFactory.createPageStore(FLAG_IDX, idxFile, allocatedTracker);

        pagesNum = (idxFile.length() - idxStore.headerSize()) / pageSize;

        partStores = new FilePageStore[partCnt];

        for (int i = 0; i < partCnt; i++) {
            final File file = getFile(i);

            // Some of array members will be null if node doesn't have all partition files locally.
            if (file != null)
                partStores[i] = storeFactory.createPageStore(FLAG_DATA, file, allocatedTracker);
        }
    }

    private void print(String s) {
        outStream.println(s);
    }

    private void printConsole(String s) {
        System.out.println(s);
    }

    private void printErr(String s) {
        outErrStream.println(s);
    }

    private static long normalizePageId(long pageId) {
        return pageId(partId(pageId), flag(pageId), pageIndex(pageId));
    }

    private File getFile(int partId) {
        File file =  new File(cacheWorkDir, partId == INDEX_PARTITION ? INDEX_FILE_NAME : String.format(PART_FILE_TEMPLATE, partId));

        if (!file.exists())
            return null;
        else if (partId == -1)
            print("Analyzing file: " + file.getPath());

        return file;
    }

    private void readIdx() throws IgniteCheckedException {
        long partPageStoresNum = Arrays.stream(partStores)
                                       .filter(Objects::nonNull)
                                       .count();

        print("Partitions files num: " + partPageStoresNum);

        Map<Class<? extends PageIO>, Long> pageClasses = new HashMap<>();

        print("Going to check " + pagesNum + " pages.");

        Map<Class, Set<Long>> pageIoIds = new HashMap<>();

        AtomicReference<PageListsInfo> pageListsInfo = new AtomicReference<>();

        List<Throwable> errors = new LinkedList<>();

        long timeStarted = 0;

        ProgressPrinter progressPrinter = new ProgressPrinter(System.out, "Reading pages sequentially", pagesNum);

        for (int i = 0; i < pagesNum; i++) {
            ByteBuffer buf = GridUnsafe.allocateBuffer(pageSize);

            try {
                long addr = GridUnsafe.bufferAddress(buf);

                long pageId = PageIdUtils.pageId(INDEX_PARTITION, FLAG_IDX, i);

                //We got int overflow here on sber dataset.
                final long off = (long)i * pageSize + idxStore.headerSize();

                idxStore.readByOffset(off, buf, false);

                PageIO io = PageIO.getPageIO(addr);

                pageClasses.merge(io.getClass(), 1L, (oldVal, newVal) -> ++oldVal);

                if (io instanceof PageMetaIO) {
                    PageMetaIO pageMetaIO = (PageMetaIO)io;

                    Map<String, TreeValidationInfo> treeInfo =
                        validateAllTrees(pageMetaIO.getTreeRoot(addr));

                    printValidationResults(treeInfo);

                    treeInfo.forEach((name, info) -> {
                        info.innerPageIds.forEach(id -> {
                            Class cls = name.equals(META_TREE_NAME)
                                ? IndexStorageImpl.MetaStoreInnerIO.class
                                : H2ExtrasInnerIO.class;

                            pageIoIds.computeIfAbsent(cls, k -> new HashSet<>()).add(id);
                        });

                        pageIoIds.computeIfAbsent(BPlusMetaIO.class, k -> new HashSet<>()).add(info.rootPageId);
                    });

                    print("");
                }
                else if (io instanceof PagesListMetaIO) {
                    pageListsInfo.set(getPageListsMetaInfo(pageId));

                    printPagesListsInfo(pageListsInfo.get());
                }
                else {
                    if (timeStarted == 0)
                        timeStarted = System.currentTimeMillis();

                    progressPrinter.printProgress(i, timeStarted);

                    ofNullable(pageIoIds.get(io.getClass())).ifPresent((pageIds) -> {
                        if (!pageIds.contains(pageId)) {
                            boolean foundInList =
                                (pageListsInfo.get() != null && pageListsInfo.get().allPages.contains(pageId));

                            throw new IgniteException(
                                "Possibly orphan " + io.getClass().getSimpleName() + " page, pageId=" + pageId +
                                    (foundInList ? ", it has been found in page list." : "")
                            );
                        }
                    });
                }
            } catch (Throwable e) {
                String err = "Exception occurred on step " + i + ": " + e.getMessage() + "; page=" + U.toHexString(buf);

                errors.add(new IgniteException(err, e));
            }
            finally {
                GridUnsafe.freeBuffer(buf);
            }
        }

        if (!errors.isEmpty()) {
            printErr("---Errors:");

            errors.forEach(e -> printErr(e.toString()));
        }

        print("---These pages types were encountered during sequential scan:");
        pageClasses.forEach((key, val) -> print(key.getSimpleName() + ": " + val));

        print("---");
        print("Total pages encountered during sequential scan: " + pageClasses.values().stream().mapToLong(a -> a).sum());
        print("Total errors while pages encountering: " + errors.size());
    }

    private PageListsInfo getPageListsMetaInfo(long metaPageListId) {
        Map<IgniteBiTuple<Long, Integer>, List<Long>> bucketsData = new HashMap<>();

        Set<Long> allPages = new HashSet<>();

        Map<Long, Throwable> errors = new HashMap<>();

        long nextMetaId = metaPageListId;

        while(nextMetaId != 0) {
            ByteBuffer buf = GridUnsafe.allocateBuffer(pageSize);

            try {
                long addr = GridUnsafe.bufferAddress(buf);

                idxStore.read(nextMetaId, buf, false);

                PagesListMetaIO io = PageIO.getPageIO(addr);

                Map<Integer, GridLongList> data = new HashMap<>();

                io.getBucketsData(addr, data);

                final long fNextMetaId = nextMetaId;

                data.forEach((k, v) -> {
                    List<Long> listIds = LongStream.of(v.array()).map(IgniteIndexReader::normalizePageId).boxed().collect(toList());

                    listIds.forEach(listId -> allPages.addAll(getPageList(listId)));

                    bucketsData.put(new IgniteBiTuple<>(fNextMetaId, k), listIds);
                });

                nextMetaId = io.getNextMetaPageId(addr);
            }
            catch (Exception e) {
                errors.put(nextMetaId, e);

                nextMetaId = 0;
            }
            finally {
                GridUnsafe.freeBuffer(buf);
            }
        }

        return new PageListsInfo(bucketsData, allPages, errors);
    }

    private List<Long> getPageList(long pageListStartId) {
        List<Long> res = new LinkedList<>();

        long nextNodeId = pageListStartId;

        while(nextNodeId != 0) {
            ByteBuffer buf = GridUnsafe.allocateBuffer(pageSize);

            try {
                long addr = GridUnsafe.bufferAddress(buf);

                idxStore.read(nextNodeId, buf, false);

                PagesListNodeIO io = PageIO.getPageIO(addr);

                for (int i = 0; i < io.getCount(addr); i++)
                    res.add(normalizePageId(io.getAt(addr, i)));

                nextNodeId = io.getNextId(addr);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e.getMessage(), e);
            }
            finally {
                GridUnsafe.freeBuffer(buf);
            }
        }

         return res;
    }

    private Map<String, TreeValidationInfo> validateAllTrees(long metaTreeRootPageId) {
        Map<String, TreeValidationInfo> treeInfos = new HashMap<>();

        TreeValidationInfo metaTreeValidationInfo = validateTree(metaTreeRootPageId, true);

        treeInfos.put(META_TREE_NAME, metaTreeValidationInfo);

        AtomicInteger progress = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        print("");

        ProgressPrinter progressPrinter = new ProgressPrinter(System.out, "Index trees traversal", metaTreeValidationInfo.idxItems.size());

        metaTreeValidationInfo.idxItems.forEach(item -> {
            progressPrinter.printProgress(progress.incrementAndGet(), startTime);

            IndexStorageImpl.IndexItem idxItem = (IndexStorageImpl.IndexItem)item;

            TreeValidationInfo treeValidationInfo = validateTree(idxItem.pageId(), false);

            treeInfos.put(idxItem.toString(), treeValidationInfo);
        });

        return treeInfos;
    }

    private void printValidationResults(Map<String, TreeValidationInfo> treeInfos) {
        print("Tree traversal results: ");

        Map<Class, AtomicLong> totalStat = new HashMap<>();

        AtomicInteger totalErr = new AtomicInteger(0);

        treeInfos.forEach((idxName, validationInfo) -> {
            print("-----");
            print("Index tree: " + idxName);
            print("-- Page stat:");

            validationInfo.ioStat.forEach((cls, cnt) -> {
                print(cls.getSimpleName() + ": " + cnt.get());

                totalStat.computeIfAbsent(cls, k -> new AtomicLong(0)).addAndGet(cnt.get());
            });

            if (validationInfo.errors.size() > 0) {
                print("-- Errors:");

                validationInfo.errors.forEach((id, errors) -> {
                    print("Page id=" + id + ", errors:");

                    errors.forEach(e -> e.printStackTrace());

                    totalErr.addAndGet(errors.size());
                });
            }
            else
                print("No errors occurred while traversing.");
        });

        print("-- Total page stat:");

        totalStat.forEach((cls, cnt) -> print(cls.getSimpleName() + ": " + cnt.get()));

        print("");
        print("Total trees: " + treeInfos.keySet().size());
        print("Total pages found in trees: " + totalStat.values().stream().mapToLong(AtomicLong::get).sum());
        print("Total errors during trees traversal: " + totalErr.get());
        print("------------------");
    }

    private void printPagesListsInfo(PageListsInfo pageListsInfo) {
        print("---Page lists info.");
        print("---Printing buckets data:");

        pageListsInfo.bucketsData.forEach((bucket, bucketData) -> {
            GridStringBuilder sb = new GridStringBuilder()
                .a("List meta id=")
                .a(bucket.get1())
                .a(", bucket number=")
                .a(bucket.get2())
                .a(", lists=[")
                .a(bucketData.stream().map(String::valueOf).collect(joining(", ")))
                .a("]");

            print(sb.toString());
        });

        if (!pageListsInfo.errors.isEmpty()) {
            print("---Errors:");

            pageListsInfo.errors.forEach((id, error) -> {
                printErr("Page id: " + id + ", error: ");

                error.printStackTrace();
            });
        }

        print("");
        print("Total index pages found in lists: " + pageListsInfo.allPages.size());
        print("Total errors during lists scan: " + pageListsInfo.errors.size());
        print("------------------");
    }

    /** */
    private TreeValidationInfo validateTree(long rootPageId, boolean isMetaTree) {
        Map<Class, AtomicLong> ioStat = new HashMap<>();

        Map<Long, Set<Throwable>> errors = new HashMap<>();

        Set<Long> innerPageIds = new HashSet<>();

        PageCallback innerCb = (content, pageId) -> innerPageIds.add(pageId);

        List<Object> idxItems = new LinkedList<>();

        ItemCallback itemCb = isMetaTree ? (currPageId, item, link) -> idxItems.add(item) : null;

        getTreeNode(rootPageId, new TreeNodeContext(idxStore, ioStat, errors, innerCb, null, itemCb));

        return new TreeValidationInfo(ioStat, errors, innerPageIds, rootPageId, idxItems);
    }

    private TreeNode getTreeNode(
        long pageId,
        TreeNodeContext nodeCtx
    ) {
        Class ioCls;

        PageContent pageContent;

        PageIOProcessor ioProcessor;

        try {
            final ByteBuffer buf = GridUnsafe.allocateBuffer(pageSize);

            try {
                nodeCtx.store.read(pageId, buf, false);

                final long addr = GridUnsafe.bufferAddress(buf);

                final PageIO io = PageIO.getPageIO(addr);

                ioCls = io.getClass();

                nodeCtx.ioStat.computeIfAbsent(io.getClass(), k -> new AtomicLong(0)).incrementAndGet();

                ioProcessor = ioProcessorsMap.get(ioCls);

                if (ioProcessor == null)
                    throw new IgniteException("Unexpected page io: " + ioCls.getSimpleName());

                pageContent = ioProcessor.getContent(io, addr, pageId, nodeCtx);
            }
            finally {
                GridUnsafe.freeBuffer(buf);
            }

            return ioProcessor.getNode(pageContent, pageId, nodeCtx);
        }
        catch (Exception e) {
            nodeCtx.errors.computeIfAbsent(pageId, k -> new HashSet<>()).add(e);

            return new TreeNode(pageId, null, "exception: " + e.getMessage(), Collections.emptyList());
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            String dir = args[0];

            int partCnt = args.length > 1 ? Integer.parseInt(args[1]) : 0;

            int pageSize = args.length > 2 ? Integer.parseInt(args[2]) : 4096;

            int filePageStoreVer = args.length > 3 ? Integer.parseInt(args[3]) : 2;

            String outputFile = args.length > 4 ? args[4] : null;

            new IgniteIndexReader(dir, pageSize, partCnt, filePageStoreVer, outputFile)
                .readIdx();
        }
        catch (Exception e) {
            System.err.println("options: path [partCnt] [pageSize] [filePageStoreVersion]");

            throw e;
        }
    }

    private static class TreeNode {
        final long pageId;
        final PageIO io;
        final String additionalInfo;
        final List<TreeNode> children;

        /** */
        public TreeNode(long pageId, PageIO io, String additionalInfo, List<TreeNode> children) {
            this.pageId = pageId;
            this.io = io;
            this.additionalInfo = additionalInfo;
            this.children = children;
        }
    }

    private static class TreeNodeContext {
        final FilePageStore store;
        final Map<Class, AtomicLong> ioStat;
        final Map<Long, Set<Throwable>> errors;
        final PageCallback innerCb;
        final PageCallback leafCb;
        final ItemCallback itemCb;

        /** */
        private TreeNodeContext(
            FilePageStore store,
            Map<Class, AtomicLong> ioStat,
            Map<Long, Set<Throwable>> errors,
            PageCallback innerCb,
            PageCallback leafCb,
            ItemCallback itemCb
        ) {
            this.store = store;
            this.ioStat = ioStat;
            this.errors = errors;
            this.innerCb = innerCb;
            this.leafCb = leafCb;
            this.itemCb = itemCb;
        }
    }

    private static class PageContent {
        final PageIO io;
        final List<Long> linkedPageIds;
        final List<Object> items;
        final String info;

        public PageContent(PageIO io, List<Long> linkedPageIds, List<Object> items, String info) {
            this.io = io;
            this.linkedPageIds = linkedPageIds;
            this.items = items;
            this.info = info;
        }
    }

    private interface PageCallback {
        void cb(PageContent pageContent, long pageId);
    }

    private interface ItemCallback {
        void cb(long currPageId, Object item, long link);
    }

    private interface PageIOProcessor {
        PageContent getContent(PageIO io, long addr, long pageId, TreeNodeContext nodeCtx);
        TreeNode getNode(PageContent content, long pageId, TreeNodeContext nodeCtx);
    }

    private class MetaPageIOProcessor implements PageIOProcessor {
        /** {@inheritDoc} */
        @Override public PageContent getContent(PageIO io, long addr, long pageId, TreeNodeContext nodeCtx) {
            BPlusMetaIO bPlusMetaIO = (BPlusMetaIO)io;

            int rootLvl = bPlusMetaIO.getRootLevel(addr);
            long rootId = bPlusMetaIO.getFirstPageId(addr, rootLvl);

            return new PageContent(io, singletonList(rootId), null, null);
        }

        /** {@inheritDoc} */
        @Override public TreeNode getNode(PageContent content, long pageId, TreeNodeContext nodeCtx) {
            return new TreeNode(pageId, content.io, null, singletonList(getTreeNode(content.linkedPageIds.get(0), nodeCtx)));
        }
    }

    private class InnerPageIOProcessor implements PageIOProcessor {
        /** {@inheritDoc} */
        @Override public PageContent getContent(PageIO io, long addr, long pageId, TreeNodeContext nodeCtx) {
            BPlusInnerIO innerIo = (BPlusInnerIO)io;

            int cnt = innerIo.getCount(addr);

            List<Long> childrenIds;

            if (cnt > 0) {
                childrenIds = new ArrayList<>(cnt + 1);

                for (int i = 0; i < cnt; i++)
                    childrenIds.add(innerIo.getLeft(addr, i));

                childrenIds.add(innerIo.getRight(addr, cnt - 1));
            }
            else {
                long left = innerIo.getLeft(addr, 0);

                childrenIds = left == 0 ? Collections.<Long>emptyList() : singletonList(left);
            }

            return new PageContent(io, childrenIds, null, null);
        }

        /** {@inheritDoc} */
        @Override public TreeNode getNode(PageContent content, long pageId, TreeNodeContext nodeCtx) {
            List<TreeNode> children = new ArrayList<>(content.linkedPageIds.size());

            for (Long id : content.linkedPageIds)
                children.add(getTreeNode(id, nodeCtx));

            if (nodeCtx.innerCb != null)
                nodeCtx.innerCb.cb(content, pageId);

            return new TreeNode(pageId, content.io, null, children);
        }
    }

    private class LeafPageIOProcessor implements PageIOProcessor {
        /** {@inheritDoc} */
        @Override public PageContent getContent(PageIO io, long addr, long pageId, TreeNodeContext nodeCtx) {
            GridStringBuilder sb = new GridStringBuilder();

            List<Object> items = new LinkedList<>();

            if (io instanceof IndexStorageImpl.MetaStoreLeafIO) {
                IndexStorageImpl.MetaStoreLeafIO metaLeafIO = (IndexStorageImpl.MetaStoreLeafIO)io;

                for (int j = 0; j < (pageSize - ITEMS_OFF) / metaLeafIO.getItemSize(); j++) {
                    IndexStorageImpl.IndexItem indexItem = null;

                    try {
                        indexItem = metaLeafIO.getLookupRow(null, addr, j);
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }

                    if (indexItem.pageId() != 0) {
                        sb.a(indexItem.toString() + " ");

                        items.add(indexItem);
                    }
                }
            }
            else {
                boolean processed = processIndexLeaf(io, addr, pageId, nodeCtx);

                if (!processed)
                    throw new IgniteException("Unexpected page io: " + io.getClass().getSimpleName());
            }

            return new PageContent(io, null, items, sb.toString());
        }

        private boolean processIndexLeaf(PageIO io, long addr, long pageId, TreeNodeContext nodeCtx) {
            if (io instanceof BPlusIO && io instanceof H2RowLinkIO) {
                if (partCnt > 0) {
                    int itemsCnt = ((BPlusIO)io).getCount(addr);

                    for (int j = 0; j < itemsCnt; j++) {
                        long link = ((H2RowLinkIO)io).getLink(addr, j);

                        long linkedPageId = pageId(link);

                        int linkedPagePartId = partId(linkedPageId);

                        if (missingPartitions.contains(linkedPagePartId))
                            continue;

                        int linkedItemId = itemId(link);

                        ByteBuffer dataBuf = GridUnsafe.allocateBuffer(pageSize);

                        try {
                            long dataBufAddr = GridUnsafe.bufferAddress(dataBuf);

                            if (linkedPagePartId > partStores.length - 1) {
                                missingPartitions.add(linkedPagePartId);

                                throw new IgniteException("Calculated data page partition id exceeds given partitions count: " +
                                    linkedPagePartId + ", partCnt=" + partCnt);
                            }

                            final FilePageStore store = partStores[linkedPagePartId];

                            if (store == null) {
                                missingPartitions.add(linkedPagePartId);

                                throw new IgniteException("Corresponding store wasn't found for partId=" + linkedPagePartId + ". Does partition file exist?");
                            }

                            store.read(linkedPageId, dataBuf, false);

                            PageIO dataIo = PageIO.getPageIO(getType(dataBuf), getVersion(dataBuf));

                            if (dataIo instanceof DataPageIO) {
                                DataPageIO dataPageIO = (DataPageIO)dataIo;

                                DataPagePayload payload = dataPageIO.readPayload(dataBufAddr, linkedItemId, pageSize);

                                if (payload.offset() <= 0 || payload.payloadSize() <= 0) {
                                    GridStringBuilder payloadInfo = new GridStringBuilder("Invalid data page payload: ")
                                        .a("off=").a(payload.offset())
                                        .a(", size=").a(payload.payloadSize())
                                        .a(", nextLink=").a(payload.nextLink());

                                    throw new IgniteException(payloadInfo.toString());
                                }
                            }
                        }
                        catch (Exception e) {
                            nodeCtx.errors.computeIfAbsent(pageId, k -> new HashSet<>())
                                .add(new IgniteException("Exception while trying to validate data page.", e));
                        }
                        finally {
                            GridUnsafe.freeBuffer(dataBuf);
                        }
                    }
                }

                return true;
            }
            else
                return false;
        }

        /** {@inheritDoc} */
        @Override public TreeNode getNode(PageContent content, long pageId, TreeNodeContext nodeCtx) {
            if (nodeCtx.leafCb != null)
                nodeCtx.leafCb.cb(content, pageId);

            if (nodeCtx.itemCb != null) {
                for (Object item : content.items)
                    nodeCtx.itemCb.cb(pageId, item, 0);
            }

            return new TreeNode(pageId, content.io, content.info, Collections.emptyList());
        }
    }

    private static class TreeValidationInfo {
        final Map<Class, AtomicLong> ioStat;
        final Map<Long, Set<Throwable>> errors;
        final Set<Long> innerPageIds;
        final long rootPageId;
        final List<Object> idxItems;

        public TreeValidationInfo(
            Map<Class, AtomicLong> ioStat,
            Map<Long, Set<Throwable>> errors,
            Set<Long> innerPageIds,
            long rootPageId,
            List<Object> idxItems
        ) {
            this.ioStat = ioStat;
            this.errors = errors;
            this.innerPageIds = innerPageIds;
            this.rootPageId = rootPageId;
            this.idxItems = idxItems;
        }
    }

    private static class PageListsInfo {
        final Map<IgniteBiTuple<Long, Integer>, List<Long>> bucketsData;
        final Set<Long> allPages;
        final Map<Long, Throwable> errors;

        public PageListsInfo(
            Map<IgniteBiTuple<Long, Integer>, List<Long>> bucketsData,
            Set<Long> allPages,
            Map<Long, Throwable> errors
        ) {
            this.bucketsData = bucketsData;
            this.allPages = allPages;
            this.errors = errors;
        }
    }
}
