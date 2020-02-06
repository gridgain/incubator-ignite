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
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.util.Collections.singletonList;
import static java.util.Optional.ofNullable;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.partId;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO.ITEMS_OFF;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getType;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO.getVersion;

public class IgniteIndexReader {
    private static final String META_TREE_NAME = "MetaTree";

    private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

    static {
        PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS);

        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();
    }

    private final int pageSize;
    private final int grpId;
    private final int partCnt;
    private final File cacheWorkDir;
    private final DataStorageConfiguration dsCfg;
    private final FilePageStoreFactory storeFactory;
    private final AllocatedPageTracker allocatedTracker = AllocatedPageTracker.NO_OP;

    private final Map<Class, TreeNodeGetter> treeNodeGetters = new HashMap<Class, TreeNodeGetter>() {{
        put(BPlusMetaIO.class, (io, id, a, c) -> getTreeNodeFromMetaIo(io, id, a, c));
        put(BPlusInnerIO.class, (io, id, a, c) -> getTreeNodeFromInnerIo(io, id, a, c));
        put(H2ExtrasInnerIO.class, (io, id, a, c) -> getTreeNodeFromInnerIo(io, id, a, c));
        put(IndexStorageImpl.MetaStoreInnerIO.class, (io, id, a, c) -> getTreeNodeFromInnerIo(io, id, a, c));
        put(BPlusLeafIO.class, (io, id, a, c) -> getTreeNodeFromLeafIo(io, id, a, c));
        put(H2ExtrasLeafIO.class, (io, id, a, c) -> getTreeNodeFromLeafIo(io, id, a, c));
        put(IndexStorageImpl.MetaStoreLeafIO.class, (io, id, a, c) -> getTreeNodeFromLeafIo(io, id, a, c));
    }};

    public IgniteIndexReader(String cacheWorkDirPath, int pageSize, int partCnt, int filePageStoreVer, int grpId) {
        this.pageSize = pageSize;
        this.grpId = grpId;
        this.partCnt = partCnt;
        this.dsCfg = new DataStorageConfiguration().setPageSize(pageSize);
        this.cacheWorkDir = new File(cacheWorkDirPath);
        this.storeFactory = new FileVersionCheckingFactory(new AsyncFileIOFactory(), dsCfg, grpId) {
            /** {@inheritDoc} */
            @Override public int latestVersion() {
                return filePageStoreVer;
            }
        };
    }

    private void print(String s) {
        System.out.println(s);
    }

    private void printErr(String s) {
        System.err.println("\r" + s);
    }

    private void printErr(Exception e) {
        e.printStackTrace();
    }

    private File getFile(int partId) {
        File file =  new File(cacheWorkDir, partId == -1 ? INDEX_FILE_NAME : String.format(PART_FILE_TEMPLATE, partId));

        if (!file.exists()) {
            printErr("Analyzing only index.bin? Pass 0 as partCnt argument.");

            throw new RuntimeException("File not found: " + file.getPath());
        }
        else if (partId == -1)
            print("Analyzing file: " + file.getPath());

        return file;
    }

    private void readIdx() throws IgniteCheckedException {
        File idxFile = getFile(-1);

        long fileSize = idxFile.length();

        FilePageStore idxPageStore = storeFactory.createPageStore(FLAG_IDX, idxFile, allocatedTracker);

        List<FilePageStore> partPageStores = new ArrayList<>(partCnt);

        for (int i = 0; i < partCnt; i++)
            partPageStores.add(storeFactory.createPageStore(FLAG_DATA, getFile(i), allocatedTracker));

        Map<Class<? extends PageIO>, Long> pageClasses = new HashMap<>();

        final long pagesNum = (fileSize - idxPageStore.headerSize()) / pageSize;

        print("Going to check " + pagesNum + " pages.");

        Map<Class, Set<Long>> pageIoIds = new HashMap<>();

        List<Throwable> errors = new LinkedList<>();

        long timeStarted = 0;

        for (int i = 0; i < pagesNum; i++) {
            ByteBuffer buf = GridUnsafe.allocateBuffer(pageSize);

            try {
                long addr = GridUnsafe.bufferAddress(buf);

                long pageId = PageIdUtils.pageId(INDEX_PARTITION, FLAG_IDX, i);

                //We got int overflow here on sber dataset.
                final long off = (long)i * pageSize + idxPageStore.headerSize();

                idxPageStore.readByOffset(off, buf, false);

                PageIO io = PageIO.getPageIO(addr);

                pageClasses.merge(io.getClass(), 1L, (oldVal, newVal) -> ++oldVal);

                if (io instanceof PageMetaIO) {
                    PageMetaIO pageMetaIO = (PageMetaIO)io;

                    Map<String, TreeValidationInfo> treeInfo =
                        validateAllTrees(idxPageStore, partPageStores, pageMetaIO.getTreeRoot(addr));

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
                else {
                    if (timeStarted == 0)
                        timeStarted = System.currentTimeMillis();

                    printProgress("Reading pages sequentially", i, pagesNum, timeStarted);

                    ofNullable(pageIoIds.get(io.getClass())).ifPresent((pageIds) -> {
                        if (!pageIds.contains(pageId))
                            throw new IgniteException(
                                "Possibly orphan " + io.getClass().getSimpleName() + " page, pageId=" + pageId
                            );
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

    private Map<String, TreeValidationInfo> validateAllTrees(
        FilePageStore idxStore,
        List<FilePageStore> partStores,
        long metaTreeRootPageId
    ) {
        Map<String, TreeValidationInfo> treeInfos = new HashMap<>();

        TreeValidationInfo metaTreeValidationInfo = validateTree(idxStore, partStores, metaTreeRootPageId, true);

        treeInfos.put(META_TREE_NAME, metaTreeValidationInfo);

        AtomicInteger progress = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        print("");

        metaTreeValidationInfo.idxItems.forEach(item -> {
            printProgress("Index trees traversal", progress.incrementAndGet(), metaTreeValidationInfo.idxItems.size(), startTime);

            IndexStorageImpl.IndexItem idxItem = (IndexStorageImpl.IndexItem)item;

            TreeValidationInfo treeValidationInfo = validateTree(idxStore, partStores, idxItem.pageId(), false);

            //treeInfos.put(idxItem.toString(), treeValidationInfo.merge(treeInfos.get(idxItem.toString())));
            treeInfos.put(idxItem.toString(), treeValidationInfo);
        });

        printValidationResults(treeInfos);

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
        print("Total errors: " + totalErr.get());
        print("------------------");
    }

    /** */
    private TreeValidationInfo validateTree(
        FilePageStore idxStore,
        List<FilePageStore> partStores,
        long rootPageId,
        boolean isMetaTree
    ) {
        Map<Class, AtomicLong> ioStat = new HashMap<>();

        Map<Long, Set<Throwable>> errors = new HashMap<>();

        Set<Long> innerPageIds = new HashSet<>();

        PageCallback innerCb = (io, pageAddr, pageId) -> innerPageIds.add(pageId);

        PageCallback leafCb =
            isMetaTree ? null : (io, pageAddr, pageId) -> validateIndexLeaf(partStores, (H2ExtrasLeafIO)io, pageAddr);

        List<Object> idxItems = new LinkedList<>();

        ItemCallback itemCb = isMetaTree ? (currPageId, targetPageId, link) -> idxItems.add(targetPageId) : null;

        getTreeNode(rootPageId, new TreeNodeContext(idxStore, ioStat, errors, innerCb, leafCb, itemCb));

        return new TreeValidationInfo(ioStat, errors, innerPageIds, rootPageId, idxItems);
    }

    private void validateIndexLeaf(List<FilePageStore> partStores, H2ExtrasLeafIO io, long pageAddr) {
        if (partCnt > 0) {
            int itemsCnt = (pageSize - ITEMS_OFF) / io.getItemSize();

            for (int j = 0; j < itemsCnt; j++) {
                long link = io.getLink(pageAddr, j);

                long linkedPageId = pageId(link);

                int linkedPagePartId = partId(linkedPageId);

                int linkedItemId = itemId(link);

                ByteBuffer dataBuf = GridUnsafe.allocateBuffer(pageSize);

                try {
                    long dataBufAddr = GridUnsafe.bufferAddress(dataBuf);

                    partStores.get(linkedPagePartId).read(linkedPageId, dataBuf, false);

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
                    throw new IgniteException("Exception while trying to validate data page.", e);
                }
                finally {
                    GridUnsafe.freeBuffer(dataBuf);
                }
            }
        }
    }

    private TreeNode getTreeNode(
        long pageId,
        TreeNodeContext nodeCtx
    ) {
        final ByteBuffer buf = GridUnsafe.allocateBuffer(pageSize);

        try {
            nodeCtx.store.read(pageId, buf, false);

            final long addr = GridUnsafe.bufferAddress(buf);

            final PageIO io = PageIO.getPageIO(addr);

            nodeCtx.ioStat.computeIfAbsent(io.getClass(), k -> new AtomicLong(0)).incrementAndGet();

            return ofNullable(treeNodeGetters.get(io.getClass()))
                .map(getter -> getter.get(io, pageId, addr, nodeCtx))
                .orElseThrow(() -> new IgniteException("Unexpected page io: " + io.getClass().getSimpleName()));
        }
        catch (Exception e) {
            nodeCtx.errors.computeIfAbsent(pageId, k -> new HashSet<>()).add(e);

            return new TreeNode(pageId, null, "exception: " + e.getMessage(), Collections.emptyList());
        }
        finally {
            GridUnsafe.freeBuffer(buf);
        }
    }

    /** */
    private TreeNode getTreeNodeFromMetaIo(PageIO io, long pageId, long addr, TreeNodeContext nodeCtx) {
        BPlusMetaIO bPlusMetaIO = (BPlusMetaIO)io;

        int rootLvl = bPlusMetaIO.getRootLevel(addr);
        long rootId = bPlusMetaIO.getFirstPageId(addr, rootLvl);

        return new TreeNode(
            pageId,
            io,
            null,
            singletonList(getTreeNode(rootId, nodeCtx))
        );
    }

    /** */
    private TreeNode getTreeNodeFromInnerIo(PageIO io, long pageId, long addr, TreeNodeContext nodeCtx) {
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

        List<TreeNode> children = new ArrayList<>(childrenIds.size());

        for (Long id : childrenIds)
            children.add(getTreeNode(id, nodeCtx));

        if (nodeCtx.innerCb != null)
            nodeCtx.innerCb.cb(io, addr, pageId);

        return new TreeNode(pageId, io, null, children);
    }

    /** */
    private TreeNode getTreeNodeFromLeafIo(PageIO io, long pageId, long addr, TreeNodeContext nodeCtx) {
        GridStringBuilder sb = new GridStringBuilder();

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

                    if (nodeCtx.itemCb != null)
                        nodeCtx.itemCb.cb(pageId, indexItem, 0);
                }
            }
        }
        else if (io instanceof H2RowLinkIO) {
            if (nodeCtx.itemCb != null)
                throw new UnsupportedOperationException("No item callback support for H2RowLinkIO.");
        }
        else
            throw new IgniteException("Unexpected page io: " + io.getClass().getSimpleName());

        if (nodeCtx.leafCb != null)
            nodeCtx.leafCb.cb(io, addr, pageId);

        return new TreeNode(pageId, io, sb.toString(), Collections.emptyList());
    }

    private static void printProgress(String caption, long curr, long total, long timeStarted) {
        if (curr > total)
            throw new RuntimeException("Current value can't be greater than total value.");

        int progressTotalLen = 20;

        String progressBarFmt = "\r%s: %4s [%" + progressTotalLen + "s] %s/%s (%s / %s)\r";

        double currRatio = (double)curr / total;
        int percentage = (int)(currRatio * 100);
        int progressCurrLen = (int)(currRatio * progressTotalLen);
        long timeRunning = System.currentTimeMillis() - timeStarted;
        long timeEstimated = (long)(timeRunning / currRatio);

        GridStringBuilder progressBuilder = new GridStringBuilder();

        for (int i = 0; i < progressTotalLen; i++)
            progressBuilder.a(i < progressCurrLen ? "=" : " ");

        SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");

        timeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        String progressBar = String.format(
            progressBarFmt,
            caption,
            percentage + "%",
            progressBuilder.toString(),
            curr,
            total,
            timeFormat.format(new Date(timeRunning)),
            timeFormat.format(new Date(timeEstimated))
        );

        System.out.print(progressBar);
    }

    public static void main(String[] args) throws Exception {
        /*RandomAccessFile file = new RandomAccessFile("C:\\Projects\\ignite\\apache-ignite\\work\\w_idxs_c\\index.bin", "rw");
        byte[] b = new byte[1];
        file.seek(20570);
        file.read(b, 0, 1);
        b[0]++;
        file.write(b, 0, 1);
        System.exit(0);*/

        try {
            String dir = args[0];

            int partCnt = args.length > 1 ? Integer.parseInt(args[1]) : 0;

            int pageSize = args.length > 2 ? Integer.parseInt(args[2]) : 4096;

            int filePageStoreVer = args.length > 3 ? Integer.parseInt(args[3]) : 2;

            int grpId = args.length > 4 ? CU.cacheId(args[4]) : Integer.MAX_VALUE;

            new IgniteIndexReader(dir, pageSize, partCnt, filePageStoreVer, grpId)
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

    private interface TreeNodeGetter {
        TreeNode get(PageIO io, long pageId, long addr, TreeNodeContext nodeCtx);
    }

    private interface PageCallback {
        void cb(PageIO io, long pageAddr, long pageId);
    }

    private interface ItemCallback {
        void cb(long currPageId, Object item, long link);
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

        TreeValidationInfo merge(TreeValidationInfo info) {
            if (info != null) {
                info.ioStat.forEach((k, v) -> ioStat.computeIfAbsent(k, kk -> new AtomicLong(0)).addAndGet(v.get()));

                info.errors.forEach((k, v) -> errors.computeIfAbsent(k, kk -> new HashSet<>()).addAll(v));

                innerPageIds.addAll(info.innerPageIds);

                idxItems.addAll(info.idxItems);
            }

            return this;
        }
    }
}
