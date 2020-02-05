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
import java.util.Collection;
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
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.AllocatedPageTracker;
import org.apache.ignite.internal.processors.cache.persistence.IndexStorageImpl;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreV2;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageMetaIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2LeafIO;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.lang.GridTreePrinter;
import org.apache.ignite.internal.util.typedef.internal.CU;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO.ITEMS_OFF;

public class IgniteIndexReader {
    private static final String META_TREE_NAME = "MetaTree";

    static {
        PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS);

        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();
    }

    private final int pageSize;
    private final int filePageStoreVer;
    private final int grpId;

    public IgniteIndexReader(int pageSize, int filePageStoreVer, int grpId) {
        this.pageSize = pageSize;
        this.filePageStoreVer = filePageStoreVer;
        this.grpId = grpId;
    }

    private void findLostRootPages(String cacheWorkDirPath, int partCnt, int pageSize, int filePageStoreVer, int grpId) throws IgniteCheckedException {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setPageSize(pageSize);

        File cacheWorkDir = new File(cacheWorkDirPath);

        File idxFile = new File(cacheWorkDir, INDEX_FILE_NAME);

        if (!idxFile.exists())
            throw new RuntimeException("File not found: " + idxFile.getPath());
        else
            System.out.println("Analyzing file: " + idxFile.getPath());

        long fileSize = idxFile.length();

        FilePageStore idxPageStore = filePageStoreVer == 1
            ? new FilePageStore(
            PageMemory.FLAG_IDX,
            idxFile,
            new AsyncFileIOFactory(),
            dsCfg,
            AllocatedPageTracker.NO_OP,
            grpId
        )
            : new FilePageStoreV2(
            PageMemory.FLAG_IDX,
            idxFile,
            new AsyncFileIOFactory(),
            dsCfg,
            AllocatedPageTracker.NO_OP,
            grpId
        );

        List<FilePageStore> partPageStores = new ArrayList<>(partCnt);

        for (int i = 0; i < partCnt; i++) {
            int partId = i;

            /*partPageStores.add(
                new FilePageStoreV2(
                    FLAG_DATA,
                    () -> getPartitionFilePath(cacheWorkDir, partId),
                    new AsyncFileIOFactory(),
                    dsCfg,
                    new LongAdderMetric("name", "desc")
                )
            );*/
        }

        Set<Long> treeMetaPageIds = new HashSet<>();
        Set<Long> bPlusMetaIds = new HashSet<>();

        Map<Class<? extends PageIO>, Long> pageClasses = new HashMap<>();

        long errorsNum = 0;

        final long pagesNum = (fileSize - idxPageStore.headerSize()) / pageSize;

        System.out.println("Going to check " + pagesNum + " pages.");

        for (int i = 0; i < pagesNum; i++) {
            ByteBuffer buf = GridUnsafe.allocateBuffer(pageSize);

            try {
                long addr = GridUnsafe.bufferAddress(buf);

                //We got int overflow here on sber dataset.
                final long off = (long)i * pageSize + idxPageStore.headerSize();

                idxPageStore.readByOffset(off, buf, false);

                PageIO io = PageIO.getPageIO(addr);

                pageClasses.merge(io.getClass(), 1L, (oldVal, newVal) -> ++oldVal);

                if (io instanceof PageMetaIO) {
                    PageMetaIO pageMetaIO = (PageMetaIO)io;

                    treeMetaPageIds.add(pageMetaIO.getTreeRoot(addr));

                    //printMetaTree(idxPageStore, pageSize, pageMetaIO.getTreeRoot(addr));
                    validateAllTrees(idxPageStore, partPageStores, pageMetaIO.getTreeRoot(addr));
                    System.exit(0);
                }
                else if (io instanceof IndexStorageImpl.MetaStoreLeafIO) {
                    IndexStorageImpl.MetaStoreLeafIO metaStoreLeafIO = (IndexStorageImpl.MetaStoreLeafIO)io;

                    for (int j = 0; j < (pageSize - ITEMS_OFF) / metaStoreLeafIO.getItemSize(); j++) {
                        //System.out.println("offset: " + String.valueOf(i * pageSize + idxPageStore.headerSize()));
                        IndexStorageImpl.IndexItem indexItem = metaStoreLeafIO.getLookupRow(null, addr, j);

                        if (indexItem.pageId() != 0) {
                            ByteBuffer idxMetaBuf = GridUnsafe.allocateBuffer(pageSize); // for tree meta page

                            try {
                                long idxMetaBufAddr = GridUnsafe.bufferAddress(idxMetaBuf);

                                idxPageStore.read(indexItem.pageId(), idxMetaBuf, false);

                                treeMetaPageIds.add(indexItem.pageId());
                            }
                            finally {
                                GridUnsafe.freeBuffer(idxMetaBuf);
                            }
                        }
                    }
                }
                else if (io instanceof BPlusMetaIO) {
                    BPlusMetaIO bPlusMetaIO = (BPlusMetaIO)io;

                    long pageId = PageIdUtils.pageId(INDEX_PARTITION, FLAG_IDX, i);

                    bPlusMetaIds.add(pageId);
                }
            } catch (Throwable e) {
                if (errorsNum < 1) {
                    System.out.println("First error occurred on iteration step " + i);

                    System.out.println("Exception occurred: " + e.toString());
                    e.printStackTrace();
                }

                errorsNum++;
            }
            finally {
                GridUnsafe.freeBuffer(buf);
            }
        }

        System.out.println("---Meta tree entries without actual index trees:");

        for (Long id : treeMetaPageIds) {
            if (!bPlusMetaIds.contains(id))
                System.out.println(id);
        }

        System.out.println("---");
        System.out.println();
        System.out.println("---Index root pages missing in meta tree: ");

        for (Long id : bPlusMetaIds) {
            if (!treeMetaPageIds.contains(id))
                System.out.println(id);
        }

        System.out.println("---");

        System.out.println("---These pages types were encountered:");
        pageClasses.forEach((key, val) -> System.out.println(key.getSimpleName() + ": " + val));

        System.out.println("---");
        System.out.println("Total errors number: " + errorsNum);
    }

    /** */
    private void printMetaTree(FilePageStore store, int pageSize, long pageId) {
        System.out.println("---Printing meta tree---");

        TreeNode rootNode = getTreeNode(store, pageSize, pageId, new HashMap<>(), new HashMap<>(), null, null);

        String s = new MetaTreePrinter().print(rootNode);

        System.out.println(s);

        System.out.println("------------------------");
    }

    private void validateAllTrees(
        FilePageStore idxStore,
        List<FilePageStore> partStores,
        long metaTreeRootPageId
    ) {
        Map<String, TreeValidationInfo> treeInfos = new HashMap<>();

        TreeValidationInfo metaTreeValidationInfo = validateTree(idxStore, partStores, metaTreeRootPageId, true);

        treeInfos.put(META_TREE_NAME, metaTreeValidationInfo);

        AtomicInteger progress = new AtomicInteger(0);

        long startTime = System.currentTimeMillis();

        System.out.println();

        metaTreeValidationInfo.idxItems.forEach(item -> {
            printProgress("Index trees validation", progress.incrementAndGet(), metaTreeValidationInfo.idxItems.size(), startTime);

            IndexStorageImpl.IndexItem idxItem = (IndexStorageImpl.IndexItem)item;

            TreeValidationInfo treeValidationInfo = validateTree(idxStore, partStores, idxItem.pageId(), false);

            treeInfos.put(idxItem.toString(), treeValidationInfo);
        });

        printValidationResults(treeInfos);
    }

    private void printValidationResults(Map<String, TreeValidationInfo> treeInfos) {
        System.out.println("Validation results: ");

        Map<Class, AtomicLong> totalStat = new HashMap<>();

        AtomicInteger totalErr = new AtomicInteger(0);

        treeInfos.forEach((idxName, validationInfo) -> {
            System.out.println("-----");
            System.out.println("Index tree: " + idxName);
            System.out.println("-- Page stat:");

            validationInfo.ioStat.forEach((cls, cnt) -> {
                System.out.println(cls.getSimpleName() + ": " + cnt.get());

                totalStat.computeIfAbsent(cls, k -> new AtomicLong(0)).addAndGet(cnt.get());
            });

            if (validationInfo.errors.size() > 0) {
                System.out.println("-- Errors:");

                validationInfo.errors.forEach((id, errors) -> {
                    System.out.println("Page id=" + id + ", errors:");

                    errors.forEach(e -> e.printStackTrace());

                    totalErr.addAndGet(errors.size());
                });
            }
        });

        System.out.println("-- Total page stat:");

        totalStat.forEach((cls, cnt) -> System.out.println(cls.getSimpleName() + ": " + cnt.get()));

        System.out.println();
        System.out.println("Total pages validated in tree: " + totalStat.values().stream().mapToLong(a -> a.get()).sum());
        System.out.println("Total errors: " + totalErr.get());
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

        Set<Long> leafPageIds = new HashSet<>();

        LeafCallback leafCb = isMetaTree
            ? (io, pageAddr, pageId) -> leafPageIds.add(pageId)
            : null;

        List<Object> idxItems = new LinkedList<>();

        ItemCallback itemCb = isMetaTree
            ? (currPageId, targetPageId, link) -> idxItems.add(targetPageId)
            : null;

        getTreeNode(idxStore, pageSize, rootPageId, ioStat, errors, leafCb, itemCb);

        return new TreeValidationInfo(ioStat, errors, leafPageIds, idxItems);
    }

    private TreeNode getTreeNode(
        FilePageStore store,
        int pageSize,
        long pageId,
        Map<Class, AtomicLong> ioStat,
        Map<Long, Set<Throwable>> errors,
        LeafCallback leafCb,
        ItemCallback itemCb
    ) {
        final ByteBuffer buf = GridUnsafe.allocateBuffer(pageSize);

        try {
            final long addr = GridUnsafe.bufferAddress(buf);

            store.read(pageId, buf, false);

            final PageIO io = PageIO.getPageIO(addr);

            ioStat.computeIfAbsent(io.getClass(), k -> new AtomicLong(0)).incrementAndGet();

            if (io instanceof BPlusMetaIO) {
                BPlusMetaIO bPlusMetaIO = (BPlusMetaIO)io;

                int rootLvl = bPlusMetaIO.getRootLevel(addr);
                long rootId = bPlusMetaIO.getFirstPageId(addr, rootLvl);

                return new TreeNode(pageId, io, null, Collections.singletonList(getTreeNode(store, pageSize, rootId, ioStat, errors, leafCb, itemCb)));
            }
            else if (io instanceof BPlusInnerIO) {
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

                    childrenIds = left == 0 ? Collections.<Long>emptyList() : Collections.singletonList(left);
                }

                List<TreeNode> children = new ArrayList<>(childrenIds.size());

                for (Long id : childrenIds)
                    children.add(getTreeNode(store, pageSize, id, ioStat, errors, leafCb, itemCb));

                return new TreeNode(pageId, io, null, children);
            }
            else if (io instanceof BPlusLeafIO) {
                GridStringBuilder sb = new GridStringBuilder();

                if (io instanceof IndexStorageImpl.MetaStoreLeafIO) {
                    IndexStorageImpl.MetaStoreLeafIO metaLeafIO = (IndexStorageImpl.MetaStoreLeafIO)io;

                    for (int j = 0; j < (pageSize - ITEMS_OFF) / metaLeafIO.getItemSize(); j++) {
                        //System.out.println("offset: " + String.valueOf(i * pageSize + idxPageStore.headerSize()));
                        IndexStorageImpl.IndexItem indexItem = metaLeafIO.getLookupRow(null, addr, j);

                        if (indexItem.pageId() != 0) {
                            sb.a(indexItem.toString() + " ");

                            if (itemCb != null)
                                itemCb.cb(pageId, indexItem, 0);
                        }
                    }
                }
                else {

                }

                if (leafCb != null)
                    leafCb.cb(io, addr, pageId);

                return new TreeNode(pageId, io, sb.toString(), Collections.emptyList());
            }
            else {
                errors.computeIfAbsent(pageId, k -> new HashSet<>())
                    .add(new Exception("Unexpected page io: " + io.getClass().getSimpleName()));

                return new TreeNode(pageId, null, null, Collections.emptyList());
            }
        }
        catch (Exception e) {
            errors.computeIfAbsent(pageId, k -> new HashSet<>()).add(e);

            return new TreeNode(pageId, null, "exception: " + e.getMessage(), Collections.emptyList());
        }
        finally {
            GridUnsafe.freeBuffer(buf);
        }
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

            int partCnt = args.length > 1 ? Integer.parseInt(args[1]) : 1024;

            int pageSize = args.length > 2 ? Integer.parseInt(args[2]) : 4096;

            int filePageStoreVer = args.length > 3 ? Integer.parseInt(args[3]) : 2;

            int grpId = args.length > 4 ? CU.cacheId(args[4]) : Integer.MAX_VALUE;

            new IgniteIndexReader(pageSize, filePageStoreVer, grpId)
                .findLostRootPages(dir, partCnt, pageSize, filePageStoreVer, grpId);
        }
        catch (Exception e) {
            System.out.println("options: path [partCnt] [pageSize] [filePageStoreVersion]");

            throw e;
        }
    }

    private static class TreeNode {
        final long pageId;
        final PageIO io;
        final String additionalInfo;
        final List<TreeNode> children;

        public TreeNode(long pageId, PageIO io, String additionalInfo, List<TreeNode> children) {
            this.pageId = pageId;
            this.io = io;
            this.additionalInfo = additionalInfo;
            this.children = children;
        }
    }

    private static class MetaTreePrinter extends GridTreePrinter<TreeNode> {
        /** {@inheritDoc} */
        @Override protected List<TreeNode> getChildren(TreeNode treeNode) {
            return treeNode.children;
        }

        /** {@inheritDoc} */
        @Override protected String formatTreeNode(TreeNode treeNode) {
            return new GridStringBuilder()
                .a(treeNode.io == null ? "UnknownPageType" : treeNode.io.getClass().getSimpleName())
                .a(" [pageId=").a(treeNode.pageId)
                .a(treeNode.additionalInfo == null ? "" : ", " + treeNode.additionalInfo)
                .a("]")
                .toString();
        }
    }

    private interface LeafCallback {
        void cb(PageIO io, long pageAddr, long pageId);
    }

    private interface ItemCallback {
        void cb(long currPageId, Object item, long link);
    }

    private static class TreeValidationInfo {
        final Map<Class, AtomicLong> ioStat;
        final Map<Long, Set<Throwable>> errors;
        final Collection<Long> leafPageIds;
        final List<Object> idxItems;

        public TreeValidationInfo(
            Map<Class, AtomicLong> ioStat,
            Map<Long, Set<Throwable>> errors,
            Collection<Long> leafPageIds,
            List<Object> idxItems
        ) {
            this.ioStat = ioStat;
            this.errors = errors;
            this.leafPageIds = leafPageIds;
            this.idxItems = idxItems;
        }
    }
}
