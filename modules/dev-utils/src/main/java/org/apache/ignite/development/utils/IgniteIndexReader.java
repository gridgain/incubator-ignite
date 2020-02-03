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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.AllocatedPageTracker;
import org.apache.ignite.internal.processors.cache.persistence.IndexStorageImpl;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreV2;
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
    static {
        PageIO.registerH2(H2InnerIO.VERSIONS, H2LeafIO.VERSIONS);

        H2ExtrasInnerIO.register();
        H2ExtrasLeafIO.register();
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

                    printMetaTree(idxPageStore, pageSize, pageMetaIO.getTreeRoot(addr));
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
                    System.out.println("First error occured on iteration step " + i);

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
    private void printMetaTree(FilePageStore store, int pageSize, long pageId) throws IgniteCheckedException {
        System.out.println("---Printing meta tree---");

        TreeNode rootNode = getTreeNode(store, pageSize, pageId);

        String s = new MetaTreePrinter().print(rootNode);

        System.out.println(s);

        System.out.println("------------------------");
    }

    private TreeNode getTreeNode(FilePageStore store, int pageSize, long pageId) throws IgniteCheckedException {
        ByteBuffer buf = GridUnsafe.allocateBuffer(pageSize);

        try {
            long addr = GridUnsafe.bufferAddress(buf);

            store.read(pageId, buf, false);

            PageIO io = PageIO.getPageIO(addr);

            if (io instanceof BPlusMetaIO) {
                BPlusMetaIO bPlusMetaIO = (BPlusMetaIO)io;

                int rootLvl = bPlusMetaIO.getRootLevel(addr);
                long rootId = bPlusMetaIO.getFirstPageId(addr, rootLvl);

                return new TreeNode("BPlusMeta [id=" + pageId + "]", Collections.singletonList(getTreeNode(store, pageSize, rootId)));
            }
            else if (io instanceof IndexStorageImpl.MetaStoreInnerIO) {
                IndexStorageImpl.MetaStoreInnerIO metaInnerIo = (IndexStorageImpl.MetaStoreInnerIO)io;

                int cnt = metaInnerIo.getCount(addr);

                List<Long> childrenIds;

                if (cnt > 0) {
                    childrenIds = new ArrayList<>(cnt + 1);

                    for (int i = 0; i < cnt; i++)
                        childrenIds.add(metaInnerIo.getLeft(addr, i));

                    childrenIds.add(metaInnerIo.getRight(addr, cnt - 1));
                }
                else {
                    long left = metaInnerIo.getLeft(addr, 0);

                    childrenIds = left == 0 ? Collections.<Long>emptyList() : Collections.singletonList(left);
                }

                List<TreeNode> children = new ArrayList<>(childrenIds.size());

                for (Long id : childrenIds)
                    children.add(getTreeNode(store, pageSize, id));

                return new TreeNode("Meta inner IO [id=" + pageId + "]", children);
            }
            else if (io instanceof IndexStorageImpl.MetaStoreLeafIO) {
                IndexStorageImpl.MetaStoreLeafIO metaLeafIO = (IndexStorageImpl.MetaStoreLeafIO)io;

                GridStringBuilder sb = new GridStringBuilder("Meta leaf [id=" + pageId + ", ");

                for (int j = 0; j < (pageSize - ITEMS_OFF) / metaLeafIO.getItemSize(); j++) {
                    //System.out.println("offset: " + String.valueOf(i * pageSize + idxPageStore.headerSize()));
                    IndexStorageImpl.IndexItem indexItem = metaLeafIO.getLookupRow(null, addr, j);

                    if (indexItem.pageId() != 0)
                        sb.a(indexItem.toString());
                }

                sb.a("]");

                return new TreeNode(sb.toString(), Collections.emptyList());
            }
            else
                return new TreeNode("Unknown node [id=" + pageId + "]", Collections.emptyList());
        }
        catch (Exception e) {
            return new TreeNode("Could not read this node! id=" + pageId + ", exception: " + e.getMessage(), Collections.emptyList());
        }
        finally {
            GridUnsafe.freeBuffer(buf);
        }
    }

    public static void main(String[] args) throws IgniteCheckedException, IOException {
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

            new IgniteIndexReader().findLostRootPages(dir, partCnt, pageSize, filePageStoreVer, grpId);
        }
        catch (Exception e) {
            System.out.println("options: path [partCnt] [pageSize] [filePageStoreVersion]");

            throw e;
        }
    }

    private static class TreeNode {
        final String s;
        final List<TreeNode> children;

        public TreeNode(String s, List<TreeNode> children) {
            this.s = s;
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
            return treeNode.s;
        }
    }
}
