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
import java.util.HashSet;
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
import org.apache.ignite.internal.util.GridUnsafe;

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
    private void findLostRootPages(String cacheWorkDirPath, int partCnt, int pageSize, int filePageStoreVer) throws IgniteCheckedException {
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
            AllocatedPageTracker.NO_OP
        )
            : new FilePageStoreV2(
            PageMemory.FLAG_IDX,
            idxFile,
            new AsyncFileIOFactory(),
            dsCfg,
            AllocatedPageTracker.NO_OP
        );

        Set<Long> treeMetaPageIds = new HashSet<>();
        Set<Long> bPlusMetaIds = new HashSet<>();

        for (int i = 0; i < (fileSize - idxPageStore.headerSize()) / pageSize; i++) {
            ByteBuffer buf = GridUnsafe.allocateBuffer(pageSize);

            try {
                long addr = GridUnsafe.bufferAddress(buf);

                idxPageStore.readByOffset(i * pageSize + idxPageStore.headerSize(), buf, false);

                PageIO io = PageIO.getPageIO(addr);

                if (io instanceof PageMetaIO) {
                    PageMetaIO pageMetaIO = (PageMetaIO)io;

                    treeMetaPageIds.add(pageMetaIO.getTreeRoot(addr));
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

                //System.out.println(io.getClass().getSimpleName());
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
    }

    public static void main(String[] args) throws IgniteCheckedException, IOException {
        /*RandomAccessFile file = new RandomAccessFile("C:\\Projects\\ignite\\apache-ignite\\work\\w_idxs_corrupted\\index.bin", "rw");
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

            new IgniteIndexReader().findLostRootPages(dir, partCnt, pageSize, filePageStoreVer);
        }
        catch (Exception e) {
            System.out.println("options: path [partCnt] [pageSize] [filePageStoreVersion]");

            throw e;
        }
    }
}
