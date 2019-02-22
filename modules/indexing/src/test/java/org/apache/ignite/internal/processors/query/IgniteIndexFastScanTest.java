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

package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndex;
import org.apache.ignite.internal.processors.query.h2.database.InlineIndexHelper;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2PlainRowFactory;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.ValueLong;

/**
 *
 */
public class IgniteIndexFastScanTest extends GridCommonAbstractTest {

    public static final String TEST_CACHE = "testCache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("userId", "java.lang.Long");
        fields.put("txId", "java.lang.Long");
        fields.put("data", "byte[]");

        QueryIndex idx = new QueryIndex(Arrays.asList("userId", "txId", "data"), QueryIndexType.SORTED);

        QueryEntity qryEntity = new QueryEntity(Long.class, UserData.class)
            .setFields(fields)
            .setIndexes(Collections.singletonList(idx));

        CacheConfiguration ccfg = new CacheConfiguration(TEST_CACHE)
            .setQueryEntities(Collections.singletonList(qryEntity))
            .setSqlIndexMaxInlineSize(2048);

        cfg.setCacheConfiguration(ccfg);

        // TODO inline that big does not work with default page size.
        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setPageSize(16384)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(8L * 1024 * 1024 * 1024)));

        return cfg;
    }

    public void testScans() throws Exception {
        try {
            IgniteEx grid = startGrid(0);

            IgniteCache<Long, UserData> cache = grid.cache(TEST_CACHE);

            for (long i = 0; i < 500_000; i++) {
                byte[] data = new byte[1536];

                Arrays.fill(data, (byte)i);

                cache.put(i, new UserData(i / 50_000, i, data));

                if (i > 0 && i % 10_000 == 0)
                    System.out.println("Done loading: " + i);
            }

            IgniteH2Indexing idxing = (IgniteH2Indexing)grid.context().query().getIndexing();

            GridH2Table table = idxing.dataTable(TEST_CACHE, "USERDATA");

            H2TreeIndex idx = (H2TreeIndex)table.getIndexes().get(3);

            for (int i = 0; i < 10_000; i++) {
                long start = System.currentTimeMillis();

                long usrId = ThreadLocalRandom.current().nextInt(10);

                Row lower = GridH2PlainRowFactory.create(null, null, null, ValueLong.get(usrId), null, null);

                TestVisitor visitor = new TestVisitor(usrId);

                idx.visitAll(null, lower, visitor);

                System.out.println("Visited userId=" + usrId + ", records=" + visitor.visited + " in " + (System.currentTimeMillis() - start) + "ms");
            }
        }
        finally {
            stopAllGrids();
        }
    }

    private static class UserData {
        private final long userId;
        private final long txId;
        private final byte[] data;

        public UserData(long userId, long txId, byte[] data) {
            this.userId = userId;
            this.txId = txId;
            this.data = data;
        }
    }

    private static class TestVisitor implements BPlusTree.TreeVisitorClosure<SearchRow, GridH2Row> {
        private final long usrId;
        private int visited;

        public TestVisitor(long usrId) {
            this.usrId = usrId;
        }

        @Override
        public int visit(BPlusTree<SearchRow, GridH2Row> tree, BPlusIO<SearchRow> io, long pageAddr, int idx, IgniteWriteAheadLogManager wal) throws IgniteCheckedException {
            H2Tree h2Tree = (H2Tree)tree;

            List<InlineIndexHelper> inlineIdxs = h2Tree.inlineIndexes();

            int inlineSize = h2Tree.inlineSize();

            int off = io.offset(idx);

            int fieldOff = 0;

            long userId = 0;
            long txId = 0;
            long valBytesAddr = 0;
            int valBytesSize = 0;

            // TODO this is obviously hard-coded values.
            for (int i = 0; i < 3; i++) {
                InlineIndexHelper inlineIdx = inlineIdxs.get(i);

                switch (i) {
                    case 0:
                        userId = inlineIdx.getValueLong(pageAddr, off + fieldOff, inlineSize - fieldOff);

                        break;

                    case 1:
                        txId = inlineIdx.getValueLong(pageAddr, off + fieldOff, inlineSize - fieldOff);

                        break;

                    case 2:
                        valBytesAddr = inlineIdx.getValueBytesAddress(pageAddr, off + fieldOff, inlineSize - fieldOff);
                        valBytesSize = inlineIdx.getValueBytesSize(pageAddr, off + fieldOff, inlineSize - fieldOff);

                        break;

                    default:
                        throw new IllegalStateException();
                }

                fieldOff += inlineIdx.fullSize(pageAddr, off + fieldOff);

                if (fieldOff > inlineSize)
                    break;
            }

            if (userId == usrId && txId >= 0 && valBytesSize >= 0 && valBytesAddr >= 0) {
                visited++;

                if (visited >= 50_000)
                    return BPlusTree.TreeVisitorClosure.STOP;
            }
            else
                return BPlusTree.TreeVisitorClosure.STOP;

            return 0;
        }

        @Override
        public int state() {
            return 0;
        }
    }
}
