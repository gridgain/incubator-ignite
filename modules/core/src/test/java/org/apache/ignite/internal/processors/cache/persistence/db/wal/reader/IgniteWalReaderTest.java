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

package org.apache.ignite.internal.processors.cache.persistence.db.wal.reader;

import java.io.File;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.mockito.Mockito;

import static org.apache.ignite.configuration.PersistentStoreConfiguration.DFLT_WAL_SEGMENTS;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

/**
 *
 */
public class IgniteWalReaderTest extends GridCommonAbstractTest {

    public static final int WAL_SEGMENTS = 2;
    private static String cacheName = "cache0";

    private static boolean fillWalBeforeTest = true;
    private static boolean deleteBefore = true;
    private static boolean deleteAfter = true;
    private static boolean dumpRecords = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, IgniteWalReaderTest.IndexedObject> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setIndexedTypes(Integer.class, IgniteWalReaderTest.IndexedObject.class);

        cfg.setCacheConfiguration(ccfg);

        MemoryConfiguration dbCfg = new MemoryConfiguration();

        dbCfg.setPageSize(4 * 1024);

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();

        memPlcCfg.setName("dfltMemPlc");
        memPlcCfg.setInitialSize(1024 * 1024 * 1024);
        memPlcCfg.setMaxSize(1024 * 1024 * 1024);

        dbCfg.setMemoryPolicies(memPlcCfg);
        dbCfg.setDefaultMemoryPolicyName("dfltMemPlc");

        cfg.setMemoryConfiguration(dbCfg);

        PersistentStoreConfiguration pCfg = new PersistentStoreConfiguration();
        pCfg.setWalHistorySize(1);
        pCfg.setWalSegments(WAL_SEGMENTS);
        cfg.setPersistentStoreConfiguration(pCfg);

        BinaryConfiguration binCfg = new BinaryConfiguration();

        binCfg.setCompactFooter(false);

        cfg.setBinaryConfiguration(binCfg);
        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        stopAllGrids();

        if (deleteBefore)
            deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        if (deleteAfter)
            deleteWorkFiles();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        if (fillWalBeforeTest)
            deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
    }

    /**
     * @throws Exception if failed.
     */
    public void testFillWalAndReadRecords() throws Exception {
        if (fillWalBeforeTest) {
            Ignite ignite0 = startGrid("node0");
            ignite0.active(true);

            IgniteCache<Object, Object> cache0 = ignite0.cache(cacheName);

            for (int i = 0; i < 10000; i++)
                cache0.put(i, new IgniteWalReaderTest.IndexedObject(i));

            stopGrid("node0");
        }

        File db = U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false);
        File wal = new File(db, "wal");
        File walArchive = new File(wal, "archive");
        int pageSize = 1024 * 4;
        int segments = WAL_SEGMENTS;
        String consistentId = "127_0_0_1_47500";
        MockWalIteratorFactory mockItFactory = new MockWalIteratorFactory(log, pageSize, consistentId, segments);
        WALIterator it =   mockItFactory.iterator(wal, walArchive);

        int count = 0;
        while (it.hasNextX()) {
            IgniteBiTuple<WALPointer, WALRecord> next = it.nextX();
            if(dumpRecords) System.out.println("Record: " + next.get2());
            count++;
        }
        System.out.println("Total records loaded " + count);
        assert count > 0;

        final File walDirWithConsistentId = new File(wal, consistentId);
        final File walArchiveDirWithConsistentId = new File(walArchive, consistentId);

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log, pageSize);
        int count2 = 0;

        try (WALIterator stIt = factory.iterator(walArchiveDirWithConsistentId)) {
            while (stIt.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> next = stIt.nextX();
                if(dumpRecords) System.out.println("Arch. Record: " + next.get2());
                count2++;
            }
        }

        try (WALIterator stIt = factory.iterator(walDirWithConsistentId)) {
            while (stIt.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> next = stIt.nextX();
                if(dumpRecords) System.out.println("Work. Record: " + next.get2());
                count2++;
            }
        }
        System.out.println("Total records loaded2: " + count2);
        assert count == count2 : "Mock based reader loaded " + count + " records but standalone has loaded only " + count2;
    }

    /**
     *
     */
    private static class IndexedObject {
        /** */
        @QuerySqlField(index = true)
        private int iVal;

        /**
         * @param iVal Integer value.
         */
        byte[] data;

        /**
         * @param iVal Integer value.
         */
        private  IndexedObject(int iVal) {
            this.iVal = iVal;
            int sz = 40000;
            data = new byte[sz];
            for (int i = 0; i < sz; i++)
                data[i] = (byte)('A' + (i % 10));
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof IgniteWalReaderTest.IndexedObject))
                return false;

            IgniteWalReaderTest.IndexedObject that = (IgniteWalReaderTest.IndexedObject)o;

            return iVal == that.iVal;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return iVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IgniteWalReaderTest.IndexedObject.class, this);
        }
    }
}
