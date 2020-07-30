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

package org.apache.ignite.cache.store.jdbc;

import java.io.File;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class Repr13331V3 extends GridCommonAbstractTest {

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    public static final int CNT = 100_000_000;

    @Test
    public void test() throws Exception {
        Ignite srv1 = Ignition.start("modules/spring/src/test/config/node13331_srv1.xml");
        Ignite srv2 = Ignition.start("modules/spring/src/test/config/node13331_srv2.xml");
        Ignite cli = Ignition.start("modules/spring/src/test/config/node13331_cli.xml");

        srv1.cluster().active(true);
        IgniteCache<Integer, String> cache = srv1.getOrCreateCache(new CacheConfiguration<Integer, String>().setName("CACHE").setBackups(1));

        GridTestUtils.runAsync(() -> {
                try (IgniteDataStreamer<Integer, String> streamer = cli.dataStreamer("CACHE")) {
                    for (int i = 0; i < CNT; i++) {
                        streamer.addData(i, "val" + i);
                    }
                }
            }
        );

        // Lets wait for 10 seconds to perform load on stable cluster.
        Thread.sleep(10_000);

        // Restart srv1 with some gap between stop and start
        srv1.close();

        Thread.sleep(10_000);

        Ignition.start("modules/spring/src/test/config/node13331_srv1.xml");

        Thread.sleep(10_000);

//        // Restart srv2 with some gap between stop and start
//        srv2.close();
//
//        Thread.sleep(10_000);
//
//        Ignition.start("modules/spring/src/test/config/node13331_srv2.xml");

        // Lets wait for default checkpoint frequency time in order to guarantee that there's at least one checkpoint.
        Thread.sleep(180_000);

        // And few more seconds to guarantee that segments cleanup took place.
        Thread.sleep(10_000);

        checkThatOnlyZipSegmentExists((IgniteEx)srv1);

        checkThatOnlyZipSegmentExists((IgniteEx)srv2);
    }

    private void checkThatOnlyZipSegmentExists(IgniteEx srv1) throws IgniteCheckedException {
        // Check that there's only 0'zip segment for srv1
        String nodeFolderName = srv1.context().pdsFolderResolver().resolveFolders().folderName();

        File dbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false);
        File walDir = new File(dbDir, "wal");
        File archiveDir = new File(walDir, "archive");
        File nodeArchiveDir = new File(archiveDir, nodeFolderName);
        File walZipSegment = new File(nodeArchiveDir, FileDescriptor.fileName(0) + ".zip");
        File walRawSegment = new File(nodeArchiveDir, FileDescriptor.fileName(0) + ".wal");

        assertTrue(walZipSegment.exists());
        assertFalse(walRawSegment.exists());
    }

}
