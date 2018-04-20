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

package org.apache.ignite.internal;

import java.io.File;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridDebug;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests leaks.
 */
public class MemoryLeaksOnRestartNodeTest extends GridCommonAbstractTest {
    /** Heap dump file name. */
    private static final String HEAP_DUMP_FILE_NAME = "test.hprof";

    /** Restarts count. */
    private static final int RESTARTS = 10;

    /** Nodes count. */
    private static final int NODES = 3;

    /** Allow 5Mb leaks on node restart. */
    private static final int ALLOW_LEAK_ON_RESTART_IN_MB = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_DELAYED_REPLACED_PAGE_WRITE, "true");

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setName("mem0").setPersistenceEnabled(false))
            .setDataRegionConfigurations(new DataRegionConfiguration[] {
                new DataRegionConfiguration().setName("disk").setPersistenceEnabled(true),
                new DataRegionConfiguration().setName("mem2").setPersistenceEnabled(false)
            }));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /**
     * @throws Exception On failed.
     */
    public void test() throws Exception {
        // Warmup
        for (int i = 0; i < RESTARTS / 2; ++i) {
            startGrids(NODES);

            Thread.sleep(500);

            stopAllGrids();
        }

        GridDebug.dumpHeap(HEAP_DUMP_FILE_NAME, true);

        final long size0 = new File(HEAP_DUMP_FILE_NAME).length();

        // Restarts
        for (int i = 0; i < RESTARTS; ++i) {
            startGrids(NODES);

            Thread.sleep(500);

            stopAllGrids();
        }

        GridDebug.dumpHeap(HEAP_DUMP_FILE_NAME, true);

        final long size1 = new File(HEAP_DUMP_FILE_NAME).length();

        final long leakSize = ((size1 - size0) >> 20);

        assertTrue("Possible leaks detected. The " + leakSize + "M leaks after " + RESTARTS + " restarts. " +
                "See the '" + new File(HEAP_DUMP_FILE_NAME).getAbsolutePath() + "'",
            leakSize < RESTARTS * ALLOW_LEAK_ON_RESTART_IN_MB * NODES);
   }
}