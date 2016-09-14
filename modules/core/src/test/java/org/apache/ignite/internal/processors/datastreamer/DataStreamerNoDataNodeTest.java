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
package org.apache.ignite.internal.processors.datastreamer;

import org.apache.ignite.*;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.stream.StreamReceiver;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Collection;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test data streamer logging if target cache have no data nodes
 */
public class DataStreamerNoDataNodeTest extends GridCommonAbstractTest {

    /** */
    private GridStringLogger strLog;

    /** Cache name. */
    public static final String CACHE_NAME = "NoDataNodesCache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration());
        cfg.setGridLogger(strLog = new GridStringLogger());
        return cfg;
    }

    /**
     * Gets cache configuration.
     *
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(CACHE_NAME);
        cacheCfg.setNodeFilter(node -> false);

        return cacheCfg;
    }

    /**
     * Test logging on {@code DataStreamer.addData()} method when cache have no data nodes
     * @throws Exception If fail.
     */
    public void testNoDataNodesCacheStreaming() throws Exception {
        Ignite ignite = startGrid(1);

        try (IgniteDataStreamer ids = ignite.dataStreamer(CACHE_NAME)) {
            ids.addData(1, 1);

            String result = strLog.toString();

            assert result.contains("Failed to find server node for cache (all affinity nodes have left the grid or cache was stopped): NoDataNodesCache") : result;
        }
        finally {
            stopAllGrids();
        }
    }

}
