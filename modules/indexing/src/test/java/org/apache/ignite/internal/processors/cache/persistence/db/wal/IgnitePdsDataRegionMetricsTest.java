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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME;

/**
 *
 */
public class IgnitePdsDataRegionMetricsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final long INIT_REGION_SIZE = 10 << 20;

    /** */
    private static final long MAX_REGION_SIZE = INIT_REGION_SIZE * 10;

    /** */
    private static final String CREATE_QUERY =
        "CREATE TABLE Person(ID INTEGER PRIMARY KEY, NAME VARCHAR(100)) WITH \"TEMPLATE=PARTITIONED, BACKUPS=1\"";

    /** */
    private static final String INSERT_QUERY = "INSERT INTO Person(ID, NAME) VALUES (1, 'Ed'), (2, 'Ann'), (3, 'Emma')";

    /** */
    private static final String DROP_QUERY = "DROP TABLE IF EXISTS Person";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setInitialSize(INIT_REGION_SIZE)
                    .setMaxSize(MAX_REGION_SIZE)
                    .setPersistenceEnabled(true)
                    .setMetricsEnabled(true))
            .setCheckpointFrequency(1000);

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>()
            .setName(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    public void testTotalAllocatedSizeOnTableRecreation() throws Exception {
        final IgniteEx node = startGrid(0);

        node.cluster().active(true);
        node.context().query().querySqlFields(new SqlFieldsQuery(CREATE_QUERY), true, false);
        node.context().query().querySqlFields(new SqlFieldsQuery(INSERT_QUERY), true, false);

        DataRegionMetrics firstMetrics = getDfltRegionMetrics(node);
        node.log().info("Allocated size on first execution: " + firstMetrics.getTotalAllocatedSize());

        node.context().query().querySqlFields(new SqlFieldsQuery(DROP_QUERY), true, false);
        node.context().query().querySqlFields(new SqlFieldsQuery(CREATE_QUERY), true, false);
        node.context().query().querySqlFields(new SqlFieldsQuery(INSERT_QUERY), true, false);

        DataRegionMetrics secondMetrics = getDfltRegionMetrics(node);
        node.log().info("Allocated size on second execution: " + secondMetrics.getTotalAllocatedSize());

        assertTrue(firstMetrics.getTotalAllocatedSize() >= secondMetrics.getTotalAllocatedSize());

        stopGrid(0, true);
    }

    /** */
    private static DataRegionMetrics getDfltRegionMetrics(Ignite node) {
        for (DataRegionMetrics m : node.dataRegionMetrics())
            if (DFLT_DATA_REG_DEFAULT_NAME.equals(m.getName()))
                return m;

        throw new RuntimeException("No metrics found for default data region");
    }
}
