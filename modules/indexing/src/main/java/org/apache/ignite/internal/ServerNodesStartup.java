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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Collections;

public class ServerNodesStartup {

    protected static final String CACHE_NAME = "default";

    public static IgniteConfiguration getIgniteConfiguration(int i) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName("ignite" + i);

        cfg.setLongQueryWarningTimeout(1000);

        cfg.setDiscoverySpi(
                new TcpDiscoverySpi()
                        .setIpFinder(
                                new TcpDiscoveryVmIpFinder()
                                        .setAddresses(Collections.singleton("127.0.0.1:47500..47509"))
                        )
        );

        cfg.setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                                .setPersistenceEnabled(true)
                                .setInitialSize(10 * 1024L * 1024L)
                                .setMaxSize(50 * 1024L * 1024L)
        ));

        CacheConfiguration cacheConfiguration = new CacheConfiguration(CACHE_NAME)
                .setBackups(1)
                .setSqlSchema("PUBLIC");

        cfg.setCacheConfiguration(cacheConfiguration);

        return cfg;
    }


    /**
     * Trying to reproduce long query and query export to csv failed.
     *
     * Start 3 server nodes.
     * Create and populate table person.
     * Run VisorGuiLauncher.
     * Execute and export query.
     *
     * @param args Not used.
     * @throws Exception if Failed.
     */
    public static void main(String[] args) throws Exception{
        Ignite ignite = null;

        for (int i = 0; i < 3; i++) {
            ignite = Ignition.start(getIgniteConfiguration(i));
        }

        ignite.cluster().active(true);

        IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME);

        cache.query(new SqlFieldsQuery("CREATE TABLE PERSON (id int, name varchar, PRIMARY KEY (ID)) WITH  \"BACKUPS=1\""));

        for (int i = 0; i < 950_000; i++)
            cache.query(new SqlFieldsQuery("insert into PERSON (id, name) values (?, ?)").setArgs(i,"person" + i));

        U.sleep(Long.MAX_VALUE);
    }
}
