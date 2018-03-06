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

package org.apache.ignite.examples.persistentstore;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.examples.datagrid.CacheQueryExample;
import org.apache.ignite.examples.model.Organization;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * This example demonstrates the usage of Apache Ignite Persistent Store.
 * <p>
 * To execute this example you should start an instance of {@link PersistentStoreExampleNodeStartup}
 * class which will start up an Apache Ignite remote server node with a proper configuration.
 * <p>
 * When {@code UPDATE} parameter of this example is set to {@code true}, the example will populate
 * the cache with some data and will then run a sample SQL query to fetch some results.
 * <p>
 * When {@code UPDATE} parameter of this example is set to {@code false}, the example will run
 * the SQL query against the cache without the initial data pre-loading from the store.
 * <p>
 * You can populate the cache first with {@code UPDATE} set to {@code true}, then restart the nodes and
 * run the example with {@code UPDATE} set to {@code false} to verify that Apache Ignite can work with the
 * data that is in the persistence only.
 */
public class PersistentStoreExample {
    /** Organizations cache name. */
    private static final String ORG_CACHE = CacheQueryExample.class.getSimpleName() + "Organizations";

    /** */
    private static final boolean UPDATE = true;

    /**
     * @param args Program arguments, ignored.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "cp", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "binary_meta", false));

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setMvccEnabled(true);

        // Durable Memory configuration.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        cfg.setDataStorageConfiguration(storageCfg);

        // Creating a new data region.
        DataRegionConfiguration regionCfg1 = new DataRegionConfiguration();
        regionCfg1.setName("PERS_region");
        regionCfg1.setPersistenceEnabled(true);
        regionCfg1.setInitialSize(100L * 1024 * 1024);
        regionCfg1.setMaxSize(500L * 1024 * 1024);

        // Creating a new data region.
        DataRegionConfiguration regionCfg2 = new DataRegionConfiguration();
        regionCfg2.setName("IN_MEM_region");
        regionCfg2.setPersistenceEnabled(false);
        regionCfg2.setInitialSize(100L * 1024 * 1024);
        regionCfg2.setMaxSize(500L * 1024 * 1024);

        storageCfg.setDataRegionConfigurations(regionCfg1, regionCfg2);

        try (Ignite ignite = Ignition.start(cfg)) {

            ignite.cluster().active(true);

            // cache1
            CacheConfiguration<Long, Integer> cacheCfg1 = new CacheConfiguration<>("cache1");
            cacheCfg1.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheCfg1.setBackups(0);
            cacheCfg1.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            IgniteCache<Long, Integer> cache1 = ignite.getOrCreateCache(cacheCfg1);
            for (int j = 0; j < 2; j++)
                for (int i = 0; i < 5; i++)
                    cache1.put((long)i, i*i);


            // cache2
            CacheConfiguration<Long, Integer> cacheCfg2 = new CacheConfiguration<>("cache2");
            cacheCfg2.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheCfg2.setBackups(1);
            cacheCfg2.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cacheCfg2.setGroupName("group1");
            cacheCfg2.setDataRegionName("PERS_region");
            IgniteCache<Long, Integer> cache2 = ignite.getOrCreateCache(cacheCfg2);
            for (int j = 0; j < 3; j++)
                for (int i = 0; i < 10; i++)
                    cache2.put((long)i, i*i);

            // cache3
            CacheConfiguration<Long, Integer> cacheCfg3 = new CacheConfiguration<>("cache3");
            cacheCfg3.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheCfg3.setBackups(1);
            cacheCfg3.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cacheCfg3.setGroupName("group1");
            cacheCfg3.setDataRegionName("PERS_region");
            IgniteCache<Long, Integer> cache3 = ignite.getOrCreateCache(cacheCfg3);
            for (int j = 0; j < 3; j++)
                for (int i = 0; i < 20; i++)
                    cache3.put((long)i, i*i);

            // cache4
            CacheConfiguration<Long, Integer> cacheCfg4 = new CacheConfiguration<>("cache4");
            cacheCfg4.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheCfg4.setBackups(1);
            cacheCfg4.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cacheCfg4.setGroupName("group2");
            cacheCfg4.setDataRegionName("IN_MEM_region");
            IgniteCache<Long, Integer> cache4 = ignite.getOrCreateCache(cacheCfg4);
            for (int j = 0; j < 3; j++)
                for (int i = 0; i < 40; i++)
                    cache4.put((long)i, i*i);

            // cache5
            CacheConfiguration<Long, Integer> cacheCfg5 = new CacheConfiguration<>("cache5");
            cacheCfg5.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheCfg5.setBackups(1);
            cacheCfg5.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cacheCfg5.setGroupName("group2");
            cacheCfg5.setDataRegionName("IN_MEM_region");
            IgniteCache<Long, Integer> cache5 = ignite.getOrCreateCache(cacheCfg5);
            for (int j = 0; j < 3; j++)
                for (int i = 0; i < 1080; i++)
                    cache5.put((long)i, i*i);

            // cache6
            CacheConfiguration<Long, Integer> cacheCfg6 = new CacheConfiguration<>("cache6");
            cacheCfg6.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cacheCfg6.setBackups(1);
            cacheCfg6.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            IgniteCache<Long, Integer> cache6 = ignite.getOrCreateCache(cacheCfg6);
            for (int j = 0; j < 4; j++)
                for (int i = 0; i < 60; i++)
                    cache6.put((long)i, i*i);




//            if (UPDATE) {
//                System.out.println("Populating the cache...");
//
//                try (IgniteDataStreamer<Long, Organization> streamer = ignite.dataStreamer(ORG_CACHE)) {
//                    streamer.allowOverwrite(true);
//
//                    for (long i = 0; i < 100_000; i++) {
//                        streamer.addData(i, new Organization(i, "organization-" + i));
//
//                        if (i > 0 && i % 10_000 == 0)
//                            System.out.println("Done: " + i);
//                    }
//                }
//            }

            // Run SQL without explicitly calling to loadCache().
//            QueryCursor<List<?>> cur = cache1.query(
//                new SqlFieldsQuery("select id, name from Organization where name like ?")
//                    .setArgs("organization-54321"));
//
//            System.out.println("SQL Result: " + cur.getAll());

            // Run get() without explicitly calling to loadCache().
            Integer org = cache1.get(54321l);

            Thread.sleep(5000);

            System.out.println("GET Result: " + org);


        }
    }
}
