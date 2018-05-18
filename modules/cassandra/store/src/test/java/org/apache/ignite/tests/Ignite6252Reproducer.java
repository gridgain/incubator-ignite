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

package org.apache.ignite.tests;

import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.store.cassandra.CassandraCacheStoreFactory;
import org.apache.ignite.cache.store.cassandra.datasource.DataSource;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.tests.utils.CassandraAdminCredentials;
import org.apache.ignite.tests.utils.CassandraHelper;
import org.apache.log4j.Logger;
import org.junit.Test;

/** */
public class Ignite6252Reproducer {
    /** Logger. */
    private static final Logger logger = Logger.getLogger(Ignite6252Reproducer.class.getName());

    /** Cache name. */
    private static final String CACHE_NAME = "cache1";

    /** */
    @Test
    public void test() {
        CassandraHelper.startEmbeddedCassandra(logger);
        CassandraHelper.dropTestKeyspaces();

        try (final Ignite ignite = Ignition.start(createIgniteConfig())) {
            Cache<Long, String> cache = ignite.cache(CACHE_NAME);

            final int BATCH_SIZE = 100;
            final String smallVal = "foo";
            final String largeVal =
                String.join(" ", IntStream.range(0, 10_000).boxed().map(unused -> "foo").toArray(String[]::new));

            IntStream.range(0, 20 /* threads */).parallel().forEach(t ->
                LongStream.range(t, t + 1_000_000).forEach(b ->
                    cache.putAll(
                        LongStream.range(0, BATCH_SIZE).boxed().map(i -> {
                            long key = b * BATCH_SIZE + i;
                            String val = key % 2 == 0 ? smallVal : largeVal;

                            return new SimpleEntry<>(key, val);
                        }).collect(
                            Collectors.toMap(SimpleEntry<Long, String>::getKey, SimpleEntry<Long, String>::getValue)
                        )
                    )
                )
            );

            Thread.sleep(30_000);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            try {
                CassandraHelper.dropTestKeyspaces();
            }
            finally {
                CassandraHelper.releaseCassandraResources();

                try {
                    CassandraHelper.stopEmbeddedCassandra();
                }
                catch (Throwable e) {
                    logger.error("Failed to stop embedded Cassandra instance", e);
                }
            }
        }
    }

    /** */
    private IgniteConfiguration createIgniteConfig() {
        DataSource dataSrc = new DataSource();
        dataSrc.setCredentials(new CassandraAdminCredentials());
        dataSrc.setContactPoints(CassandraHelper.getContactPointsArray());
        dataSrc.setReadConsistency("ONE");
        dataSrc.setWriteConsistency("ONE");
        dataSrc.setLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));

        String persistenceSettings = "<persistence keyspace=\"test1\" table=\"test1\">" +
            "<keyPersistence class=\"java.lang.Long\" strategy=\"PRIMITIVE\"/>" +
            "<valuePersistence class=\"java.lang.String\" strategy=\"BLOB\"/>" +
            "</persistence>";

        return new IgniteConfiguration()
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(
                new TcpDiscoveryVmIpFinder().setAddresses(Collections.singletonList("127.0.0.1:47500"))
            ))
            .setCacheConfiguration(
                new CacheConfiguration<Long, String>(CACHE_NAME)
                    .setReadThrough(true)
                    .setWriteThrough(true)
                    .setCacheStoreFactory(
                        new CassandraCacheStoreFactory<>()
                            .setDataSource(dataSrc)
                            .setPersistenceSettings(new KeyValuePersistenceSettings(persistenceSettings))
                    )
            );
    }
}
