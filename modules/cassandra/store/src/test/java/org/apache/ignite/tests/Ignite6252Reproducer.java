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

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.tests.pojos.Person;
import org.apache.ignite.tests.utils.CassandraHelper;
import org.apache.log4j.Logger;
import org.junit.Test;

/** */
public class Ignite6252Reproducer {
    /** Logger. */
    private static final Logger logger = Logger.getLogger(Ignite6252Reproducer.class.getName());

    /** */
    @Test
    public void test() throws InterruptedException {
        CassandraHelper.startEmbeddedCassandra(logger);
        CassandraHelper.dropTestKeyspaces();

        try (Ignite ignite = Ignition.start("org/apache/ignite/tests/persistence/pojo/ignite-config.xml")) {
            final IgniteCache<Long, Person> personCache = ignite.getOrCreateCache(new CacheConfiguration<>("cache1"));

            while (true) {
                personCache.put(1L, new Person(
                    1L,
                    "John",
                    "Smith",
                    (short)30,
                    true,
                    200,
                    100,
                    new Date(),
                    Collections.singletonList("1234-56-78")
                ));
            }
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

//    /** */
//    private static class Person {
//        /** Id. */
//        private final String id;
//
//        /** Name. */
//        private final String name;
//
//        /** */
//        Person(String id, String name) {
//            this.id = id;
//            this.name = name;
//        }
//
//        /** */
//        public String getId() {
//            return id;
//        }
//
//        /** */
//        public String getName() {
//            return name;
//        }
//    }
}
