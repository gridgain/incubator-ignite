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

package org.apache.ignite.client.system;

import org.apache.ignite.*;
import org.apache.ignite.binary.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.client.*;
import org.apache.ignite.client.CacheKeyConfiguration;
import org.apache.ignite.client.QueryEntity;
import org.apache.ignite.client.QueryIndex;
import org.apache.ignite.configuration.*;
import org.junit.*;

import javax.cache.*;
import java.util.*;
import java.util.stream.*;

import static org.junit.Assert.*;

/**
 * Thin client functional tests.
 */
public class FunctionalTest {
    /**
     * Tested API:
     * <ul>
     * <li>{@link IgniteClient#cache(String)}</li>
     * <li>{@link IgniteClient#getOrCreateCache(CacheClientConfiguration)}</li>
     * <li>{@link IgniteClient#cacheNames()}</li>
     * <li>{@link IgniteClient#createCache(String)}</li>
     * <li>{@link IgniteClient#createCache(CacheClientConfiguration)}</li>
     * <li>{@link IgniteCache#size(CachePeekMode...)}</li>
     * </ul>
     */
    @Test
    public void testCacheManagement() throws Exception {
        try (LocalIgniteCluster ignored = LocalIgniteCluster.start(2);
             IgniteClient client = IgniteClient.start(getClientConfiguration())
        ) {
            final String CACHE_NAME = "testCacheManagement";

            CacheClientConfiguration cacheCfg = new CacheClientConfiguration(CACHE_NAME)
                .setCacheMode(CacheMode.REPLICATED)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

            int key = 1;
            Person val = new Person(key, Integer.toString(key));

            CacheClient<Integer, Person> cache = client.getOrCreateCache(cacheCfg);

            cache.put(key, val);

            assertEquals(1, cache.size());
            assertEquals(2, cache.size(CachePeekMode.ALL));

            cache = client.cache(CACHE_NAME);

            Person cachedVal = cache.get(key);

            assertEquals(val, cachedVal);

            Object[] cacheNames = new TreeSet<>(client.cacheNames()).toArray();

            assertArrayEquals(new TreeSet<>(Arrays.asList(Config.DEFAULT_CACHE_NAME, CACHE_NAME)).toArray(), cacheNames);

            client.destroyCache(CACHE_NAME);

            cacheNames = client.cacheNames().toArray();

            assertArrayEquals(new Object[] {Config.DEFAULT_CACHE_NAME}, cacheNames);

            cache = client.createCache(CACHE_NAME);

            assertFalse(cache.containsKey(key));

            cacheNames = client.cacheNames().toArray();

            assertArrayEquals(new TreeSet<>(Arrays.asList(Config.DEFAULT_CACHE_NAME, CACHE_NAME)).toArray(), cacheNames);

            client.destroyCache(CACHE_NAME);

            cache = client.createCache(cacheCfg);

            assertFalse(cache.containsKey(key));

            assertArrayEquals(new TreeSet<>(Arrays.asList(Config.DEFAULT_CACHE_NAME, CACHE_NAME)).toArray(), cacheNames);
        }
    }

    /**
     * Tested API:
     * <ul>
     * <li>{@link CacheClient#getName()}</li>
     * <li>{@link CacheClient#getConfiguration()}</li>
     * </ul>
     */
    @Test
    public void testCacheConfiguration() throws Exception {
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = IgniteClient.start(getClientConfiguration())
        ) {
            final String CACHE_NAME = "testCacheConfiguration";

            CacheClientConfiguration cacheCfg = new CacheClientConfiguration(CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(3)
                .setCacheMode(CacheMode.PARTITIONED)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setEagerTtl(false)
                .setGroupName("FunctionalTest")
                .setDefaultLockTimeout(12345)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_ALL)
                .setReadFromBackup(true)
                .setRebalanceBatchSize(67890)
                .setRebalanceBatchesPrefetchCount(102938)
                .setRebalanceDelay(54321)
                .setRebalanceMode(CacheRebalanceMode.SYNC)
                .setRebalanceOrder(2)
                .setRebalanceThrottle(564738)
                .setRebalanceTimeout(142536)
                .setKeyConfiguration(Collections.singletonList(new CacheKeyConfiguration("Employee", "orgId")))
                .setQueryEntities(Collections.singletonList(
                    new QueryEntity(int.class.getName(), "Employee")
                        .setTableName("EMPLOYEE")
                        .setFields(Arrays.asList(
                            new QueryField("id", Integer.class.getName()),
                            new QueryField("orgId", Integer.class.getName())
                        ))
                        .setIndexes(Collections.singletonList(new QueryIndex("id", true, "IDX_EMPLOYEE_ID")))
                        .setAliases(Stream.of("id", "orgId").collect(Collectors.toMap(f -> f, String::toUpperCase)))
                ));

            CacheClient cache = client.createCache(cacheCfg);

            assertEquals(CACHE_NAME, cache.getName());

            assertEquals(cacheCfg, cache.getConfiguration());
        }
    }

    /**
     * Tested API:
     * <ul>
     * <li>{@link IgniteClient#start(IgniteClientConfiguration)}</li>
     * <li>{@link IgniteClient#getOrCreateCache(String)}</li>
     * <li>{@link CacheClient#put(Object, Object)}</li>
     * <li>{@link CacheClient#get(Object)}</li>
     * <li>{@link CacheClient#containsKey(Object)}</li>
     * </ul>
     */
    @Test
    public void testPutGet() throws Exception {
        // Existing cache, primitive key and object value
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = IgniteClient.start(getClientConfiguration())
        ) {
            CacheClient<Integer, Person> cache = client.getOrCreateCache(Config.DEFAULT_CACHE_NAME);

            Integer key = 1;
            Person val = new Person(key, "Joe");

            cache.put(key, val);

            assertTrue(cache.containsKey(key));

            Person cachedVal = cache.get(key);

            assertEquals(val, cachedVal);
        }

        // Non-existing cache, object key and primitive value
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = IgniteClient.start(getClientConfiguration())
        ) {
            CacheClient<Person, Integer> cache = client.getOrCreateCache("testPutGet");

            Integer val = 1;

            Person key = new Person(val, "Joe");

            cache.put(key, val);

            Integer cachedVal = cache.get(key);

            assertEquals(val, cachedVal);
        }

        // Object key and Object value
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = IgniteClient.start(getClientConfiguration())
        ) {
            CacheClient<Person, Person> cache = client.getOrCreateCache("testPutGet");

            Person key = new Person(1, "Joe Key");

            Person val = new Person(1, "Joe Value");

            cache.put(key, val);

            Person cachedVal = cache.get(key);

            assertEquals(val, cachedVal);
        }
    }

    /**
     * Tested API:
     * <ul>
     * <li>{@link CacheClient#putAll(Map)}</li>
     * <li>{@link CacheClient#getAll(Set)}</li>
     * <li>{@link CacheClient#clear()}</li>
     * </ul>
     */
    @Test
    public void testBatchPutGet() throws Exception {
        // Existing cache, primitive key and object value
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = IgniteClient.start(getClientConfiguration())
        ) {
            CacheClient<Integer, Person> cache = client.cache(Config.DEFAULT_CACHE_NAME);

            Map<Integer, Person> data = IntStream
                .rangeClosed(1, 1000).boxed()
                .collect(Collectors.toMap(i -> i, i -> new Person(i, String.format("Person %s", i))));

            cache.putAll(data);

            Map<Integer, Person> cachedData = cache.getAll(data.keySet());

            assertEquals(data, cachedData);
        }

        // Non-existing cache, object key and primitive value
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = IgniteClient.start(getClientConfiguration())
        ) {
            CacheClient<Person, Integer> cache = client.createCache("testBatchPutGet");

            Map<Person, Integer> data = IntStream
                .rangeClosed(1, 1000).boxed()
                .collect(Collectors.toMap(i -> new Person(i, String.format("Person %s", i)), i -> i));

            cache.putAll(data);

            Map<Person, Integer> cachedData = cache.getAll(data.keySet());

            assertEquals(data, cachedData);

            cache.clear();

            assertEquals(0, cache.size(CachePeekMode.ALL));
        }
    }

    /**
     * Tested API:
     * <ul>
     * <li>{@link CacheClient#getAndPut(Object, Object)}</li>
     * <li>{@link CacheClient#getAndRemove(Object)}</li>
     * <li>{@link CacheClient#getAndReplace(Object, Object)}</li>
     * <li>{@link CacheClient#putIfAbsent(Object, Object)}</li>
     * </ul>
     */
    @Test
    public void testAtomicPutGet() throws Exception {
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = IgniteClient.start(getClientConfiguration())
        ) {
            CacheClient<Integer, String> cache = client.createCache("testRemoveReplace");

            assertNull(cache.getAndPut(1, "1"));
            assertEquals("1", cache.getAndPut(1, "1.1"));

            assertEquals("1.1", cache.getAndRemove(1));
            assertNull(cache.getAndRemove(1));

            assertTrue(cache.putIfAbsent(1, "1"));
            assertFalse(cache.putIfAbsent(1, "1.1"));

            assertEquals("1", cache.getAndReplace(1, "1.1"));
            assertEquals("1.1", cache.getAndReplace(1, "1"));
            assertNull(cache.getAndReplace(2, "2"));
        }
    }

    /**
     * Tested API:
     * <ul>
     * <li>{@link CacheClient#replace(Object, Object)}</li>
     * <li>{@link CacheClient#replace(Object, Object, Object)}</li>
     * <li>{@link CacheClient#remove(Object)}</li>
     * <li>{@link CacheClient#remove(Object, Object)}</li>
     * <li>{@link CacheClient#removeAll()}</li>
     * <li>{@link CacheClient#removeAll(Set)}</li>
     * </ul>
     */
    @Test
    public void testRemoveReplace() throws Exception {
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = IgniteClient.start(getClientConfiguration())
        ) {
            CacheClient<Integer, String> cache = client.createCache("testRemoveReplace");

            Map<Integer, String> data = IntStream.rangeClosed(1, 100).boxed()
                .collect(Collectors.toMap(i -> i, Object::toString));

            cache.putAll(data);

            assertFalse(cache.replace(1, "2", "3"));
            assertEquals("1", cache.get(1));
            assertTrue(cache.replace(1, "1", "3"));
            assertEquals("3", cache.get(1));

            assertFalse(cache.replace(101, "101"));
            assertNull(cache.get(101));
            assertTrue(cache.replace(100, "101"));
            assertEquals("101", cache.get(100));

            assertFalse(cache.remove(101));
            assertTrue(cache.remove(100));
            assertNull(cache.get(100));

            assertFalse(cache.remove(99, "100"));
            assertEquals("99", cache.get(99));
            assertTrue(cache.remove(99, "99"));
            assertNull(cache.get(99));

            cache.put(101, "101");

            cache.removeAll(data.keySet());
            assertEquals(1, cache.size());
            assertEquals("101", cache.get(101));

            cache.removeAll();
            assertEquals(0, cache.size());
        }
    }

    /**
     * Tested API:
     * <ul>
     * <li>{@link CacheClient#query(Query)}</li>
     * </ul>
     */
    @Test
    public void testQueries() throws Exception {
        IgniteConfiguration srvCfg = Config.getServerConfiguration();

        // No peer class loading from thin clients: we need the server to know about this class to deserialize
        // ScanQuery filter.
        BinaryConfiguration binCfg = new BinaryConfiguration();

        BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration(getClass().getName());

        binCfg.setTypeConfigurations(Collections.singletonList(typeCfg));

        srvCfg.setBinaryConfiguration(binCfg);

        try (Ignite ignored = Ignition.start(srvCfg);
             IgniteClient client = IgniteClient.start(getClientConfiguration())
        ) {
            CacheClient<Integer, Person> cache = client.getOrCreateCache(Config.DEFAULT_CACHE_NAME);

            Map<Integer, Person> data = IntStream.rangeClosed(1, 100).boxed()
                .collect(Collectors.toMap(i -> i, i -> new Person(i, String.format("Person %s", i))));

            cache.putAll(data);

            int minId = data.size() / 2 + 1;
            int pageSize = (data.size() - minId) / 3;
            int expSize = data.size() - minId + 1; // expected query result size

            // Expected result
            Map<Integer, Person> exp = data.entrySet().stream()
                .filter(e -> e.getKey() >= minId)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            // Scan and SQL queries
            Collection<Query<Cache.Entry<Integer, Person>>> queries = Arrays.asList(
                new ScanQuery<Integer, Person>((i, p) -> p.getId() >= minId).setPageSize(pageSize),
                new SqlQuery<Integer, Person>(Person.class, "id >= ?").setArgs(minId).setPageSize(pageSize)
            );

            for (Query<Cache.Entry<Integer, Person>> qry : queries) {
                try (QueryCursor<Cache.Entry<Integer, Person>> cur = cache.query(qry)) {
                    List<Cache.Entry<Integer, Person>> res = cur.getAll();

                    assertEquals(
                        String.format("Unexpected number of rows from %s", qry.getClass().getSimpleName()),
                        expSize, res.size()
                    );

                    Map<Integer, Person> act = res.stream()
                        .collect(Collectors.toMap(Cache.Entry::getKey, Cache.Entry::getValue));

                    assertEquals(String.format("unexpected rows from %s", qry.getClass().getSimpleName()), exp, act);
                }
            }

            // Fields query
            SqlFieldsQuery qry = new SqlFieldsQuery("select id, name from Person where id >= ?")
                .setArgs(minId)
                .setPageSize(pageSize);

            try (QueryCursor<List<?>> cur = cache.query(qry)) {
                List<List<?>> res = cur.getAll();

                assertEquals(expSize, res.size());

                Map<Integer, Person> act = res.stream().collect(Collectors.toMap(
                    r -> Integer.parseInt(r.get(0).toString()),
                    r -> new Person(Integer.parseInt(r.get(0).toString()), r.get(1).toString())
                ));

                assertEquals(exp, act);
            }
        }
    }

    /**
     * Tested API:
     * <ul>
     * <li>{@link IgniteClient#query(SqlFieldsQuery)}</li>
     * <li>{@link IgniteClient#nonQuery(SqlFieldsQuery)}</li>
     * </ul>
     */
    @Test
    public void testSql() throws Exception {
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = IgniteClient.start(new IgniteClientConfiguration(Config.HOST))
        ) {
            client.nonQuery(
                new SqlFieldsQuery(String.format(
                    "CREATE TABLE IF NOT EXISTS Person (id INT PRIMARY KEY, name VARCHAR) WITH \"VALUE_TYPE=%s\"",
                    Person.class.getName()
                )).setSchema("PUBLIC")
            );

            int key = 1;
            Person val = new Person(key, "Person 1");

            client.nonQuery(new SqlFieldsQuery(
                "INSERT INTO Person(id, name) VALUES(?, ?)"
            ).setArgs(val.getId(), val.getName()).setSchema("PUBLIC"));

            Object cachedName = client.query(
                new SqlFieldsQuery("SELECT name from Person WHERE id=?").setArgs(key).setSchema("PUBLIC")
            ).getAll().iterator().next().iterator().next();

            assertEquals(val.getName(), cachedName);
        }
    }

    /** */
    private static IgniteClientConfiguration getClientConfiguration() {
        return new IgniteClientConfiguration(Config.HOST);
    }
}
