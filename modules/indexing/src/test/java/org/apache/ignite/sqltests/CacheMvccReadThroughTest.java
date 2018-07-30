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

package org.apache.ignite.sqltests;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.sqltests.CacheMvccReadThroughTest.TestCacheLoader.readThroughBuddy;

public class CacheMvccReadThroughTest extends CacheMvccAbstractTest {
    @Override
    protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    private IgniteCache<Integer, Buddy> buddyCache;

    @Override
    protected void beforeTest() throws Exception {
        IgniteEx ignite = startGrid(0);
        buddyCache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Buddy>("buddy0")
            .setTypes(Integer.class, Buddy.class)
            .setIndexedTypes(Integer.class, Buddy.class)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheLoaderFactory((Factory<CacheLoader<Integer, Buddy>>)TestCacheLoader::new)
            .setReadThrough(true));
    }

    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    public void testReadsThroughIfValueAbsent() throws Exception {
        Buddy buddy = buddyCache.get(1);

        assertEquals(readThroughBuddy(1), buddy);
    }

    public void testDoesNotReadThroughIfValuePreset() throws Exception {
        Buddy buddy = new Buddy(1, "ivan");
        buddyCache.put(1, buddy);

        assertEquals(buddy, buddyCache.get(1));
    }

    public void testReadThroughAllIfPartiallyPresent() throws Exception {
        Buddy buddy = new Buddy(2, "ivan");
        buddyCache.put(2, buddy);

        Map<Integer, Buddy> cached = buddyCache.getAll(new HashSet<>(Arrays.asList(1, 2, 3)));

        assertEquals(readThroughBuddy(1), cached.get(1));
        assertEquals(buddy, cached.get(2));
        assertEquals(readThroughBuddy(3), cached.get(3));

    }

    public void testReadThroughConcurrent() throws Exception {
        int threadCount = 2;
        CyclicBarrier sync = new CyclicBarrier(threadCount);
        for (int i = 0; i < 100; i++) {
            Integer key = i;
            Buddy putBuddy = new Buddy(key, "ivan");
            IgniteInternalFuture<Object> putF = GridTestUtils.runAsync(() -> {
                sync.await();
                buddyCache.put(key, putBuddy);
                return null;
            });
            IgniteInternalFuture<Object> getF = GridTestUtils.runAsync(() -> {
                sync.await();
                buddyCache.get(key);
                return null;
            });
            putF.get();
            getF.get();

            assertEquals(putBuddy, buddyCache.get(key));
        }
    }

    public void testTransactionAndReadThrough() throws Exception {
        buddyCache.query(q("begin"));

        FieldsQueryCursor<List<?>> buddies1 = buddyCache.query(q("select * from Buddy"));
        assertEquals(0, nRows(buddies1));

        CompletableFuture.runAsync(() -> {
            buddyCache.get(1);
        }).join();

        // currently non-repeatable read in SQL is possible
        FieldsQueryCursor<List<?>> buddies2 = buddyCache.query(q("select * from Buddy"));
        assertEquals(1, nRows(buddies2));

        buddyCache.query(q("commit"));
    }

    private static long nRows(FieldsQueryCursor<?> cursor) {
        return StreamSupport.stream(cursor.spliterator(), false).count();
    }

    public static class Buddy implements Serializable {
        @QuerySqlField(index = true)
        private Integer id;
        @QuerySqlField
        private String name;

        public Buddy(Integer id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public String toString() {
            return id + " " + name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Buddy buddy = (Buddy)o;
            return Objects.equals(id, buddy.id) &&
                Objects.equals(name, buddy.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name);
        }
    }

    public static class TestCacheLoader implements CacheLoader<Integer, Buddy> {
        @Override
        public Buddy load(Integer key) throws CacheLoaderException {
            return loadAll(Collections.singleton(key)).get(key);
        }

        @Override
        public Map<Integer, Buddy> loadAll(Iterable<? extends Integer> keys) throws CacheLoaderException {
            return StreamSupport.stream(keys.spliterator(), false)
                .collect(Collectors.toMap(Function.identity(), TestCacheLoader::readThroughBuddy));
        }

        static Buddy readThroughBuddy(Integer k) {
            return new Buddy(k, "name" + k);
        }
    }

    private static SqlFieldsQuery q(String sql) {
        return new SqlFieldsQuery(sql);
    }
}
