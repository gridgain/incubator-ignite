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

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.cache.Cache.Entry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.CacheInterceptorDeserializeAdapter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

public abstract class CacheInterceptorClientsAbstractTest extends GridCommonAbstractTest {

    private static final int POPULATE_DATA_SIZE = 100;

    private static final String SERVER_NODE_NAME = "server-node";

    private static final String ATOMIC_CACHE_NAME_REPLICATED = "atomic-cache-rep";

    private static final String ATOMIC_CACHE_NAME_PARTITIONED = "atomic-cache-part";

    private static final String ATOMIC_CACHE_NAME_LOCAL = "atomic-cache-loc";

    private static final String TX_CACHE_NAME_REPLICATED = "tx-cache-rep";

    private static final String TX_CACHE_NAME_PARTITIONED = "tx-cache-part";

    private static final String TX_CACHE_NAME_LOCAL = "tx-cache-loc";

    private static final Collection<String> CACHE_NAMES;

    static {
        Set<String> set = new HashSet<>();

        set.add(ATOMIC_CACHE_NAME_REPLICATED);
        set.add(ATOMIC_CACHE_NAME_PARTITIONED);
        set.add(ATOMIC_CACHE_NAME_LOCAL);

        set.add(TX_CACHE_NAME_REPLICATED);
        set.add(TX_CACHE_NAME_PARTITIONED);
        set.add(TX_CACHE_NAME_LOCAL);

        CACHE_NAMES = Collections.unmodifiableCollection(set);
    }

    private final boolean binary;

    private Ignite fatClient;

    private IgniteClient thinClient;

    protected CacheInterceptorClientsAbstractTest(boolean binary) {
        this.binary = binary;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureHandler(new StopNodeOrHaltFailureHandler());

        cfg.setDataStorageConfiguration(new DataStorageConfiguration());

        cfg.setCacheConfiguration(
            createCacheCfg(ATOMIC_CACHE_NAME_REPLICATED, REPLICATED, ATOMIC),
            createCacheCfg(ATOMIC_CACHE_NAME_PARTITIONED, PARTITIONED, ATOMIC),
            createCacheCfg(ATOMIC_CACHE_NAME_LOCAL, LOCAL, ATOMIC),

            createCacheCfg(TX_CACHE_NAME_REPLICATED, REPLICATED, TRANSACTIONAL),
            createCacheCfg(TX_CACHE_NAME_PARTITIONED, PARTITIONED, TRANSACTIONAL),
            createCacheCfg(TX_CACHE_NAME_LOCAL, LOCAL, TRANSACTIONAL)
        );

        return cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IgniteConfiguration thickClientCfg = getConfiguration("thick-client").setClientMode(true);

        Ignite ignite = startGrid(SERVER_NODE_NAME);

        fatClient = startGrid(thickClientCfg);

        ignite.cluster().active(true);

        ClientConnectorConfiguration ccfg = grid(SERVER_NODE_NAME).configuration().getClientConnectorConfiguration();

        ClientConfiguration thinClientCfg = new ClientConfiguration().setAddresses("127.0.0.1:" + ccfg.getPort());

        thinClient = Ignition.startClient(thinClientCfg);
    }

    private CacheConfiguration createCacheCfg(String name, CacheMode cMode, CacheAtomicityMode aMode) {
        CacheConfiguration cfg = new CacheConfiguration().setName(name).setCacheMode(cMode).setAtomicityMode(aMode);

        return cfg.setInterceptor(binary ? new NoopBinaryInterceptor() : new NoopDeserializedInterceptor());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        populateData();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        CACHE_NAMES.forEach(c -> grid(SERVER_NODE_NAME).cache(c).clear());
    }

    @Test
    public void testGet() {
        Object k = convertToBinaryIfNeeded(new Key(0));

        for(String cacheName : CACHE_NAMES) {
            Object vs = serverCache(cacheName).get(k);
            Object vfc = fatClientCache(cacheName).get(k);
            Object vtc = thinClientCache(cacheName).get(k);

            assertEquals(cacheName, vs, vfc);
            assertEquals(cacheName, vs, vtc);
        }
    }

    private void populateData() {
        for (String cacheName : CACHE_NAMES) {
            for (int i = 0; i < POPULATE_DATA_SIZE; i++) {
                Object k = convertToBinaryIfNeeded(new Key(i));
                Object v = convertToBinaryIfNeeded(new Value(cacheName + i));

                serverCache(cacheName).put(k, v);
            }
        }
    }

    private Object convertToBinaryIfNeeded(Object o) {
        return binary ? grid(SERVER_NODE_NAME).context().cacheObjects().binary().toBinary(o) : o;
    }

    private ClientCache thinClientCache(String cacheName) {
        if (binary)
            return thinClient.cache(cacheName).withKeepBinary();
        else
            return thinClient.cache(cacheName);
    }

    private IgniteCache fatClientCache(String cacheName) {
        if (binary)
            return fatClient.cache(cacheName).withKeepBinary();
        else
            return fatClient.cache(cacheName);
    }

    private IgniteCache serverCache(String cacheName) {
        if (binary)
            return grid(SERVER_NODE_NAME).cache(cacheName).withKeepBinary();
        else
            return grid(SERVER_NODE_NAME).cache(cacheName);
    }

    private static class Key {
        private final int id;

        public Key(int id) {
            this.id = id;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Key key = (Key)o;
            return id == key.id;
        }

        @Override public int hashCode() {
            return Objects.hash(id);
        }

        @Override public String toString() {
            return id + "";
        }
    }

    private static class Value {
        private final String v;

        public Value(String v) {
            this.v = v;
        }

        public String v() {
            return v;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Value value = (Value)o;

            return Objects.equals(v, value.v);
        }

        @Override public int hashCode() {
            return Objects.hash(v);
        }

        @Override public String toString() {
            return v;
        }
    }

    private static class NoopDeserializedInterceptor extends CacheInterceptorDeserializeAdapter<Key, Value> {
        /** {@inheritDoc} */
        @Nullable @Override public Value onGet(Key key, Value val) {
            return super.onGet(key, val);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Value onBeforePut(Entry<Key, Value> entry, Value newVal) {
            return super.onBeforePut(entry, newVal);
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Entry<Key, Value> entry) {
            super.onAfterPut(entry);
        }

        /** {@inheritDoc} */
        @Override public @Nullable IgniteBiTuple<Boolean, Value> onBeforeRemove(Entry<Key, Value> entry) {
            return super.onBeforeRemove(entry);
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Entry<Key, Value> entry) {
            super.onAfterRemove(entry);
        }
    }

    private static class NoopBinaryInterceptor extends CacheInterceptorAdapter<BinaryObject, BinaryObject> {
        /** {@inheritDoc} */
        @Nullable @Override public BinaryObject onGet(BinaryObject key, BinaryObject val) {
            checkKey(key);

            checkValue(val);

            return super.onGet(key, val);
        }

        /** {@inheritDoc} */
        @Nullable @Override
        public BinaryObject onBeforePut(Entry<BinaryObject, BinaryObject> entry, BinaryObject newVal) {
            checkKey(entry.getKey());

            checkValue(entry.getValue());

            checkValue(newVal);

            return super.onBeforePut(entry, newVal);
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Entry<BinaryObject, BinaryObject> entry) {
            checkKey(entry.getKey());

            checkValue(entry.getValue());

            super.onAfterPut(entry);
        }

        /** {@inheritDoc} */
        @Override
        public @Nullable IgniteBiTuple<Boolean, BinaryObject> onBeforeRemove(Entry<BinaryObject, BinaryObject> entry) {
            checkKey(entry.getKey());

            checkValue(entry.getValue());

            return super.onBeforeRemove(entry);
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Entry<BinaryObject, BinaryObject> entry) {
            checkKey(entry.getKey());

            checkValue(entry.getValue());

            super.onAfterRemove(entry);
        }

        private void checkKey(BinaryObject key) {
            if (key != null) {
                Object deserKey = key.deserialize();

                assertTrue(deserKey.getClass().getName(), deserKey instanceof Key);
            }
        }

        private void checkValue(BinaryObject obj) {
            if (obj != null) {
                Object deserVal = obj.deserialize();

                assertTrue(deserVal.getClass().getName(), deserVal instanceof Value);
            }
        }
    }
}
