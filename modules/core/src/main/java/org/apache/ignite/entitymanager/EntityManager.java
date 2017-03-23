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

package org.apache.ignite.entitymanager;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * The <code>EntityManager</code> which support only manual ids assignment.
 */
public class EntityManager<K, V> {
    /** */
    private final int parts;

    /** */
    private ThreadLocal<StringBuilder> builder = new ThreadLocal<StringBuilder>() {
        @Override protected StringBuilder initialValue() {
            return new StringBuilder();
        }
    };

    /** */
    protected final String name;

    /** */
    protected final Map<String, IgniteBiClosure<StringBuilder, Object, String>> incices;

    /** */
    protected Ignite ignite;

    /** */
    private final IdGenerator<K> idGenerator;

    /**
     * @param name Name.
     * @param indices Indices.
     */
    public EntityManager(int partitions, String name,
        Map<String, IgniteBiClosure<StringBuilder, Object, String>> indices,
        IdGenerator<K> idGenerator) {
        this.name = name;
        this.parts = partitions;

        this.incices = indices == null ?
            Collections.<String, IgniteBiClosure<StringBuilder, Object, String>>emptyMap() : indices;

        this.idGenerator = idGenerator;
    }

    /**
     * Returns cache configurations.
     */
    public CacheConfiguration[] cacheConfigurations() {
        CacheConfiguration[] ccfgs = new CacheConfiguration[incices.size() + 1];

        int c = 0;

        ccfgs[c] = new CacheConfiguration(entityCacheName());
        ccfgs[c].setCacheMode(CacheMode.PARTITIONED);
        ccfgs[c].setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfgs[c].setAffinity(new RendezvousAffinityFunction(false, parts));

        c++;

        for (Map.Entry<String, IgniteBiClosure<StringBuilder, Object, String>> idx : incices.entrySet()) {
            String idxName = idx.getKey();
            ccfgs[c] = new CacheConfiguration(indexCacheName(idxName));
            ccfgs[c].setCacheMode(CacheMode.PARTITIONED);
            ccfgs[c].setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            ccfgs[c].setIndexedTypes(IndexFieldKey.class, IndexFieldValue.class);
            ccfgs[c].setAffinity(new RendezvousAffinityFunction(false, parts));
            ccfgs[c].setCopyOnRead(false);

            c++;
        }

        return ccfgs;
    }

    /**
     * @param idxName Index name.
     */
    protected IgniteCache<IndexFieldKey, IndexFieldValue> indexCache(String idxName) {
        return ignite.getOrCreateCache(indexCacheName(idxName));
    }

    /**
     * @param idxName Index name.
     */
    protected String indexCacheName(String idxName) {
        return name + "_" + idxName;
    }

    /**
     * Attaches ignite instance to a manager.
     *
     * @param ignite Ignite.
     */
    public void attach(Ignite ignite) {
        this.ignite = ignite;

        idGenerator.attach(ignite, name);
    }

    /**
     * @param key key;
     * @param val Value.
     * @return List of indexed fields.
     */
    protected IndexChange<K> indexChange(K key, V val) {
        IndexChange<K> idxChange = new IndexChange<>(name, key);

        for (Map.Entry<String, IgniteBiClosure<StringBuilder, Object, String>> idx : incices.entrySet())
            idxChange.addChange(idx.getKey(), idx.getValue().apply(builder(), val));

        return idxChange;
    }

    public V get(K key) {
        return (V)entityCache().get(key);
    }

    public K save(K key, V val) {
        Session ses = Session.current();

        assert ses != null;

        V oldVal = null;

        IndexChange<K> oldChange = null;

        if (key == null)
            key = nextKey(); // New value.
        else {
            oldVal = get(key);

            if (oldVal != null)
                oldChange = indexChange(key, oldVal);
        }

        // Merge indices.
        final IndexChange<K> newChange = indexChange(key, val);

        IgnitePredicate<String> exclSameValsPred = null;

        if (oldChange != null) {
            final IndexChange<K> finalOldChange = oldChange;

            exclSameValsPred = new IgnitePredicate<String>() {
                @Override public boolean apply(String s) {
                    return !finalOldChange.changes().get(s).equals(newChange.changes().get(s));
                }
            };

            // Remove only changed index values.
            removeEntry(ses, key, F.view(oldChange.changes(), exclSameValsPred));
        }

        // Insert only changed index values.
        addEntry(ses, key, exclSameValsPred == null ? newChange.changes() : F.view(newChange.changes(), exclSameValsPred));

        //entityCache().put(key, val);

        return key;
    }

    /** */
    protected K nextKey() {
        return idGenerator.nextId();
    }

    /** */
    public boolean delete(K key) {
        Session ses = Session.current();

        V v = get(key);

        if (v == null)
            return false;

        IndexChange<K> idxChange = indexChange(key, v);

        removeEntry(ses, idxChange.id(), idxChange.changes());

        entityCache().remove(key);

        return true;
    }

    /** */
    protected void addEntry(Session ses, K key, Map<String, String> changes) {
        if (changes == null)
            return;

        for (Map.Entry<String, String> change : changes.entrySet()) {
            IndexFieldKey idxKey = new IndexFieldKey(change.getValue(), key);

            //indexCache(change.getKey()).put(idxKey, IndexFieldValue.MARKER);

            Map<IndexFieldKey, IndexFieldValue> additions = ses.getAdditions(indexCacheName(change.getKey()));

            additions.put(idxKey, IndexFieldValue.MARKER);
        }
    }

    /** */
    protected void removeEntry(Session ses, K key, Map<String, String> changes) {
        if (changes == null)
            return;

        for (Map.Entry<String, String> change : changes.entrySet()) {
            IndexFieldKey idxKey = new IndexFieldKey(change.getValue(), key);

            //indexCache(change.getKey()).remove(idxKey);

            Map<IndexFieldKey, IndexFieldValue> removals = ses.getRemovals(indexCacheName(change.getKey()));

            removals.put(idxKey, null);
        }
    }

    /**
     * @param idxName Field.
     * @param val Value.
     * @param id Id.
     */
    public boolean contains(String idxName, Object val, K id) {
        IgniteBiClosure<StringBuilder, Object, String> clo = incices.get(idxName);

        String strVal = clo.apply(builder(), val);

        IndexFieldKey idxKey = new IndexFieldKey(strVal, id);

        return indexCache(idxName).containsKey(idxKey);
    }

    /**
     * Returns all entities matching the example.
     *
     * @param example Example value.
     * @param idxName Index name.
     * @return Entities iterator.
     */
    @SuppressWarnings("unchecked")
    public Collection<T2<K, V>> findAll(V example, String idxName) {
        IgniteBiClosure<StringBuilder, Object, String> clo = incices.get(idxName);

        String strVal = clo.apply(builder(), example);

        SqlQuery<IndexFieldKey, IndexFieldValue> sqlQry = new SqlQuery<>(IndexFieldValue.class, "fieldValue = ?");

        sqlQry.setArgs(strVal);

        // TODO set partition when IGNITE-4523 will be ready.

        QueryCursor<Cache.Entry<IndexFieldKey, IndexFieldValue>> cur = indexCache(idxName).query(sqlQry);

        List<Cache.Entry<IndexFieldKey, IndexFieldValue>> rows = U.arrayList(cur, 16);

        return F.viewReadOnly(rows, new IgniteClosure<Cache.Entry<IndexFieldKey, IndexFieldValue>, T2<K, V>>() {
            @Override public T2<K, V> apply(Cache.Entry<IndexFieldKey, IndexFieldValue> e) {
                Object entryKey = e.getKey().getPayload();

                return new T2(entryKey, entityCache().get((K)entryKey)); // TODO use batching
            }
        });
    }

    /** */
    protected IgniteCache<K, V> entityCache() {
        return ignite.getOrCreateCache(entityCacheName());
    }

    /** */
    protected String entityCacheName() {
        return name + "_entity";
    }

    /** */
    protected StringBuilder builder() {
        StringBuilder builder = this.builder.get();

        builder.setLength(0);

        return builder;
    }

    /**
     * @param field Field.
     */
    public int indexSize(String field) {
        return indexCache(field).size();
    }
}