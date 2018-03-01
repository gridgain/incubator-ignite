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

package org.apache.ignite.client;

import java.util.*;

/**
 * Query entity is a description of {@link CacheClient} entry (composed of key and value) in a way of how it must be
 * indexed and can be queried.
 */
public final class QueryEntity {
    /** Key type. */
    private String keyType;

    /** Value type. */
    private String valType;

    /** Table name. */
    private String tblName;

    /** Key name. Can be used in field list to denote the key as a whole. */
    private String keyFieldName;

    /** Value name. Can be used in field list to denote the entire value. */
    private String valFieldName;

    /** Query fields. */
    private Collection<QueryField> fields = new ArrayList<>();

    /** Name -> Alias map. */
    private Map<String, String> aliases = new HashMap<>();

    /** Query indexes. */
    private Collection<QueryIndex> idxs = new ArrayList<>();

    /**
     * Creates an empty query entity.
     */
    public QueryEntity() {
    }

    /**
     * Creates a query entity with the given key and value types.
     *
     * @param keyType Key type.
     * @param valType Value type.
     */
    public QueryEntity(String keyType, String valType) {
        setKeyType(keyType);
        setValueType(valType);
    }

    /**
     * @return Key type.
     */
    public String getKeyType() {
        return keyType;
    }

    /**
     * @param keyType Key type.
     */
    public QueryEntity setKeyType(String keyType) {
        ensureNotNullOrEmpty(keyType);

        this.keyType = keyType;

        return this;
    }

    /**
     * @return Value type.
     */
    public String getValueType() {
        return valType;
    }

    /**
     * @param valType Value type.
     */
    public QueryEntity setValueType(String valType) {
        ensureNotNullOrEmpty(valType);

        this.valType = valType;

        return this;
    }

    /**
     * @return Fields.
     */
    public Collection<QueryField> getFields() {
        return fields;
    }

    /**
     * @param fields Fields.
     */
    public QueryEntity setFields(Collection<QueryField> fields) {
        if (fields == null)
            throw new NullPointerException("fields");

        this.fields = fields;

        return this;
    }

    /**
     * @return Key field name.
     */
    public String getKeyFieldName() {
        return keyFieldName;
    }

    /**
     * @param keyFieldName Key field name.
     */
    public QueryEntity setKeyFieldName(String keyFieldName) {
        this.keyFieldName = keyFieldName;

        return this;
    }

    /**
     * @return Value field name.
     */
    public String getValueFieldName() {
        return valFieldName;
    }

    /**
     * @param valFieldName Value field name.
     */
    public QueryEntity setValueFieldName(String valFieldName) {
        this.valFieldName = valFieldName;

        return this;
    }

    /**
     * @return Collection of index entities.
     */
    public Collection<QueryIndex> getIndexes() {
        return idxs == null ? Collections.emptyList() : idxs;
    }

    /**
     * @param idxs Collection of index entities.
     */
    public QueryEntity setIndexes(Collection<QueryIndex> idxs) {
        if (idxs == null)
            throw new NullPointerException("idxs");

        this.idxs = idxs;

        return this;
    }

    /**
     * @return Name -> Alias map.
     */
    public Map<String, String> getAliases() {
        return aliases;
    }

    /**
     * Sets mapping from full property name in dot notation to an alias that will be used as SQL column name.
     * Example: {"parent.name" -> "parentName"}.
     *
     * @param aliases Aliases map.
     */
    public QueryEntity setAliases(Map<String, String> aliases) {
        if (aliases == null)
            throw new NullPointerException("aliases");

        this.aliases = aliases;

        return this;
    }

    /**
     * @return Table name.
     */
    public String getTableName() {
        return tblName;
    }

    /**
     * @param tblName Table name.
     */
    public QueryEntity setTableName(String tblName) {
        this.tblName = tblName;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (!(obj instanceof QueryEntity))
            return false;

        QueryEntity other = (QueryEntity)obj;

        return Objects.equals(keyType, other.keyType) &&
            Objects.equals(valType, other.valType) &&
            Objects.equals(keyFieldName, other.keyFieldName) &&
            Objects.equals(valFieldName, other.valFieldName) &&
            Arrays.equals(fields.toArray(), other.fields.toArray()) &&
            Objects.equals(aliases, other.aliases) &&
            idxs.size() == other.idxs.size() && idxs.containsAll(other.idxs) &&
            Objects.equals(tblName, other.tblName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(keyType, valType, keyFieldName, valFieldName, fields, aliases, idxs, tblName);
    }

    /** Used for arguments check. */
    private static void ensureNotNullOrEmpty(String s) {
        if (s == null || s.length() == 0)
            throw new IllegalArgumentException(s);
    }
}
