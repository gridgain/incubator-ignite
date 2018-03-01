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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import org.apache.ignite.cache.QueryIndexType;

/**
 * Contains list of fields to be indexed. It is possible to provide field name
 * suffixed with index specific extension, for example for {@link QueryIndexType#SORTED sorted} index
 * the list can be provided as following {@code (id, name asc, age desc)}.
 */
public final class QueryIndex {
    /** Name. */
    private String name;

    /** Fields. */
    private Collection<QueryIndexField> fields = new ArrayList<>();

    /** Type. */
    private QueryIndexType type = QueryIndexType.SORTED;

    /** Inline size. */
    private int inlineSize = -1;

    /**
     * Creates single-field sorted ascending index.
     *
     * @param field Field name.
     */
    public QueryIndex(String field) {
        this(field, QueryIndexType.SORTED, true);
    }

    /**
     * Creates single-field sorted index.
     *
     * @param field Field name.
     * @param asc Ascending flag.
     */
    public QueryIndex(String field, boolean asc) {
        this(field, QueryIndexType.SORTED, asc);
    }

    /**
     * Creates single-field sorted index.
     *
     * @param field Field name.
     * @param asc Ascending flag.
     * @param name Index name.
     */
    public QueryIndex(String field, boolean asc, String name) {
        this(field, QueryIndexType.SORTED, asc);

        this.name = name;
    }

    /**
     * Creates index for one field.
     * If index is sorted, then ascending sorting is used by default.
     * To specify sort order, use the next method.
     *
     * @param field Field name.
     * @param type Index type.
     */
    public QueryIndex(String field, QueryIndexType type) {
        this(Collections.singletonList(new QueryIndexField(field)), type);
    }

    /**
     * Creates index for one field. The last boolean parameter is ignored for non-sorted indexes.
     *
     * @param field Field name.
     * @param type Index type.
     * @param asc Ascending flag.
     */
    public QueryIndex(String field, QueryIndexType type, boolean asc) {
        if (field == null || field.length() == 0)
            throw new IllegalArgumentException("field");

        fields = new ArrayList<>();
        fields.add(new QueryIndexField(field, asc));

        this.type = type;
    }

    /**
     * Creates index for one field. The last boolean parameter is ignored for non-sorted indexes.
     *
     * @param field Field name.
     * @param type Index type.
     * @param asc Ascending flag.
     * @param name Index name.
     */
    public QueryIndex(String field, QueryIndexType type, boolean asc, String name) {
        if (field == null || field.length() == 0)
            throw new IllegalArgumentException("field");

        fields = new ArrayList<>();
        fields.add(new QueryIndexField(field, asc));

        this.type = type;
        this.name = name;
    }

    /**
     * Creates index for a collection of fields. The order of fields in the created index will be the same
     * as iteration order in the passed map. Map value defines whether the index will be ascending.
     *
     * @param fields Field name to field sort direction for sorted indexes.
     * @param type Index type.
     */
    public QueryIndex(Collection<QueryIndexField> fields, QueryIndexType type) {
        if (fields == null)
            throw new NullPointerException("fields");

        this.fields = fields;
        this.type = type;
    }

    /**
     * @return Index name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name Index name.
     */
    public QueryIndex setName(String name) {
        this.name = name;

        return this;
    }

    /**
     * @return Collection of index fields.
     */
    public Collection<QueryIndexField> getFields() {
        return fields;
    }

    /**
     * @param fields Collection of index fields.
     */
    public QueryIndex setFields(Collection<QueryIndexField> fields) {
        if (fields == null)
            throw new NullPointerException("fields");

        this.fields = fields;

        return this;
    }

    /**
     * @return Index type.
     */
    public QueryIndexType getIndexType() {
        return type;
    }

    /**
     * @param type Index type.
     */
    public QueryIndex setType(QueryIndexType type) {
        this.type = type;

        return this;
    }

    /**
     * Gets index inline size in bytes. When enabled part of indexed value will be placed directly to index pages,
     * thus minimizing data page accesses, thus increasing query performance.
     * <p>
     * Allowed values:
     * <ul>
     * <li>{@code -1} (default) - determine inline size automatically (see below)</li>
     * <li>{@code 0} - index inline is disabled (not recommended)</li>
     * <li>positive value - fixed index inline</li>
     * </ul>
     * When set to {@code -1}, Ignite will try to detect inline size automatically.
     * Index inline will be enabled for all fixed-length types, but <b>will not be enabled</b> for {@code String}.
     *
     * @return Index inline size in bytes.
     */
    public int getInlineSize() {
        return inlineSize;
    }

    /**
     * Sets index inline size in bytes. When enabled part of indexed value will be placed directly to index pages,
     * thus minimizing data page accesses, thus increasing query performance.
     * <p>
     * Allowed values:
     * <ul>
     * <li>{@code -1} (default) - determine inline size automatically (see below)</li>
     * <li>{@code 0} - index inline is disabled (not recommended)</li>
     * <li>positive value - fixed index inline</li>
     * </ul>
     * When set to {@code -1}, Ignite will try to detect inline size automatically.
     * Index inline will be enabled for all fixed-length types, but <b>will not be enabled</b> for {@code String}.
     *
     * @param inlineSize Inline size.
     */
    public QueryIndex setInlineSize(int inlineSize) {
        this.inlineSize = inlineSize;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (!(obj instanceof QueryIndex))
            return false;

        QueryIndex other = (QueryIndex)obj;

        return inlineSize == other.inlineSize &&
            Objects.equals(name, other.name) &&
            Arrays.equals(fields.toArray(), other.fields.toArray()) &&
            type == other.type;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(name, fields, type, inlineSize);
    }
}
