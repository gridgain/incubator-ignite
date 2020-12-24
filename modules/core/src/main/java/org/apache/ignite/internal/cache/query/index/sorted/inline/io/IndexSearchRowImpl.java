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

package org.apache.ignite.internal.cache.query.index.sorted.inline.io;

import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexSchema;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a search row that used to find a place in a tree.
 */
public class IndexSearchRowImpl extends IndexRowImpl {
    /**
     * If {@code true} then length of {@link #keys()} must be equal to length of schema, so use full
     * schema to search. If {@code false} then it's possible to use only part of schema for search.
     */
    private final boolean fullSchemaSearch;

    /** Constructor. */
    public IndexSearchRowImpl(Object[] idxKeys, @Nullable CacheDataRow row, SortedIndexSchema schema) {
        super(schema, row, idxKeys);

        fullSchemaSearch = isFullSchemaSearch(idxKeys, schema.getKeyDefinitions().length);
    }

    /** {@inheritDoc} */
    @Override public Object getKey(int idx) {
        return keys()[idx];
    }

    /** {@inheritDoc} */
    @Override public int getSearchKeysCount() {
        return keys().length;
    }

    /** {@inheritDoc} */
    @Override public boolean isFullSchemaSearch() {
        return fullSchemaSearch;
    }

    /** */
    private boolean isFullSchemaSearch(Object[] idxKeys, int schemaLength) {
        if (idxKeys.length != schemaLength)
            return false;

        for (int i = 0; i < schemaLength; i++) {
            // Java null means that column is not specified in a search row, for SQL NULL a special constant is used
            if (idxKeys[i] == null)
                return false;
        }

        return true;
    }
}
