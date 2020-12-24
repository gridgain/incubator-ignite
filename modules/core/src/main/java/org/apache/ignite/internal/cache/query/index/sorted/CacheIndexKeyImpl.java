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

package org.apache.ignite.internal.cache.query.index.sorted;

import java.util.Arrays;
import org.apache.ignite.cache.query.index.sorted.IndexKey;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;

/**
 * Complex index key that represents an index cache row.
 */
public class CacheIndexKeyImpl implements IndexKey {
    /** Underlying index keys. */
    private final Object[] keys;

    /** Underlying cache row. */
    private final CacheDataRow row;

    /** */
    public CacheIndexKeyImpl(SortedIndexSchema schema, CacheDataRow row) {
        keys = new Object[schema.getKeyDefinitions().length];

        for (int i = 0; i < schema.getKeyDefinitions().length; ++i)
            keys[i] = schema.getIndexKey(i, row);

        this.row = row;
    }

    /** {@inheritDoc} */
    @Override public Object[] keys() {
        return keys;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        CacheIndexKeyImpl key = (CacheIndexKeyImpl) o;

        return Arrays.equals(keys, key.keys);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Arrays.hashCode(keys);
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow cacheRow() {
        return row;
    }
}
