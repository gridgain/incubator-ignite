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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Represents a search row that used to find a place in a tree.
 */
public class IndexSearchRowImpl implements IndexRow {
    /** */
    private final Object[] keys;

    /** */
    private final SortedIndexSchema schema;

    /** Constructor. */
    public IndexSearchRowImpl(Object[] keys, SortedIndexSchema schema) {
        this.keys = keys;
        this.schema = schema;
    }

    /** {@inheritDoc} */
    @Override public Object key(int idx) {
        return keys[idx];
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return schema.getKeyDefinitions().length;
    }

    /** {@inheritDoc} */
    @Override public SortedIndexSchema schema() {
        return schema;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IndexSearchRowImpl.class, this);
    }

    /** {@inheritDoc} */
    @Override public long getLink() {
        assert false : "Should not get link by IndexSearchRowImpl";

        return 0;
    }
}
