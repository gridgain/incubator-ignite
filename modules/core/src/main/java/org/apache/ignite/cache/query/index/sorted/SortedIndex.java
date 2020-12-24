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

package org.apache.ignite.cache.query.index.sorted;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexSearchRow;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.jetbrains.annotations.Nullable;

/**
 * Interface for sorted Ignite indexes.
 */
public interface SortedIndex extends Index {
    /**
     * Finds index rows by specified range in specifed tree segment. Range can be bound or unbound.
     *
     * @param lower Nullable lower bound.
     * @param upper Nullable upper bound.
     * @param segment Number of tree segment to find.
     * @return Cursor of found index rows.
     */
    public GridCursor<IndexSearchRow> find(@Nullable IndexKey lower, @Nullable IndexKey upper, int segment) throws IgniteCheckedException;

    /**
     * Finds index rows by specified range in specifed tree segment with cache filtering. Range can be bound or unbound.
     *
     * @param lower Nullable lower bound.
     * @param upper Nullable upper bound.
     * @param segment Number of tree segment to find.
     * @param filter Cache entry filter.
     * @return Cursor of found index rows.
     */
    public GridCursor<IndexSearchRow> find(IndexKey lower, IndexKey upper, int segment, IndexingQueryFilter filter)
        throws IgniteCheckedException;

    /**
     * Finds first or last index row for specified tree segment and cache filter.
     *
     * @param firstOrLast if {@code true} then return first index row or otherwise last row.
     * @param segment Number of tree segment to find.
     * @param filter Cache entry filter.
     * @return Cursor of found index rows.
     */
    public GridCursor<IndexSearchRow> findFirstOrLast(boolean firstOrLast, int segment, IndexingQueryFilter filter)
        throws IgniteCheckedException;

    /**
     * Counts index rows in specified tree segment.
     *
     * @param segment Number of tree segment to find.
     * @return count of index rows for specified segment.
     */
    public long count(int segment) throws IgniteCheckedException;

    /**
     * Counts index rows in specified tree segment with cache filter.
     *
     * @param segment Number of tree segment to find.
     * @param filter Cache entry filter.
     * @return count of index rows for specified segment.
     */
    public long count(int segment, IndexingQueryFilter filter) throws IgniteCheckedException;

    /**
     * Counts index rows for all segments.
     *
     * @return total count of index rows.
     */
    public long totalCount() throws IgniteCheckedException;

    /**
     * Returns amount of index tree segments.
     *
     * @return amount of index tree segments.
     */
    public int segmentsCount();
}
