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
 *
 */

package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Keep information about all running queries.
 */
public class RunningQueryManager {
    /** Keep registered user queries. */
    private final ConcurrentMap<Long, GridRunningQueryInfo> runs = new ConcurrentHashMap<>();

    /** Unique id for queries on single node. */
    private final AtomicLong qryIdGen = new AtomicLong();

    /** History size. */
    private final int histSz;

    /** Query history tracker. */
    private volatile QueryHistoryTracker queryHistoryTracker;

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public RunningQueryManager(GridKernalContext ctx) {
        histSz = ctx.config().getSqlQueryHistorySize();

        queryHistoryTracker = new QueryHistoryTracker(histSz);
    }

    /**
     * Register running query.
     *
     * @param qry Query text.
     * @param qryType Query type.
     * @param schemaName Schema name.
     * @param loc Local query flag.
     * @param cancel Query cancel. Should be passed in case query is cancelable, or {@code null} otherwise.
     * @return Registered RunningQueryInfo.
     */
    public GridRunningQueryInfo register(String qry, GridCacheQueryType qryType, String schemaName,
        boolean loc, @Nullable GridQueryCancel cancel) {
        long qryId = qryIdGen.incrementAndGet();

        GridRunningQueryInfo run = new GridRunningQueryInfo(
            qryId,
            qry,
            qryType,
            schemaName,
            System.currentTimeMillis(),
            cancel,
            loc
        );

        GridRunningQueryInfo preRun = runs.putIfAbsent(qryId, run);

        assert preRun == null : "Running query already registered [prev_qry=" + preRun + ", newQry=" + run + ']';

        return run;
    }

    /**
     * Unregister running query.
     *
     * @param runningQryInfo Running query info..
     * @param failed {@code true} In case query was failed.
     * @return Unregistered running query info. {@code null} in case running query is not registered.
     */
    @Nullable public GridRunningQueryInfo unregister(@Nullable GridRunningQueryInfo runningQryInfo, boolean failed) {
        return (runningQryInfo != null) ? unregister(runningQryInfo.id(), failed) : null;
    }

    /**
     * Unregister running query.
     *
     * @param qryId Query id.
     * @param failed {@code true} In case query was failed.
     * @return Unregistered running query info. {@code null} in case running query with give id wasn't found.
     */
    @Nullable public GridRunningQueryInfo unregister(Long qryId, boolean failed) {
        return unregister(qryId, failed, true);
    }

    /**
     * Unregister running query.
     *
     * @param qryId Query id.
     * @param failed {@code true} In case query was failed.
     * @param collectMetrics {@code true} In case metrics should be collected.
     * @return Unregistered running query info. {@code null} in case running query with give id wasn't found.
     */
    @Nullable public GridRunningQueryInfo unregister(Long qryId, boolean failed, boolean collectMetrics) {
        if (qryId == null)
            return null;

        GridRunningQueryInfo unregistered = runs.remove(qryId);

        if (collectMetrics)
            queryHistoryTracker.collectMetrics(unregistered, failed);

        return unregistered;
    }

    /**
     * Return long running user queries.
     *
     * @param duration Duration of long query.
     * @return List of queries which running longer than given duration.
     */
    public Collection<GridRunningQueryInfo> longRunningQueries(long duration) {
        Collection<GridRunningQueryInfo> res = new ArrayList<>();

        long curTime = System.currentTimeMillis();

        for (GridRunningQueryInfo runningQryInfo : runs.values()) {
            if (runningQryInfo.longQuery(curTime, duration))
                res.add(runningQryInfo);
        }

        return res;
    }

    /**
     * Cancel query.
     *
     * @param qryId Query id.
     */
    public void cancel(Long qryId) {
        GridRunningQueryInfo run = runs.get(qryId);

        if (run != null)
            run.cancel();
    }

    /**
     * Cancel all executing queries and deregistering all of them.
     */
    public void stop() {
        for (GridRunningQueryInfo r : runs.values()) {
            try {
                unregister(r.id(), false, false);

                r.cancel();
            }
            catch (Exception ignore) {
                // No-op.
            }
        }
    }

    /**
     * Gets query history statistics. Size of history could be configured via {@link
     * IgniteConfiguration#setSqlQueryHistorySize(int)}
     *
     * @return Queries history statistics aggregated by query text, schema and local flag.
     */
    public Map<QueryHistoryMetricsKey, QueryHistoryMetrics> queryHistoryMetrics() {
        return queryHistoryTracker.queryHistoryMetrics();
    }

    /**
     * Reset query history metrics.
     */
    public void resetQueryHistoryMetrics() {
        queryHistoryTracker = new QueryHistoryTracker(histSz);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RunningQueryManager.class, this);
    }
}
