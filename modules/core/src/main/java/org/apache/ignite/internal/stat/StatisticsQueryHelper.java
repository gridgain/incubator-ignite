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

package org.apache.ignite.internal.stat;

import static org.apache.ignite.internal.stat.StatisticsHolderQuery.UNKNOWN_QUERY_ID;

/**
 * Helper for gathering query IO statistics.
 */
public class StatisticsQueryHelper {
    /** */
    private static final ThreadLocal<StatisticsHolderQuery> CURRENT_QUERY_STATISTICS = new ThreadLocal<>();

    /**
     * Start gathering IO statistics for a query. Should be used together with {@code finishGatheringQueryStatistics}
     * method.
     */
    public static void startGatheringQueryStatistics() {
        startGatheringQueryStatistics(UNKNOWN_QUERY_ID);
    }

    /**
     * Start gathering IO statistics for query with given id. Should be used together with {@code
     * finishGatheringQueryStatistics} method.
     *
     * @param qryId Identifier of query.
     * @return {@code true} In case Gathering statistics has been started. {@code false} If statistics gathering already
     * started.
     */
    public static boolean startGatheringQueryStatistics(long qryId) {
        StatisticsHolderQuery currQryStatisticsHolder = CURRENT_QUERY_STATISTICS.get();

        assert currQryStatisticsHolder == null || currQryStatisticsHolder.queryId() == qryId : currQryStatisticsHolder;

        if (currQryStatisticsHolder != null && currQryStatisticsHolder.queryId() == qryId)
            return false;

        CURRENT_QUERY_STATISTICS.set(new StatisticsHolderQuery(qryId));

        return true;
    }

    /**
     * Start gathering IO statistics for query with given statistics as start point. Should be used together with {@code
     * finishGatheringQueryStatistics} method.
     *
     * @param statisticsHolderQry Statistics holder which will be used to gather query statistics.
     * @return {@code true} In case Gathering statistics has been started. {@code false} If statistics gathering already
     * started.
     */
    public static boolean startGatheringQueryStatistics(StatisticsHolderQuery statisticsHolderQry) {
        assert statisticsHolderQry != null;

        StatisticsHolderQuery currQry = CURRENT_QUERY_STATISTICS.get();

        assert currQry == null || currQry == statisticsHolderQry : currQry;

        if (currQry == statisticsHolderQry)
            return false;

        CURRENT_QUERY_STATISTICS.set(statisticsHolderQry);

        return true;
    }

    /**
     * Merge query statistics.
     *
     * @param logicalReads Logical reads which will be added to current query statistics.
     * @param physicalReads Physical reads which will be added to current query statistics,
     */
    public static void mergeQueryStatistics(long logicalReads, long physicalReads) {
        StatisticsHolderQuery currQryStatisticsHolder = CURRENT_QUERY_STATISTICS.get();

        assert currQryStatisticsHolder != null;

        currQryStatisticsHolder.merge(logicalReads, physicalReads);
    }

    /**
     * Finish gathering IO statistics for query. Should be used together with {@code startGatheringQueryStatistics}
     * method.
     *
     * @return Gathered statistics.
     */
    public static StatisticsHolder finishGatheringQueryStatistics() {
        StatisticsHolderQuery currQryStatisticsHolder = CURRENT_QUERY_STATISTICS.get();

        assert currQryStatisticsHolder != null;

        CURRENT_QUERY_STATISTICS.remove();

        return currQryStatisticsHolder;
    }

    /**
     * @return Current query statistics holder. Can be {@code null} in case gathering statistics was not started or
     * already finished.
     */
    public static StatisticsHolderQuery deriveQueryStatistics() {
        return CURRENT_QUERY_STATISTICS.get();
    }

    /**
     * Track logical read for current query.
     *
     * @param pageAddr Address of page.
     */
    static void trackLogicalReadQuery(long pageAddr) {
        StatisticsHolderQuery currQryStatisticsHolder = CURRENT_QUERY_STATISTICS.get();

        if (currQryStatisticsHolder != null)
            currQryStatisticsHolder.trackLogicalRead(pageAddr);

    }

    /**
     * Track physical and logical read for current query.
     *
     * @param pageAddr Address of page.
     */
    static void trackPhysicalAndLogicalReadQuery(long pageAddr) {
        StatisticsHolderQuery currQryStatisticsHolder = CURRENT_QUERY_STATISTICS.get();

        if (currQryStatisticsHolder != null)
            currQryStatisticsHolder.trackPhysicalAndLogicalRead(pageAddr);
    }

}
