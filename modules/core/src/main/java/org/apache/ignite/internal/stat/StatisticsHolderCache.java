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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

/**
 *
 */
public class StatisticsHolderCache implements StatisticsHolder {
    /** */
    public static final String PHYSICAL_READS = "PHYSICAL_READS";
    /** */
    public static final String LOGICAL_READS = "LOGICAL_READS";
    /** */
    private LongAdder logicalReadCntr = new LongAdder();

    /** */
    private LongAdder physicalReadCntr = new LongAdder();

    /** */
    private final String cacheName;

    /**
     * @param cacheName Name of cache.
     */
    public StatisticsHolderCache(String cacheName) {
        assert cacheName != null;

        this.cacheName = cacheName;
    }

    /** {@inheritDoc} */
    @Override public void trackLogicalRead(long pageAddr) {
        int pageIoType = PageIO.getType(pageAddr);

        if (pageIoType == PageIO.T_DATA) {
            logicalReadCntr.increment();

            StatisticsHelper.trackLogicalReadQuery(pageAddr);
        }

    }

    /** {@inheritDoc} */
    @Override public void trackPhysicalAndLogicalRead(long pageAddr) {
        int pageIoType = PageIO.getType(pageAddr);

        if (pageIoType == PageIO.T_DATA) {
            logicalReadCntr.increment();

            physicalReadCntr.increment();

            StatisticsHelper.trackPhysicalAndLogicalReadQuery(pageAddr);
        }
    }

    /** {@inheritDoc} */
    @Override public long logicalReads() {
        return logicalReadCntr.longValue();
    }

    /** {@inheritDoc} */
    @Override public long physicalReads() {
        return physicalReadCntr.longValue();
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> logicalReadsMap() {
        Map<String, Long> res = new HashMap<>(2);

        res.put(LOGICAL_READS, logicalReads());

        return res;
    }

    /** {@inheritDoc} */
    @Override public Map<String, Long> physicalReadsMap() {
        Map<String, Long> res = new HashMap<>(2);

        res.put(PHYSICAL_READS, physicalReads());

        return res;
    }

    /** {@inheritDoc} */
    @Override public void resetStatistics() {
        logicalReadCntr.reset();

        physicalReadCntr.reset();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "StatisticsHolderCache{" + "logicalReadCntr=" + logicalReadCntr +
            ", physicalReadCntr=" + physicalReadCntr +
            ", cacheName='" + cacheName + '\'' +
            '}';
    }
}
