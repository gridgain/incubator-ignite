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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridLongList;
import org.jetbrains.annotations.Nullable;

/**
 */
public class PartitionAtomicDebugUpdateCounterImpl extends PartitionAtomicUpdateCounterImpl {
    /** */
    private final CacheGroupContext grp;

    /** */
    private final int partId;

    /** */
    private final IgniteLogger log;

    public PartitionAtomicDebugUpdateCounterImpl(CacheGroupContext grp, int partId) {
        this.grp = grp;
        this.partId = partId;
        if (grp != null)
            this.log = grp.shared().logger(getClass());
        else
            this.log = null;
    }

    @Override public void init(long initUpdCntr, @Nullable byte[] cntrUpdData) {
        super.init(initUpdCntr, cntrUpdData);

        log.debug("[op=init" +
            ", grpId=" + grp.groupId() +
            ", grpName=" + grp.cacheOrGroupName() +
            ", caches=" + grp.caches() +
            ", atomicity=" + grp.config().getAtomicityMode() +
            ", syncMode=" + grp.config().getWriteSynchronizationMode() +
            ", mode=" + grp.config().getCacheMode() +
            ", partId=" + partId +
            ", cur=" + toString() +
            ']');
    }

    @Override public void update(long val) {
        super.update(val);
    }

    @Override public synchronized boolean update(long start, long delta) {
        long cur = get();

        try {
            return super.update(start, delta);
        }
        finally {
            log.debug("[op=update" +
                ", range=(" + start + "," + delta + ")" +
                ", cur=" + cur +
                ", new=" + get() + ']');
        }
    }

    @Override public synchronized void updateInitial(long start, long delta) {
        long cur = get();

        try {
            super.updateInitial(start, delta);
        }
        finally {
            log.debug("[op=updateInitial" +
                ", range=(" + start + "," + delta + ")" +
                ", cur=" + cur +
                ", new=" + get() + ']');
        }
    }

    @Override public long reserve(long delta) {
        long cur = get();

        try {
            return super.next(delta);
        }
        finally {
            log.debug("[op=reserve" +
                ", delta=" + delta +
                ", cur=" + cur +
                ", new=" + get() + ']');
        }
    }

    @Override public long next(long delta) {
        long cur = get();

        try {
            return super.next(delta);
        }
        finally {
            log.debug("[op=next" +
                ", delta=" + delta +
                ", cur=" + cur +
                ", new=" + get() + ']');
        }
    }
}
