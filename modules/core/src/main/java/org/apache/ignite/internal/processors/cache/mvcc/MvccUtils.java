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

package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPagePayload;

import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccProcessor.MVCC_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.MVCC_INFO_SIZE;

/**
 * Utils for MVCC.
 */
public class MvccUtils {
    /** */
    private static IsVisibleMvccClosure isVisible = new IsVisibleMvccClosure();

    /** */
    private static HasNewVersionMvccClosure hasNewVer = new HasNewVersionMvccClosure();

    /** */
    private static GetNewVersionMvccClosure getNewVer = new GetNewVersionMvccClosure();

    /**
     *
     */
    private MvccUtils(){
    }

    /**
     * Checks if version is visible from the given snapshot.
     *
     * @param snapshot Snapshot.
     * @param mvccCrd Mvcc coordinator.
     * @param mvccCntr Mvcc counter.
     * @param newMvccCrd New mvcc coordinator.
     * @param newMvccCntr New mvcc counter.
     * @return {@code True} if visible.
     */
    public static boolean isVisibleForSnapshot(MvccSnapshot snapshot, long mvccCrd, long mvccCntr, long newMvccCrd,
        long newMvccCntr) {

        long snapshotCrd = snapshot.coordinatorVersion();
        long snapshotCntr = snapshot.counter();

        if (mvccCrd > snapshotCrd || mvccCrd == snapshotCrd && mvccCntr > snapshotCntr ||
            mvccCrd == snapshotCrd && snapshot.activeTransactions().contains(mvccCntr))
            return false; // invisible if xid_min transaction is in the future or is active now.
        else if (newMvccCrd == 0 && newMvccCntr == MVCC_COUNTER_NA)
            return true; // visible if xid_max is empty.
        else if (newMvccCrd == snapshotCrd && newMvccCntr == snapshotCntr)
            return false; // invisible if deleted by the current tx.
        else // visible if xid_max is in the future or is active now.
            return newMvccCrd > snapshotCrd || newMvccCrd == snapshotCrd && newMvccCntr > snapshotCntr ||
                (newMvccCrd == snapshotCrd && snapshot.activeTransactions().contains(newMvccCntr));
    }

    /**
     * @param crdVer Mvcc coordinator version.
     * @param cntr Counter.
     * @return Always {@code true}.
     */
    public static boolean assertMvccVersionValid(long crdVer, long cntr) {
        assert crdVer > 0 && cntr != MVCC_COUNTER_NA;

        return true;
    }

    /**
     * @param topVer Topology version for cache operation.
     * @return Error.
     */
    public static IgniteCheckedException noCoordinatorError(AffinityTopologyVersion topVer) {
        return new ClusterTopologyServerNotFoundException("Mvcc coordinator is not assigned for " +
            "topology version: " + topVer);
    }

    /**
     * Checks if a row has not empty new version (xid_max).
     *
     * @param grp Cache group context.
     * @param link Link to the row.
     * @return {@code True} if row has a new version.
     * @throws IgniteCheckedException If failed.
     */
    public static boolean hasNewMvccVersion(CacheGroupContext grp, long link)
        throws IgniteCheckedException {
        return applyMvccClosure(grp, link, hasNewVer, null);
    }


    /**
     * Checks if a row is visible for the given snapshot.
     *
     * @param grp Cache group context.
     * @param link Link to the row.
     * @param snapshot Mvcc snapshot.
     * @return {@code True} if row is visible for the given snapshot.
     * @throws IgniteCheckedException If failed.
     */
    public static boolean isVisibleForSnapshot(CacheGroupContext grp, long link, MvccSnapshot snapshot)
        throws IgniteCheckedException {
        return applyMvccClosure(grp, link, isVisible, snapshot);
    }

    /**
     * Returns new version of row (xid_max) if any.
     *
     * @param grp Cache group context.
     * @param link Link to the row.
     * @return New {@code MvccVersion} if row has xid_max, or null if doesn't.
     * @throws IgniteCheckedException If failed.
     */
    public static MvccVersion getNewVersion(CacheGroupContext grp, long link)
        throws IgniteCheckedException {
        return applyMvccClosure(grp, link, getNewVer, null);
    }

    /**
     * Encapsulates common logic for working with row mvcc info: page locking/unlocking, checks and other.
     * Strategy pattern.
     *
     * @param grp Cache group.
     * @param link Row link.
     * @param clo Closure to apply.
     * @param snapshot Mvcc snapshot.
     * @param <R> Return type.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    private static <R> R applyMvccClosure(CacheGroupContext grp, long link, MvccClosure<R> clo, MvccSnapshot snapshot)
        throws IgniteCheckedException {
        assert grp.mvccEnabled();

        PageMemory pageMem = grp.dataRegion().pageMemory();
        int grpId = grp.groupId();

        long pageId = pageId(link);
        long page = pageMem.acquirePage(grpId, pageId);

        try {
            long pageAddr = pageMem.readLock(grpId, pageId, page);

            try{
                DataPageIO dataIo = DataPageIO.VERSIONS.forPage(pageAddr);

                DataPagePayload data = dataIo.readPayload(pageAddr, itemId(link), pageMem.pageSize());

                assert data.payloadSize() >= MVCC_INFO_SIZE : "MVCC info should fit on the very first data page.";

                long mvccCrd = dataIo.mvccCoordinator(pageAddr, data.offset());
                long mvccCntr = dataIo.mvccCounter(pageAddr, data.offset());
                long newMvccCrd = dataIo.newMvccCoordinator(pageAddr, data.offset());
                long newMvccCntr = dataIo.newMvccCounter(pageAddr, data.offset());

                assert mvccCrd > 0 && mvccCntr > MVCC_COUNTER_NA;
                assert newMvccCrd > 0 == newMvccCntr > MVCC_COUNTER_NA;

                return clo.apply(snapshot, mvccCrd, mvccCntr, newMvccCrd, newMvccCntr);
            }
            finally {
                pageMem.readUnlock(grpId, pageId, page);
            }
        }
        finally {
            pageMem.releasePage(grpId, pageId, page);
        }
    }

    /**
     * Mvcc closure interface.
     * @param <R> Return type.
     */
    private interface MvccClosure<R> {
        /**
         * Runs closure over the Mvcc info.
         * @param snapshot Mvcc snapshot.
         * @param mvccCrd Coordinator version.
         * @param mvccCntr Counter.
         * @param newMvccCrd New mvcc coordinator
         * @param newMvccCntr New mvcc counter.
         * @return Result.
         */
        public R apply(MvccSnapshot snapshot, long mvccCrd, long mvccCntr, long newMvccCrd, long newMvccCntr);
    }

    /**
     * Closure for checking row visibility for snapshot.
     */
    private static class IsVisibleMvccClosure implements MvccClosure<Boolean> {
        /** {@inheritDoc} */
        @Override public Boolean apply(MvccSnapshot snapshot, long mvccCrd, long mvccCntr,
            long newMvccCrd, long newMvccCntr) {
            return isVisibleForSnapshot(snapshot, mvccCrd, mvccCntr, newMvccCrd, newMvccCntr);
        }
    }

    /**
     * Closure for checking if row has a new version (xid_max).
     */
    private static class HasNewVersionMvccClosure implements MvccClosure<Boolean> {
        /** {@inheritDoc} */
        @Override public Boolean apply(MvccSnapshot snapshot, long mvccCrd, long mvccCntr,
            long newMvccCrd, long newMvccCntr) {
            return newMvccCrd > 0;
        }
    }

    /**
     * Closure for getting xid_max version of row.
     */
    private static class GetNewVersionMvccClosure implements MvccClosure<MvccVersion> {
        /** {@inheritDoc} */
        @Override public MvccVersion apply(MvccSnapshot snapshot, long mvccCrd, long mvccCntr,
            long newMvccCrd, long newMvccCntr) {
            return newMvccCrd == 0 ? null : new MvccVersionImpl(newMvccCrd, newMvccCntr);
        }
    }
}
