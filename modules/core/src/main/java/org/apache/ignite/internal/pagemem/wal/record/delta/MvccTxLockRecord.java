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
package org.apache.ignite.internal.pagemem.wal.record.delta;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.tree.RowLinkIO;

/**
 * Delta record for MVCC TX lock.
 */
public class MvccTxLockRecord extends PageDeltaRecord {
    /** Item index. */
    private int idx;

    /** Lock version coordinator. */
    private long crd;

    /** Lock version counter. */
    private long cntr;

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param idx Item index.
     * @param crd Lock version coordinator.
     * @param cntr Lock version counter.
     */
    public MvccTxLockRecord(int grpId, long pageId, int idx, long crd, long cntr) {
        super(grpId, pageId);

        this.idx = idx;
        this.crd = crd;
        this.cntr = cntr;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        RowLinkIO io = PageIO.getPageIO(pageAddr);

        io.setMvccLockCoordinatorVersion(pageAddr, idx, crd);
        io.setMvccLockCounter(pageAddr, idx, cntr);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.MVCC_TX_LOCK_RECORD;
    }

    /**
     * @return Item index.
     */
    public int itemIndex() {
        return idx;
    }

    /**
     * @return Mvcc coordinator version.
     */
    public long coordinatorVersion() {
        return crd;
    }

    /**
     * @return Mvcc counter.
     */
    public long counter() {
        return cntr;
    }
}
