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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker;
import org.apache.ignite.internal.processors.cache.mvcc.MvccTxInfo;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.CIX1;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker.MVCC_TRACKER_ID_NA;

/** */
public class AckCoordinatorOnRollback extends CIX1<IgniteInternalFuture<IgniteInternalTx>> {
    /** */
    private static final long serialVersionUID = 8172699207968328284L;

    /** */
    private final GridNearTxLocal tx;

    public AckCoordinatorOnRollback(GridNearTxLocal tx) {
        this.tx = tx;
    }

    /** {@inheritDoc} */
    @Override public void applyx(IgniteInternalFuture<IgniteInternalTx> future) throws IgniteCheckedException {
        assert future.isDone();

        MvccQueryTracker qryTracker = tx.mvccQueryTracker();
        MvccTxInfo mvccInfo = tx.mvccInfo();

        assert qryTracker != null || mvccInfo != null;

        if (qryTracker != null)
            qryTracker.onTxDone(mvccInfo, tx.context(), false);
        else
            tx.context().coordinators().ackTxRollback(mvccInfo.snapshot(), null, MVCC_TRACKER_ID_NA);
    }
}
