/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.internal.commandline.tx;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;

public class StopTxJob extends AbstractTxJob {
    String stopXid;

    /**
     * @param arg Argument.
     */
    public StopTxJob(TransactionsTaskArguments arg) {
        super(arg);

        this.stopXid = arg.stopXid();
    }

    /** {@inheritDoc} */
    public Object execute() throws IgniteException {
        Collection<IgniteInternalTx> activeTxs = ignite.context().cache().context().tm().activeTransactions();

        if (activeTxs.isEmpty())
            return Collections.emptySet();

        long curTime = System.currentTimeMillis();

        for (IgniteInternalTx tx0 : activeTxs) {
            if (!(tx0 instanceof GridNearTxLocal))
                continue;

            if (!tx0.xid().toString().equals(stopXid))
                continue;

            GridNearTxLocal tx = (GridNearTxLocal)tx0;

            if (!checkTx(curTime, tx))
                return Collections.emptyList();

            ClusterNode node = ignite.context().discovery().node(tx.nodeId());

            try {
                tx.close();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            TxDTO txInfo = TxDTO.of(node, tx);

            return Collections.singletonList(txInfo);
        }

        return Collections.emptyList();
    }
}
