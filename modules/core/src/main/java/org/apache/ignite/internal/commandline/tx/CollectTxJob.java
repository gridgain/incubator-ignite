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
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;

/**
 * Retrieving transactions from a node
 */
public class CollectTxJob extends AbstractTxJob {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param arg Argument.
     */
    public CollectTxJob(TransactionsTaskArguments arg) {
        super(arg);
    }

    /** {@inheritDoc} */
    public Object execute() throws IgniteException {
        Collection<IgniteInternalTx> activeTxs = ignite.context().cache().context().tm().activeTransactions();

        if (activeTxs.isEmpty())
            return Collections.emptySet();

        long curTime = System.currentTimeMillis();

        Set<TxDTO> txInfos = new HashSet<>();

        for (IgniteInternalTx tx0 : activeTxs) {
            if (!(tx0 instanceof GridNearTxLocal))
                continue;

            GridNearTxLocal tx = (GridNearTxLocal)tx0;

            if (!checkTx(curTime, tx))
                continue;

            ClusterNode node = ignite.context().discovery().node(tx.nodeId());
            txInfos.add(TxDTO.of(node, tx));

        }

        return txInfos;
    }

}
