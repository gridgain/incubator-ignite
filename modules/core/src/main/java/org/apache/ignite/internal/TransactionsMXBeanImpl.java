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

package org.apache.ignite.internal;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionState;

/**
 * TransactionsMXBean implementation.
 */
public class TransactionsMXBeanImpl implements TransactionsMXBean {
    /** */
    private final GridKernalContextImpl gridKernalCtx;

    /**
     * @param ctx Context.
     */
    TransactionsMXBeanImpl(GridKernalContextImpl ctx) {
        this.gridKernalCtx = ctx;
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> getAllLocalTransactions() {
        return getLocalTxs(0);
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> getLongRunningLocalTransactions(final int duration) {
        return getLocalTxs(duration);
    }

    /** {@inheritDoc} */
    @Override public String stopTransaction(String txId) throws IgniteCheckedException {
        final Collection<Transaction> txs = gridKernalCtx.cache().transactions().localActiveTransactions();
        if (!F.isEmpty(txId))
            for (Transaction tx : txs)
                if (tx.xid().toString().equals(txId)) {
                    tx.close();
                    return String.format("Transaction %s is %s", tx.xid(), tx.state());
                }

        throw new IgniteCheckedException("Transaction with id " + txId + " is not found");
    }

    /**
     * @param duration Duration.
     */
    private Map<String, String> getLocalTxs(long duration) {
        final Collection<Transaction> txs = gridKernalCtx.cache().transactions().localActiveTransactions();

        final long start = System.currentTimeMillis();

        final HashMap<String, String> res = new HashMap<>();

        for (Transaction tx : txs)
            if (start - tx.startTime() >= duration)
                res.put(tx.xid().toString(), composeTx(tx));

        return res;
    }

    /**
     * @param id Id.
     */
    private String composeNodeInfo(final UUID id) {
        final ClusterNode node = gridKernalCtx.discovery().node(id);

        if (node == null)
            return "";

        return String.format("%s %s",
            node.id(),
            node.hostNames());
    }

    /**
     * @param ids Ids.
     */
    private String composeNodeInfo(final Set<UUID> ids) {
        final GridStringBuilder sb = new GridStringBuilder();

        sb.a("[");

        String delim = "";

        for (UUID id : ids) {
            sb.a(delim).a(composeNodeInfo(id));

            delim = ", ";
        }

        sb.a("]");

        return sb.toString();
    }

    /**
     * @param tx Transaction.
     */
    private String composeTx(final Transaction tx) {
        final UUID node = tx.nodeId();

        final UUID originating = tx.originatingNodeId();

        final TransactionState txState = tx.state();

        String top = txState + ", NEAR, ";

        if (txState == TransactionState.PREPARING) {
            final Map<UUID, Collection<UUID>> transactionNodes = tx.transactionNodes();

            if (!F.isEmpty(transactionNodes)) {
                final Set<UUID> primaryNodes = transactionNodes.keySet();

                if (!F.isEmpty(primaryNodes))
                    top += "PRIMARY: " + composeNodeInfo(primaryNodes) + ", ";
            }
        }

        final Long duration = System.currentTimeMillis() - tx.startTime();

        return top + "DURATION: " + duration;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransactionsMXBeanImpl.class, this);
    }
}


