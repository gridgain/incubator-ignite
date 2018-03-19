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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.NotNull;

/**
 * Transactions MXBean implementation.
 */
public class TransactionsMXBeanImpl implements TransactionsMXBean {
    /** Grid kernal context. */
    private final GridKernalContextImpl gridKernalCtx;

    /**
     * @param ctx Context.
     */
    public TransactionsMXBeanImpl(GridKernalContextImpl ctx) {
        this.gridKernalCtx = ctx;
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> getLocalActiveTransactions() {
        Collection<Transaction> transactions = transactions();
        Map<UUID, ClusterNode> nodes = nodes();

        HashMap<String, String> res = new HashMap<>(transactions.size());

        for (Transaction transaction : transactions)
            res.put(transaction.xid().toString(), compose(nodes, transaction));

        return res;
    }

    /** {@inheritDoc} */
    @Override public String getTransaction(String txId) {
        Collection<Transaction> transactions = transactions();
        if (txId != null && !txId.isEmpty())
            for (Transaction transaction : transactions)
                if (transaction.xid().toString().equals(txId))
                    return compose(nodes(), transaction);
        return "";
    }

    /** {@inheritDoc} */
    @Override public void stopTransaction(String txId) {
        Collection<Transaction> transactions = transactions();
        if (txId != null && !txId.isEmpty())
            for (Transaction transaction : transactions)
                if (transaction.xid().toString().equals(txId)) {
                    transaction.close();
                    return;
                }
        throw new RuntimeException("Transaction with id " + txId + " is not found");
    }

    /**
     * @param nodes Nodes.
     * @param transaction Transaction.
     */
    private String compose(Map<UUID, ClusterNode> nodes, Transaction transaction) {
        Collection<String> ips = nodes.containsKey(transaction.nodeId()) ? nodes.get(transaction.nodeId()).addresses() : Collections.emptyList();
        Collection<String> hostNames = nodes.containsKey(transaction.nodeId()) ? nodes.get(transaction.nodeId()).hostNames() : Collections.emptyList();

        return String.format("%s %s %s %s %s",
            transaction.nodeId(),
            ips.toString(),
            hostNames.toString(),
            transaction.state(),
            System.currentTimeMillis() - transaction.startTime());
    }

    /**
     *
     */
    @NotNull
    private Map<UUID, ClusterNode> nodes() {
        Collection<ClusterNode> nodesColl = gridKernalCtx.config().getDiscoverySpi().getRemoteNodes();
        if (nodesColl == null)
            return Collections.emptyMap();
        HashMap<UUID, ClusterNode> nodesMap = new HashMap<>(nodesColl.size());
        for (ClusterNode clusterNode : nodesColl)
            nodesMap.put(clusterNode.id(), clusterNode);
        return nodesMap;
    }

    /**
     *
     */
    @NotNull
    private Collection<Transaction> transactions() {
        Collection<Transaction> transactions = gridKernalCtx.cache().transactions().localActiveTransactions();
        return transactions == null ? Collections.emptyList() : transactions;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransactionsMXBeanImpl.class, this);
    }
}

