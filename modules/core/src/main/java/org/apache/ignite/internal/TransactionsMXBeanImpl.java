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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.transactions.Transaction;

/**
 * Transactions MXBean implementation.
 */
public class TransactionsMXBeanImpl implements TransactionsMXBean {
    private IgniteTransactionsEx transactionsEx;


    public TransactionsMXBeanImpl(IgniteTransactionsEx transactionsEx) {
        this.transactionsEx = transactionsEx;
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> getLocalActiveTransactions() {
        Collection<Transaction> transactions = transactionsEx.localActiveTransactions();
        HashMap result = new HashMap(transactions.size());
        for (Transaction transaction : transactionsEx.localActiveTransactions())
            result.put(transaction.xid().toString(), compose(transaction));
        return result;
    }

    /** {@inheritDoc} */
    @Override public String getTransaction(String txId) {
        if (txId != null && !txId.isEmpty())
            for (Transaction transaction : transactionsEx.localActiveTransactions())
                if (transaction.xid().toString().equals(txId))
                    return compose(transaction);
        return "";
    }

    /** {@inheritDoc} */
    @Override public void stopTransaction(String txId) {
        if (txId != null && !txId.isEmpty())
            for (Transaction transaction : transactionsEx.localActiveTransactions())
                if (transaction.xid().toString().equals(txId)) {
                    transaction.close();
                    return;
                }
        throw new RuntimeException("Transaction with id " + txId + " is not found");
    }

    private String compose(Transaction transaction) {
        return String.format("%s %s %s",
            transaction.nodeId(),
            transaction.state(),
            System.currentTimeMillis() - transaction.startTime());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TransactionsMXBeanImpl.class, this);
    }

}

