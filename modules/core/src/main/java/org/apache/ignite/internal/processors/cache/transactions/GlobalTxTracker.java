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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.TransactionState;

import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.COMMITTING;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;
import static org.apache.ignite.transactions.TransactionState.ROLLING_BACK;

/**
 *
 */
public class GlobalTxTracker {
    /** Txs. */
    private final ConcurrentMap<GridCacheVersion,
        GridTuple3<Map<IgniteInternalTx, List<TransactionState>>, List<IgniteInternalTx>, ?>> txs = new ConcurrentHashMap<>();

    /**
     * @return Transaction groups mapped to the their {@link IgniteInternalTx#nearXidVersion()}.
     */
    public Map<GridCacheVersion, GridTuple3<Map<IgniteInternalTx, List<TransactionState>>, List<IgniteInternalTx>, ?>> txs() {
        return new HashMap<>(txs);
    }

    /**
     * @param tx Internal transaction.
     * @param log Logger.
     */
    void onTxPrepared(IgniteInternalTx tx, IgniteLogger log) {
        computeTxMapping(tx, log, PREPARED, ROLLING_BACK, ROLLED_BACK);
    }

    /**
     * @param tx Internal transaction.
     * @param log Logger.
     */
    void onTxCommitting(IgniteInternalTx tx, IgniteLogger log) {
        computeTxMapping(tx, log, PREPARED, COMMITTING, COMMITTED);
    }

    /**
     * @param tx Internal transaction.
     * @param log Logger.
     */
    void onTxCommitted(IgniteInternalTx tx, IgniteLogger log) {
        for (IgniteInternalTx tx0 : computeTxMapping(tx, log, PREPARED, COMMITTING, COMMITTED)) {
            if (tx0.state() != COMMITTED)
                return;
        }

        txs.remove(tx.nearXidVersion());

        U.warn(log, "All Tx[nearXidVer=" + tx.nearXidVersion() + "] are " + COMMITTED + ", mapping removed.");
    }

    /**
     * @param tx Internal transaction.
     * @param log Logger.
     */
    void onTxRolledBack(IgniteInternalTx tx, IgniteLogger log) {
        for (IgniteInternalTx tx0 : computeTxMapping(tx, log, PREPARED, ROLLING_BACK, ROLLED_BACK)) {
            if (tx0.state() != ROLLED_BACK)
                return;
        }

        txs.remove(tx.nearXidVersion());

        U.warn(log, "All Tx[nearXidVer=" + tx.nearXidVersion() + "] are " + ROLLED_BACK + ", mapping removed.");
    }

    /**
     * @param tx Tx.
     * @param log Logger.
     * @param expected Expected tx states.
     */
    private List<IgniteInternalTx> computeTxMapping(IgniteInternalTx tx, IgniteLogger log, TransactionState... expected) {
        Set<TransactionState> expectedStates = new HashSet<>(Arrays.asList(expected));

        return txs.compute(tx.nearXidVersion(), (ver, tuple) -> {
            if (tuple == null)
                tuple = new GridTuple3<>(new IdentityHashMap<>(), new ArrayList<>(), null);

            final List<IgniteInternalTx> txs0 = tuple.get2();

            tuple.get1().computeIfAbsent(tx, k -> {
                txs0.add(tx);

                return new ArrayList<>();
            }).add(tx.state());

            assert txs0.size() == tuple.get1().size();

            for (IgniteInternalTx tx0 : txs0) {
                assert tx0.nearXidVersion().equals(ver);

                if (!expectedStates.contains(tx0.state())) {
                    U.warn(log, "Tx[nearXidVer=" + tx0.nearXidVersion() + ", class=" + tx0.getClass().getSimpleName() +
                        ", nodeId=" + tx0.nodeId() + "]\n  has state " + tx0.state() + " but expected is " + expectedStates +
                        " because Tx[class=" + tx.getClass().getSimpleName() + ", nodeId=" + tx.nodeId() + "] is " + tx.state());
                }
            }

            return tuple;
        }).get2();
    }
}
