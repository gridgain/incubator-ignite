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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;

/**
 *
 */
public class TxDTO implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final NodeDTO node;

    /** */
    private final IgniteUuid xid;

    /** */
    private final long duration;

    /** */
    private final TransactionIsolation isolation;

    /** */
    private final TransactionConcurrency concurrency;

    /** */
    private final long timeout;

    /** */
    private final String lb;

    /** */
    private final Collection<UUID> primaryNodes;

    /** */
    private final TransactionState state;

    /** */
    private final String nearXidVer;

    /**
     * @param node Node.
     * @param xid Xid.
     * @param state State.
     * @param nearXidVer Near xid version.
     * @param startTime Start time.
     * @param isolation Isolation.
     * @param concurrency Concurrency.
     * @param timeout Timeout.
     * @param lb Label.
     * @param txNodes Transactions nodes mapping.
     */
    TxDTO(NodeDTO node, IgniteUuid xid, TransactionState state, String nearXidVer,
        long startTime, TransactionIsolation isolation,
        TransactionConcurrency concurrency, long timeout, String lb, Map<UUID, Collection<UUID>> txNodes) {
        this.node = node;
        this.xid = xid;
        this.state = state;
        this.nearXidVer = nearXidVer;
        this.duration = U.currentTimeMillis() - startTime;
        this.isolation = isolation;
        this.concurrency = concurrency;
        this.timeout = timeout;
        this.lb = lb;
        this.primaryNodes = txNodes != null ? new ArrayList<>(txNodes.keySet()) : Collections.EMPTY_SET;
    }

    /**
     * @param node
     * @param tx
     * @return instance of TxDTO
     */
    public static TxDTO of(ClusterNode node, IgniteInternalTx tx) {
        String lb = (tx instanceof GridNearTxLocal) ? ((GridNearTxLocal)tx).label() : tx.getClass().getSimpleName();

        return new TxDTO(NodeDTO.of(node), tx.xid(), tx.state(), String.valueOf(tx.nearXidVersion()),
            tx.startTime(), tx.isolation(), tx.concurrency(), tx.timeout(), lb,
            tx.transactionNodes()
        );
    }

    public NodeDTO node() {
        return node;
    }

    public IgniteUuid xid() {
        return xid;
    }

    public TransactionIsolation isolation() {
        return isolation;
    }

    public TransactionConcurrency concurrency() {
        return concurrency;
    }

    public long timeout() {
        return timeout;
    }

    public Collection<UUID> primaryNodes() {
        return primaryNodes;
    }

    public TransactionState state() {
        return state;
    }

    public String nearXidVersion() {
        return nearXidVer;
    }

    public long duration() {
        return duration;
    }

    public String label() {
        return lb;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TxDTO dto = (TxDTO)o;
        return duration == dto.duration &&
            timeout == dto.timeout &&
            Objects.equals(node, dto.node) &&
            Objects.equals(xid, dto.xid) &&
            isolation == dto.isolation &&
            concurrency == dto.concurrency &&
            Objects.equals(lb, dto.lb) &&
            Objects.equals(primaryNodes, dto.primaryNodes) &&
            state == dto.state &&
            Objects.equals(nearXidVer, dto.nearXidVer);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(node, xid, duration, isolation, concurrency, timeout, lb, primaryNodes, state, nearXidVer);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "TxDTO{" +
            "node=" + node +
            ",\n xid=" + xid +
            ", duration=" + duration +
            ", isolation=" + isolation +
            ", concurrency=" + concurrency +
            ", timeout=" + timeout +
            ", label=" + lb +
            ", state=" + state +
            ",\n primaryNodes=" + primaryNodes +
            ",\n nearXidVersion='" + nearXidVer + '\'' +
            "\n}";
    }

}

