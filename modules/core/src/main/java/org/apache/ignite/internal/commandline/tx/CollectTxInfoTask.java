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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

/**
 * Task collecting information about transactions in the cluster
 */
public class CollectTxInfoTask extends ComputeTaskAdapter<CollectTxInfoArguments, Collection<TxDTO>> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default constructor.
     */
    public CollectTxInfoTask() {
    }

    /** {@inheritDoc} */
    @Nullable public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> nodes,
        @Nullable CollectTxInfoArguments arg) throws IgniteException {
        Map<Job, ClusterNode> map = new HashMap<>();


        for (ClusterNode node : nodes) {
            if ((!arg.clients() && arg.consistentIds() == null) //All
                || (arg.clients() && (node.isClient() && !node.isDaemon())) //Clients
                || (arg.consistentIds() != null && arg.consistentIds().contains(node.consistentId().toString()))) //Consistent ids
                map.put(new Job(arg), node);
        }

        return map;
    }

    /** {@inheritDoc} */
    @Nullable public Collection<TxDTO> reduce(List<ComputeJobResult> results) throws IgniteException {
        List<TxDTO> txInfos = new ArrayList<>();

        for (ComputeJobResult result : results)
            txInfos.addAll(result.<Collection<TxDTO>>getData());

        return txInfos;
    }

    /**
     * Retrieving transactions from a node
     */
    public static class Job extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final long durationMillis;
        /** */
        private final long size;
        /** */
        private final Pattern lbPat;
        /** */
        @IgniteInstanceResource
        private IgniteEx ignite;
        /** */
        @LoggerResource
        private IgniteLogger log;

        /**
         * @param arg Argument.
         */
        public Job(CollectTxInfoArguments arg) {
            this.durationMillis = arg.duration() * 1000;
            this.size = arg.size();
            this.lbPat = arg.labelPattern();
        }

        /** {@inheritDoc} */
        public Object execute() throws IgniteException {
            Collection<IgniteInternalTx> activeTxs = ignite.context().cache().context().tm().activeTransactions();

            if (activeTxs.isEmpty())
                return Collections.emptySet();

            log.info("jobNodeId = " + ignite.cluster().localNode().id());

            long curTime = System.currentTimeMillis();

            Set<TxDTO> txInfos = new HashSet<>();

            for (IgniteInternalTx tx0 : activeTxs) {
                if (!(tx0 instanceof GridNearTxLocal))
                    continue;

                GridNearTxLocal tx = (GridNearTxLocal)tx0;

                if (durationMillis > 0 && (curTime - tx.startTime()) < durationMillis) {
                    if (log.isTraceEnabled()) {
                        ClusterNode node = ignite.context().discovery().node(tx.nodeId());
                        log.trace("filtered out by " + durationMillis + " duration: " + TxDTO.of(node, tx));
                    }
                    continue;
                }

                if (size > 0 && tx.size() < size) {
                    if (log.isTraceEnabled()) {
                        ClusterNode node = ignite.context().discovery().node(tx.nodeId());
                        log.trace("filtered out by " + size + " size: " + TxDTO.of(node, tx));
                    }
                    continue;
                }

                if (lbPat != null && !lbPat.matcher(tx.label()).matches()) {
                    if (log.isTraceEnabled()) {
                        ClusterNode node = ignite.context().discovery().node(tx.nodeId());
                        log.trace("filtered out by " + lbPat.pattern() + " label: " + TxDTO.of(node, tx));
                    }
                    continue;
                }

                ClusterNode node = ignite.context().discovery().node(tx.nodeId());
                txInfos.add(TxDTO.of(node, tx));

            }

            return txInfos;
        }
    }
}
