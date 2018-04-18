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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Task collecting information about transactions in the cluster
 */
public class TransactionsTask extends ComputeTaskAdapter<TransactionsTaskArguments, Collection<TxDTO>> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default constructor.
     */
    public TransactionsTask() {
    }

    /** {@inheritDoc} */
    @Nullable public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> nodes,
        @Nullable TransactionsTaskArguments arg) throws IgniteException {
        Map<ComputeJobAdapter, ClusterNode> map = new HashMap<>();

        for (ClusterNode node : nodes) {
            if ((!arg.clients() && arg.consistentIds() == null) //All
                || (arg.clients() && (node.isClient() && !node.isDaemon())) //Clients
                || (arg.consistentIds() != null && arg.consistentIds().contains(node.consistentId().toString()))) //Consistent ids
                map.put(createJob(arg), node);
        }

        return map;
    }

    @NotNull public ComputeJobAdapter createJob(@NotNull TransactionsTaskArguments arg) {
        return arg.stopXid() == null ? new CollectTxJob(arg) : new StopTxJob(arg);
    }

    /** {@inheritDoc} */
    @Nullable public Collection<TxDTO> reduce(List<ComputeJobResult> results) throws IgniteException {
        List<TxDTO> txInfos = new ArrayList<>();

        for (ComputeJobResult result : results)
            txInfos.addAll(result.<Collection<TxDTO>>getData());

        return txInfos;
    }

}
