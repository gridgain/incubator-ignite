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

package org.apache.ignite.internal.visor.tx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
@GridInternal
public class VisorTxTask extends VisorMultiNodeTask<VisorTxTaskArg, Map<VisorTxNodeInfo, VisorTxTaskResult>, VisorTxTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorTxTaskArg, VisorTxTaskResult> job(VisorTxTaskArg arg) {
        return new VisorTxJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<VisorTxTaskArg> arg) {
        final VisorTxTaskArg taskArg = arg.getArgument();

        if (taskArg.getConsistentIds() != null) {
            return F.transform(ignite.cluster().forPredicate(new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    return taskArg.getConsistentIds().contains((String)node.consistentId());
                }
            }).nodes(), new IgniteClosure<ClusterNode, UUID>() {
                @Override public UUID apply(ClusterNode node) {
                    return node.id();
                }
            });
        }

        if (taskArg.getProjection() == VisorTxProjection.SERVER) {
            return F.transform(ignite.cluster().forServers().nodes(), new IgniteClosure<ClusterNode, UUID>() {
                @Override public UUID apply(ClusterNode node) {
                    return node.id();
                }
            });
        }

        if (taskArg.getProjection() == VisorTxProjection.CLIENT) {
            return F.transform(ignite.cluster().forClients().nodes(), new IgniteClosure<ClusterNode, UUID>() {
                @Override public UUID apply(ClusterNode node) {
                    return node.id();
                }
            });
        }

        return F.transform(ignite.cluster().nodes(), new IgniteClosure<ClusterNode, UUID>() {
            @Override public UUID apply(ClusterNode node) {
                return node.id();
            }
        });
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<VisorTxNodeInfo, VisorTxTaskResult> reduce0(List<ComputeJobResult> results) throws IgniteException {
        Map<VisorTxNodeInfo, VisorTxTaskResult> mapRes = new TreeMap<>();

        for (ComputeJobResult result : results) {
            VisorTxTaskResult data = result.getData();

            if (data.getInfos().isEmpty())
                continue;

            mapRes.put(new VisorTxNodeInfo(result.getNode()), data);
        }

        return mapRes;
    }

    /**
     *
     */
    private static class VisorTxJob extends VisorJob<VisorTxTaskArg, VisorTxTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        public static final int DEFAULT_LIMIT = 50;

        /**
         * @param arg Formal job argument.
         * @param debug Debug flag.
         */
        private VisorTxJob(VisorTxTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorTxTaskResult run(@Nullable VisorTxTaskArg arg) throws IgniteException {
            if (arg == null)
                return new VisorTxTaskResult(Collections.emptyList());

            Collection<Transaction> transactions = ignite.transactions().localActiveTransactions();

            List<VisorTxInfo> infos = new ArrayList<>();

            int limit = arg.getLimit() == null ? DEFAULT_LIMIT : arg.getLimit();

            Pattern lbMatch = null;

            if (arg.getLabelRegex() != null) {
                try {
                    lbMatch = Pattern.compile(arg.getLabelRegex());
                }
                catch (PatternSyntaxException ignored) {
                    // No-op.
                }
            }

            IgniteUuid kill = null;

            if (arg.getKillXid() != null)
                kill = IgniteUuid.fromString(arg.getKillXid());

            for (Transaction transaction : transactions) {
                GridNearTxLocal locTx = ((TransactionProxyImpl)transaction).tx();

                if (kill != null && !kill.equals(transaction.xid()))
                    continue;

                if (arg.getState() != null && locTx.state() != arg.getState())
                    continue;

                long duration = U.currentTimeMillis() - transaction.startTime();

                if (arg.getMinDuration() != null &&
                    duration < arg.getMinDuration())
                    continue;

                if (arg.getMinSize() != null && locTx.size() < arg.getMinSize())
                    continue;

                if (lbMatch != null && (locTx.label() == null || !lbMatch.matcher(locTx.label()).matches()))
                    continue;

                Collection<UUID> mappings = new ArrayList<>();

                int size = 0;

                if (locTx.mappings() != null) {
                    for (GridDistributedTxMapping mapping : locTx.mappings().mappings()) {
                        mappings.add(mapping.primary().id());

                        size += mapping.entries().size(); // Entries are not synchronized so no visibility guaranties.
                    }
                }

                infos.add(new VisorTxInfo(locTx.xid(), duration, locTx.isolation(), locTx.concurrency(),
                    locTx.timeout(), locTx.label(), mappings, locTx.state(), size));

                if (kill != null)
                    locTx.rollbackAsync();

                if (infos.size() == limit)
                    break;
            }

            Comparator<VisorTxInfo> comp = TxDurationComparator.INSTANCE;

            if (arg.getSortOrder() != null) {
                switch (arg.getSortOrder()) {
                    case DURATION:
                        comp = TxDurationComparator.INSTANCE;

                        break;

                    case SIZE:
                        comp = TxSizeComparator.INSTANCE;

                        break;

                    default:
                }
            }

            Collections.sort(infos, comp);

            return new VisorTxTaskResult(infos);
        }
    }

    /**
     *
     */
    private static class TxDurationComparator implements Comparator<VisorTxInfo> {
        /** Instance. */
        public static final TxDurationComparator INSTANCE = new TxDurationComparator();

        /** {@inheritDoc} */
        @Override public int compare(VisorTxInfo o1, VisorTxInfo o2) {
            return Long.compare(o2.getDuration(), o1.getDuration());
        }
    }

    /**
     *
     */
    private static class TxSizeComparator implements Comparator<VisorTxInfo> {
        /** Instance. */
        public static final TxSizeComparator INSTANCE = new TxSizeComparator();

        /** {@inheritDoc} */
        @Override public int compare(VisorTxInfo o1, VisorTxInfo o2) {
            return Long.compare(o2.getSize(), o1.getSize());
        }
    }
}
