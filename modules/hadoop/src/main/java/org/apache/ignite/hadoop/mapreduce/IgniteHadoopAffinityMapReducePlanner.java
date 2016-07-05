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

package org.apache.ignite.hadoop.mapreduce;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.hadoop.HadoopFileBlock;
import org.apache.ignite.internal.processors.hadoop.HadoopInputSplit;
import org.apache.ignite.internal.processors.hadoop.HadoopJob;
import org.apache.ignite.internal.processors.hadoop.HadoopMapReducePlan;
import org.apache.ignite.internal.processors.hadoop.HadoopUtils;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsEndpoint;
import org.apache.ignite.internal.processors.hadoop.planner.HadoopAbstractMapReducePlanner;
import org.apache.ignite.internal.processors.hadoop.planner.HadoopDefaultMapReducePlan;
import org.apache.ignite.internal.processors.hadoop.planner.HadoopMapReducePlanGroup;
import org.apache.ignite.internal.processors.hadoop.planner.HadoopMapReducePlanTopology;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.ignite.IgniteFileSystem.IGFS_SCHEME;

/**
 * Map-reduce planner which tries to assign map jobs to affinity nodes.
 */
// TODO: Docs.
public class IgniteHadoopAffinityMapReducePlanner extends HadoopAbstractMapReducePlanner {
    /** Default local mapper weight. */
    public static final int DFLT_LOC_MAPPER_WEIGHT = 100;

    /** Default remote mapper weight. */
    public static final int DFLT_RMT_MAPPER_WEIGHT = 120;

    /** Default local reducer weight. */
    public static final int DFLT_LOC_REDUCER_WEIGHT = 100;

    /** Default remote reducer weight. */
    public static final int DFLT_RMT_REDUCER_WEIGHT = 120;

    /** Default reducer migration threshold weight. */
    public static final int DFLT_REDUCER_MIGRATION_THRESHOLD_WEIGHT = 1000;

    /** Local mapper weight. */
    private int locMapperWeight = DFLT_LOC_MAPPER_WEIGHT;

    /** Remote mapper weight. */
    private int rmtMapperWeight = DFLT_RMT_MAPPER_WEIGHT;

    /** Local reducer weight. */
    private int locReducerWeight = DFLT_LOC_REDUCER_WEIGHT;

    /** Remote reducer weight. */
    private int rmtReducerWeight = DFLT_RMT_REDUCER_WEIGHT;

    /** Reducer migration threshold weight. */
    private int reducerMigrationThresholdWeight = DFLT_REDUCER_MIGRATION_THRESHOLD_WEIGHT;

    /** {@inheritDoc} */
    @Override public HadoopMapReducePlan preparePlan(HadoopJob job, Collection<ClusterNode> nodes,
        @Nullable HadoopMapReducePlan oldPlan) throws IgniteCheckedException {
        List<HadoopInputSplit> splits = HadoopUtils.sortInputSplits(job.input());
        int reducerCnt = job.info().reducers();

        if (reducerCnt < 0)
            throw new IgniteCheckedException("Number of reducers must be non-negative, actual: " + reducerCnt);

        HadoopMapReducePlanTopology top = topology(nodes);

        Map<UUID, Collection<HadoopInputSplit>> mappers = assignMappers(splits, top);

        Map<UUID, int[]> reducers = assignReducers(top, mappers, reducerCnt);

        return new HadoopDefaultMapReducePlan(mappers, reducers);
    }

    /**
     * Assign mappers to nodes.
     *
     * @param splits Input splits.
     * @param top Topology.
     * @return Mappers.
     * @throws IgniteCheckedException If failed.
     */
    private Map<UUID, Collection<HadoopInputSplit>> assignMappers(Collection<HadoopInputSplit> splits,
        HadoopMapReducePlanTopology top) throws IgniteCheckedException {
        Map<UUID, Collection<HadoopInputSplit>> res = new HashMap<>();

        for (HadoopInputSplit split : splits) {
            // Try getting IGFS affinity.
            Collection<UUID> nodeIds = affinityNodesForSplit(split, top);

            // Get best node.
            UUID node = bestMapperNode(nodeIds, top);

            // Add to result.
            Collection<HadoopInputSplit> nodeSplits = res.get(node);

            if (nodeSplits == null) {
                nodeSplits = new HashSet<>();

                res.put(node, nodeSplits);
            }

            nodeSplits.add(split);
        }

        return res;
    }

    /**
     * Get affinity nodes for the given input split.
     *
     * @param split Split.
     * @param top Topology.
     * @return Affintiy nodes.
     * @throws IgniteCheckedException If failed.
     */
    private Collection<UUID> affinityNodesForSplit(HadoopInputSplit split, HadoopMapReducePlanTopology top)
        throws IgniteCheckedException {
        Collection<UUID> nodeIds = igfsAffinityNodesForSplit(split);

        if (nodeIds == null) {
            nodeIds = new HashSet<>();

            for (String host : split.hosts()) {
                HadoopMapReducePlanGroup grp = top.groupForHost(host);

                if (grp != null) {
                    for (int i = 0; i < grp.nodeCount(); i++)
                        nodeIds.add(grp.node(i).id());
                }
            }
        }

        return nodeIds;
    }

    /**
     * Get IGFS affinity nodes for split if possible.
     *
     * @param split Input split.
     * @return IGFS affinity or {@code null} if IGFS is not available.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private Collection<UUID> igfsAffinityNodesForSplit(HadoopInputSplit split) throws IgniteCheckedException {
        if (split instanceof HadoopFileBlock) {
            HadoopFileBlock split0 = (HadoopFileBlock)split;

            if (IGFS_SCHEME.equalsIgnoreCase(split0.file().getScheme())) {
                HadoopIgfsEndpoint endpoint = new HadoopIgfsEndpoint(split0.file().getAuthority());

                IgfsEx igfs = null;

                if (F.eq(ignite.name(), endpoint.grid()))
                    igfs = (IgfsEx)((IgniteEx)ignite).igfsx(endpoint.igfs());

                if (igfs != null && !igfs.isProxy(split0.file())) {
                    IgfsPath path = new IgfsPath(split0.file());

                    if (igfs.exists(path)) {
                        Collection<IgfsBlockLocation> blocks;

                        try {
                            blocks = igfs.affinity(path, split0.start(), split0.length());
                        }
                        catch (IgniteException e) {
                            throw new IgniteCheckedException("Failed to get IGFS file block affinity [path=" + path +
                                ", start=" + split0.start() + ", len=" + split0.length() + ']', e);
                        }

                        assert blocks != null;

                        if (blocks.size() == 1)
                            return blocks.iterator().next().nodeIds();
                        else {
                            // The most "local" nodes go first.
                            Map<UUID, Long> idToLen = new HashMap<>();

                            for (IgfsBlockLocation block : blocks) {
                                for (UUID id : block.nodeIds()) {
                                    Long len = idToLen.get(id);

                                    idToLen.put(id, len == null ? block.length() : block.length() + len);
                                }
                            }

                            Map<NodeIdAndLength, UUID> res = new TreeMap<>();

                            for (Map.Entry<UUID, Long> idToLenEntry : idToLen.entrySet()) {
                                UUID id = idToLenEntry.getKey();

                                res.put(new NodeIdAndLength(id, idToLenEntry.getValue()), id);
                            }

                            return new HashSet<>(res.values());
                        }
                    }
                }
            }
        }

        return null;
    }

    /**
     * Find best mapper node.
     *
     * @param affIds Affinity node IDs.
     * @param top Topology.
     * @return Result.
     */
    private UUID bestMapperNode(@Nullable Collection<UUID> affIds, HadoopMapReducePlanTopology top) {
        // Priority node.
        UUID priorityAffId = F.first(affIds);

        // Find group with the least weight.
        HadoopMapReducePlanGroup resGrp = null;
        MapperPriority resPrio = MapperPriority.NORMAL;
        int resWeight = Integer.MAX_VALUE;

        for (HadoopMapReducePlanGroup grp : top.groups()) {
            MapperPriority priority = groupPriority(grp, affIds, priorityAffId);

            int weight = grp.mappersWeight() +
                (priority == MapperPriority.NORMAL ? rmtMapperWeight : locMapperWeight);

            if (resGrp == null || weight < resWeight || weight == resWeight && priority.value() > resPrio.value()) {
                resGrp = grp;
                resPrio = priority;
                resWeight = weight;
            }
        }

        assert resGrp != null;

        // Update group weight for further runs.
        resGrp.mappersWeight(resWeight);

        // Return the best node from the group.
        return bestMapperNodeForGroup(resGrp, resPrio, affIds, priorityAffId);
    }

    /**
     * Get best node in the group.
     *
     * @param grp Group.
     * @param priority Priority.
     * @param affIds Affinity IDs.
     * @param priorityAffId Priority affinitiy IDs.
     * @return Best node ID in the group.
     */
    private static UUID bestMapperNodeForGroup(HadoopMapReducePlanGroup grp, MapperPriority priority,
        @Nullable Collection<UUID> affIds, @Nullable UUID priorityAffId) {
        // Return the best node from the group.
        int idx = 0;

        // This is rare situation when several nodes are started on the same host.
        if (!grp.single()) {
            switch (priority) {
                case NORMAL: {
                    // Pick any node.
                    idx = ThreadLocalRandom.current().nextInt(grp.nodeCount());

                    break;
                }
                case HIGH: {
                    // Pick any affinity node.
                    assert affIds != null;

                    List<Integer> cands = new ArrayList<>();

                    for (int i = 0; i < grp.nodeCount(); i++) {
                        UUID id = grp.node(i).id();

                        if (affIds.contains(id))
                            cands.add(i);
                    }

                    idx = cands.get(ThreadLocalRandom.current().nextInt(cands.size()));

                    break;
                }
                default: {
                    // Find primary node.
                    assert priorityAffId != null;

                    for (int i = 0; i < grp.nodeCount(); i++) {
                        UUID id = grp.node(i).id();

                        if (F.eq(id, priorityAffId)) {
                            idx = i;

                            break;
                        }
                    }

                    assert priority == MapperPriority.HIGHEST;
                }
            }
        }

        return grp.node(idx).id();
    }

    /**
     * Generate reducers.
     *
     * @param top Topology.
     * @param mappers Mappers.
     * @param reducerCnt Reducer count.
     * @return Reducers.
     * @throws IgniteCheckedException If failed.
     */
    private Map<UUID, int[]> assignReducers(HadoopMapReducePlanTopology top, Map<UUID, Collection<HadoopInputSplit>> mappers,
        int reducerCnt) throws IgniteCheckedException {


        // TODO
        return null;
    }

    /**
     * Calculate group priority.
     *
     * @param grp Group.
     * @param affIds Affintiy IDs.
     * @param priorityAffId Priority affinity ID.
     * @return Group priority.
     */
    private static MapperPriority groupPriority(HadoopMapReducePlanGroup grp, @Nullable Collection<UUID> affIds,
        @Nullable UUID priorityAffId) {
        MapperPriority priority = MapperPriority.NORMAL;

        if (!F.isEmpty(affIds)) {
            for (int i = 0; i < grp.nodeCount(); i++) {
                UUID id = grp.node(i).id();

                if (affIds.contains(id)) {
                    priority = MapperPriority.HIGH;

                    if (F.eq(priorityAffId, id)) {
                        priority = MapperPriority.HIGHEST;

                        break;
                    }
                }
            }
        }

        return priority;
    }

    /**
     * Get local mapper weight.
     *
     * @return Remote mapper weight.
     */
    // TODO: Docs.
    public int getLocalMapperWeight() {
        return locMapperWeight;
    }

    /**
     * Set local mapper weight. See {@link #getLocalMapperWeight()} for more information.
     *
     * @param locMapperWeight Local mapper weight.
     */
    public void setLocalMapperWeight(int locMapperWeight) {
        this.locMapperWeight = locMapperWeight;
    }

    /**
     * Get remote mapper weight.
     *
     * @return Remote mapper weight.
     */
    // TODO: Docs.
    public int getRemoteMapperWeight() {
        return rmtMapperWeight;
    }

    /**
     * Set remote mapper weight. See {@link #getRemoteMapperWeight()} for more information.
     *
     * @param rmtMapperWeight Remote mapper weight.
     */
    public void setRemoteMapperWeight(int rmtMapperWeight) {
        this.rmtMapperWeight = rmtMapperWeight;
    }

    /**
     * Get local reducer weight.
     *
     * @return Local reducer weight.
     */
    // TODO: Docs.
    public int getLocalReducerWeight() {
        return locReducerWeight;
    }

    /**
     * Set local reducer weight. See {@link #getLocalReducerWeight()} for more information.
     *
     * @param locReducerWeight Local reducer weight.
     */
    public void setLocalReducerWeight(int locReducerWeight) {
        this.locReducerWeight = locReducerWeight;
    }

    /**
     * Get remote reducer weight.
     *
     * @return Remote reducer weight.
     */
    // TODO: Docs.
    public int getRemoteReducerWeight() {
        return rmtMapperWeight;
    }

    /**
     * Set remote reducer weight. See {@link #getRemoteReducerWeight()} for more information.
     *
     * @param rmtMapperWeight Remote reducer weight.
     */
    public void setRemoteReducerWeight(int rmtMapperWeight) {
        this.rmtMapperWeight = rmtMapperWeight;
    }

    /**
     * Get reducer migration threshold weight.
     *
     * @return Reducer migration threshold weight.
     */
    // TODO: Docs.
    public int getReducerMigrationThresholdWeight() {
        return reducerMigrationThresholdWeight;
    }

    /**
     * Set reducer migration threshold weight. See {@link #getReducerMigrationThresholdWeight()} for more information.
     *
     * @param reducerMigrationThresholdWeight Reducer migration threshold weight.
     */
    public void setReducerMigrationThresholdWeight(int reducerMigrationThresholdWeight) {
        this.reducerMigrationThresholdWeight = reducerMigrationThresholdWeight;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteHadoopAffinityMapReducePlanner.class, this);
    }

    /**
     * Node ID and length.
     */
    private static class NodeIdAndLength implements Comparable<NodeIdAndLength> {
        /** Node ID. */
        private final UUID id;

        /** Length. */
        private final long len;

        /**
         * Constructor.
         *
         * @param id Node ID.
         * @param len Length.
         */
        public NodeIdAndLength(UUID id, long len) {
            this.id = id;
            this.len = len;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("NullableProblems")
        @Override public int compareTo(NodeIdAndLength obj) {
            long res = len - obj.len;

            if (res > 0)
                return 1;
            else if (res < 0)
                return -1;
            else
                return id.compareTo(obj.id);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof NodeIdAndLength && F.eq(id, ((NodeIdAndLength)obj).id);
        }
    }

    /**
     * Mapper priority enumeration.
     */
    private enum MapperPriority {
        /** Normal node. */
        NORMAL(0),

        /** Affinity node. */
        HIGH(1),

        /** Node with the highest priority (e.g. because it hosts more data than other nodes). */
        HIGHEST(2);

        /** Value. */
        private final int val;

        /**
         * Constructor.
         *
         * @param val Value.
         */
        MapperPriority(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }
    }
}