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

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.AffinityVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinator;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Cache affinity manager.
 */
public class GridCacheAffinityManager extends GridCacheManagerAdapter {
    /** */
    private static final AffinityTopologyVersion LOC_CACHE_TOP_VER = new AffinityTopologyVersion(1, 1, 0);

    /** */
    private static final String FAILED_TO_FIND_CACHE_ERR_MSG = "Failed to find cache (cache was not started " +
        "yet or cache was already stopped): ";

    /** Affinity cached function. */
    private GridAffinityAssignmentCache aff;

    /** */
    private AffinityFunction affFunction;

    /** */
    private AffinityKeyMapper affMapper;

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        affFunction = cctx.config().getAffinity();
        affMapper = cctx.config().getAffinityMapper();

        aff = cctx.group().affinity();
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        if (cctx.isLocal())
            // No discovery event needed for local affinity.
            aff.calculate(LOC_CACHE_TOP_VER, null, null);
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel, boolean destroy) {
        aff = null;
    }

    /**
     * Gets affinity ready future, a future that will be completed after affinity with given
     * topology version is calculated.
     *
     * @param affVer Topology version to wait.
     * @return Affinity ready future.
     */
    public IgniteInternalFuture<AffinityVersion> affinityReadyFuture(AffinityVersion affVer) {
        assert !cctx.isLocal();

        IgniteInternalFuture<AffinityVersion> fut = aff.readyFuture(affVer);

        return fut != null ? fut : new GridFinishedFuture<>(aff.lastVersion());
    }

    /**
     * @param topVer Topology version.
     * @return Affinity assignments.
     */
    public List<List<ClusterNode>> assignments(AffinityTopologyVersion topVer) {
        if (cctx.isLocal())
            topVer = LOC_CACHE_TOP_VER;

        GridAffinityAssignmentCache aff0 = aff;

        if (aff0 == null)
            throw new IgniteException(FAILED_TO_FIND_CACHE_ERR_MSG + cctx.name());

        return aff0.assignments(topVer.affinityVersion());
    }

    /**
     * @return Assignment.
     */
    public List<List<ClusterNode>> idealAssignment() {
        assert !cctx.isLocal();

        return aff.idealAssignment();
    }

    /**
     * @return Partition count.
     */
    public int partitions() {
        GridAffinityAssignmentCache aff0 = aff;

        if (aff0 == null)
            throw new IgniteException(FAILED_TO_FIND_CACHE_ERR_MSG + cctx.name());

        return aff0.partitions();
    }

    /**
     * @param key Key.
     * @return Partition.
     */
    public int partition(Object key) {
        return partition(key, true);
    }

    /**
     * NOTE: Use this method always when you need to calculate partition id for
     * a key provided by user. It's required since we should apply affinity mapper
     * logic in order to find a key that will eventually be passed to affinity function.
     *
     * @param key Key.
     * @param useKeyPart If {@code true} can use pre-calculated partition stored in KeyCacheObject.
     * @return Partition.
     */
    public int partition(Object key, boolean useKeyPart) {
        GridAffinityAssignmentCache aff0 = aff;

        if (aff0 == null)
            throw new IgniteException(FAILED_TO_FIND_CACHE_ERR_MSG + cctx.name());

        if (useKeyPart && (key instanceof KeyCacheObject)) {
            int part = ((KeyCacheObject)key).partition();

            if (part != -1)
                return part;
        }

        return affFunction.partition(affinityKey(key));
    }

    /**
     * If Key is {@link GridCacheInternal GridCacheInternal} entry when won't passed into user's mapper and
     * will use {@link GridCacheDefaultAffinityKeyMapper default}.
     *
     * @param key Key.
     * @return Affinity key.
     */
    public Object affinityKey(Object key) {
        if (key instanceof CacheObject && !(key instanceof BinaryObject))
            key = ((CacheObject)key).value(cctx.cacheObjectContext(), false);

        return (key instanceof GridCacheInternal ? cctx.defaultAffMapper() : affMapper).affinityKey(key);
    }

    /**
     * @param key Key.
     * @param affVer Affinity version.
     * @return Affinity nodes.
     */
    public List<ClusterNode> nodesByKey(Object key, AffinityVersion affVer) {
        return nodesByPartition(partition(key), affVer);
    }

    /**
     * @param part Partition.
     * @param affVer Topology version.
     * @return Affinity nodes.
     */
    public List<ClusterNode> nodesByPartition(int part, AffinityVersion affVer) {
        if (cctx.isLocal())
            affVer = LOC_CACHE_TOP_VER.affinityVersion();

        GridAffinityAssignmentCache aff0 = aff;

        if (aff0 == null)
            throw new IgniteException(FAILED_TO_FIND_CACHE_ERR_MSG + cctx.name());

        return aff0.nodes(part, affVer);
    }

    /**
     * Get affinity assignment for the given topology version.
     *
     * @param affVer Topology version.
     * @return Affinity assignment.
     */
    public AffinityAssignment assignment(AffinityVersion affVer) {
        if (cctx.isLocal())
            affVer = LOC_CACHE_TOP_VER.affinityVersion();

        GridAffinityAssignmentCache aff0 = aff;

        if (aff0 == null)
            throw new IgniteException(FAILED_TO_FIND_CACHE_ERR_MSG + cctx.name());

        return aff0.cachedAffinity(affVer);
    }

    public MvccCoordinator mvccCoordinator(AffinityTopologyVersion topVer) {
        return assignment(topVer.affinityVersion()).mvccCoordinator();
    }

    /**
     * @param key Key to check.
     * @param affVer Topology version.
     * @return Primary node for given key.
     */
    @Nullable public ClusterNode primaryByKey(Object key, AffinityVersion affVer) {
        return primaryByPartition(partition(key), affVer);
    }

    /**
     * @param part Partition.
     * @param affVer Affinity version.
     * @return Primary node for given key.
     */
    @Nullable public ClusterNode primaryByPartition(int part, AffinityVersion affVer) {
        List<ClusterNode> nodes = nodesByPartition(part, affVer);

        if (nodes.isEmpty())
            return null;

        return nodes.get(0);
    }

    /**
     * @param n Node to check.
     * @param key Key to check.
     * @param affVer Affinity version.
     * @return {@code True} if checked node is primary for given key.
     */
    public boolean primaryByKey(ClusterNode n, Object key, AffinityVersion affVer) {
        return F.eq(primaryByKey(key, affVer), n);
    }

    /**
     * @param n Node to check.
     * @param part Partition.
     * @param affVer Affinity version.
     * @return {@code True} if checked node is primary for given partition.
     */
    public boolean primaryByPartition(ClusterNode n, int part, AffinityVersion affVer) {
        return F.eq(primaryByPartition(part, affVer), n);
    }

    /**
     * @param key Key to check.
     * @param affVer Affinity version.
     * @return Backup nodes.
     */
    public Collection<ClusterNode> backupsByKey(Object key, AffinityVersion affVer) {
        return backupsByPartition(partition(key), affVer);
    }

    /**
     * @param part Partition.
     * @param affVer Topology version.
     * @return Backup nodes.
     */
    private Collection<ClusterNode> backupsByPartition(int part, AffinityVersion affVer) {
        List<ClusterNode> nodes = nodesByPartition(part, affVer);

        assert !F.isEmpty(nodes);

        if (nodes.size() == 1)
            return Collections.emptyList();

        return F.view(nodes, F.notEqualTo(nodes.get(0)));
    }

    /**
     * @param n Node to check.
     * @param part Partition.
     * @param affVer Affinity version.
     * @return {@code True} if checked node is a backup node for given partition.
     */
    public boolean backupByPartition(ClusterNode n, int part, AffinityVersion affVer) {
        List<ClusterNode> nodes = nodesByPartition(part, affVer);

        assert !F.isEmpty(nodes);

        return nodes.indexOf(n) > 0;
    }

    /**
     * @param key Key to check.
     * @param affVer Topology version.
     * @return {@code true} if given key belongs to local node.
     */
    public boolean keyLocalNode(Object key, AffinityVersion affVer) {
        return partitionLocalNode(partition(key), affVer);
    }

    /**
     * @param part Partition number to check.
     * @param affVer Topology version.
     * @return {@code true} if given partition belongs to local node.
     */
    public boolean partitionLocalNode(int part, AffinityVersion affVer) {
        assert part >= 0 : "Invalid partition: " + part;

        return nodesByPartition(part, affVer).contains(cctx.localNode());
    }

    /**
     * @param node Node.
     * @param part Partition number to check.
     * @param affVer Affinity version.
     * @return {@code true} if given partition belongs to specified node.
     */
    public boolean partitionBelongs(ClusterNode node, int part, AffinityVersion affVer) {
        assert node != null;
        assert part >= 0 : "Invalid partition: " + part;

        return nodesByPartition(part, affVer).contains(node);
    }

    /**
     * @param nodeId Node ID.
     * @param affVer Affinity version to calculate affinity.
     * @return Partitions for which given node is primary.
     */
    public Set<Integer> primaryPartitions(UUID nodeId, AffinityVersion affVer) {
        if (cctx.isLocal())
            affVer = LOC_CACHE_TOP_VER.affinityVersion();

        GridAffinityAssignmentCache aff0 = aff;

        if (aff0 == null)
            throw new IgniteException(FAILED_TO_FIND_CACHE_ERR_MSG + cctx.name());

        return aff0.primaryPartitions(nodeId, affVer);
    }

    /**
     * @param nodeId Node ID.
     * @param affVer Affinity version to calculate affinity.
     * @return Partitions for which given node is backup.
     */
    public Set<Integer> backupPartitions(UUID nodeId, AffinityVersion affVer) {
        if (cctx.isLocal())
            affVer = LOC_CACHE_TOP_VER.affinityVersion();

        GridAffinityAssignmentCache aff0 = aff;

        if (aff0 == null)
            throw new IgniteException(FAILED_TO_FIND_CACHE_ERR_MSG + cctx.name());

        return aff0.backupPartitions(nodeId, affVer);
    }

    /**
     * @return Affinity-ready topology version.
     */
    public AffinityVersion affinityVersion() {
        GridAffinityAssignmentCache aff0 = aff;

        if (aff0 == null)
            throw new IgniteException(FAILED_TO_FIND_CACHE_ERR_MSG + cctx.name());

        return aff0.lastVersion();
    }

    /**
     * @param part Partition.
     * @param startVer Start version.
     * @param endVer End version.
     * @return {@code True} if primary changed or required affinity version not found in history.
     */
    public boolean primaryChanged(int part, AffinityVersion startVer, AffinityVersion endVer) {
        assert !cctx.isLocal() : cctx.name();

        GridAffinityAssignmentCache aff0 = aff;

        if (aff0 == null)
            throw new IgniteException(FAILED_TO_FIND_CACHE_ERR_MSG + cctx.name());

        return aff0.primaryChanged(part, startVer, endVer);
    }
}
