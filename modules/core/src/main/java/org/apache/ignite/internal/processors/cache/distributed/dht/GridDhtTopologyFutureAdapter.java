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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_ALL;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_SAFE;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_ALL;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFutureAdapter.OperationType.WRITE;

/**
 *
 */
public abstract class GridDhtTopologyFutureAdapter extends GridFutureAdapter<AffinityTopologyVersion>
    implements GridDhtTopologyFuture {
    /** Cache groups validation results. */
    protected volatile Map<Integer, CacheValidation> grpValidRes = Collections.emptyMap();

    /** Whether or not cluster is active. */
    protected volatile boolean clusterIsActive = true;

    /**
     * @param grp Cache group.
     * @param topNodes Topology nodes.
     * @return Validation result.
     */
    protected final CacheValidation validateCacheGroup(CacheGroupContext grp, Collection<ClusterNode> topNodes) {
        Collection<Integer> lostParts = grp.isLocal() ?
            Collections.<Integer>emptyList() : grp.topology().lostPartitions();

        boolean valid = true;

        if (!grp.systemCache()) {
            TopologyValidator validator = grp.topologyValidator();

            if (validator != null)
                valid = validator.validate(topNodes);
        }

        return new CacheValidation(valid, lostParts);
    }

    /** {@inheritDoc} */
    @Nullable @Override public final Throwable validateCache(
        GridCacheContext cctx,
        boolean recovery,
        boolean read,
        @Nullable Object key,
        @Nullable Collection<?> keys
    ) {
        assert isDone() : this;

        Throwable err = error();

        if (err != null)
            return err;

        if (!clusterIsActive)
            return new CacheInvalidStateException(
                "Failed to perform cache operation (cluster is not activated): " + cctx.name());

        CacheGroupContext grp = cctx.group();

        PartitionLossPolicy partLossPlc = grp.config().getPartitionLossPolicy();

        if (grp.needsRecovery() && !recovery) {
            if (!read && (partLossPlc == READ_ONLY_SAFE || partLossPlc == READ_ONLY_ALL))
                return new IgniteCheckedException("Failed to write to cache (cache is moved to a read-only state): " +
                    cctx.name());
        }

        if (cctx.shared().readOnlyMode() && !read)
            return new IgniteCheckedException("Failed to perform cache operation (cluster is in read only mode)" );

        if (grp.needsRecovery() || grp.topologyValidator() != null) {
            CacheValidation validation = grpValidRes.get(grp.groupId());

            if (validation == null)
                return null;

            if (!validation.valid && !read)
                return new IgniteCheckedException("Failed to perform cache operation " +
                    "(cache topology is not valid): " + cctx.name());

            if (recovery || !grp.needsRecovery())
                return null;

            if (key != null) {
                int p = cctx.affinity().partition(key);

                CacheInvalidStateException ex = validatePartitionOperation(cctx.name(), read, key, p,
                    validation.lostParts, partLossPlc);

                if (ex != null)
                    return ex;
            }

            if (keys != null) {
                for (Object k : keys) {
                    int p = cctx.affinity().partition(k);

                    CacheInvalidStateException ex = validatePartitionOperation(cctx.name(), read, k, p,
                        validation.lostParts, partLossPlc);

                    if (ex != null)
                        return ex;
                }
            }
        }

        return null;
    }

    /**
     * @param cacheName Cache name.
     * @param read Read flag.
     * @param key Key to check.
     * @param part Partition this key belongs to.
     * @param lostParts Collection of lost partitions.
     * @param plc Partition loss policy.
     * @return Invalid state exception if this operation is disallowed.
     */
    private CacheInvalidStateException validatePartitionOperation(
        String cacheName,
        boolean read,
        Object key,
        int part,
        Collection<Integer> lostParts,
        PartitionLossPolicy plc
    ) {
        if (lostParts.contains(part)) {
            if (!read) {
                assert plc == READ_WRITE_ALL || plc == READ_WRITE_SAFE;

                if (plc == READ_WRITE_SAFE) {
                    return new CacheInvalidStateException("Failed to execute cache operation " +
                        "(all partition owners have left the grid, partition data has been lost) [" +
                        "cacheName=" + cacheName + ", part=" + part + ", key=" + key + ']');
                }
            }
            else {
                // Read.
                if (plc == READ_ONLY_SAFE || plc == READ_WRITE_SAFE)
                    return new CacheInvalidStateException("Failed to execute cache operation " +
                        "(all partition owners have left the grid, partition data has been lost) [" +
                        "cacheName=" + cacheName + ", part=" + part + ", key=" + key + ']');
            }
        }

        return null;
    }

    /**
     * Cache validation result.
     */
    protected static class CacheValidation {
        /** Topology validation result. */
        private boolean valid;

        /** Lost partitions on this topology version. */
        private Collection<Integer> lostParts;

        /**
         * @param valid Valid flag.
         * @param lostParts Lost partitions.
         */
        private CacheValidation(boolean valid, Collection<Integer> lostParts) {
            this.valid = valid;
            this.lostParts = lostParts;
        }
    }

    /**
     *
     */
    public enum OperationType {
        /**
         * Read operation.
         */
        READ,
        /**
         * Write operation.
         */
        WRITE
    }

    /**
     * Lost policy validator.
     */
    public static class LostPolicyValidator {
        /**
         *
         */
        public static Throwable validate(
            GridCacheContext cctx,
            Object key,
            OperationType opType,
            Collection<Integer> lostParts
        ) {
            CacheGroupContext grp = cctx.group();

            PartitionLossPolicy lostPlc = grp.config().getPartitionLossPolicy();

            int partition = cctx.affinity().partition(key);

            return validate(cctx, key, partition, opType, lostPlc, lostParts);
        }

        /**
         *
         */
        public static Throwable validate(
            GridCacheContext cctx,
            Collection<?> keys,
            OperationType opType,
            Collection<Integer> lostParts
        ) {
            CacheGroupContext grp = cctx.group();

            PartitionLossPolicy lostPlc = grp.config().getPartitionLossPolicy();

            for (Object key : keys) {
                int partition = cctx.affinity().partition(key);

                Throwable res = validate(cctx, key, partition, opType, lostPlc, lostParts);

                if (res != null)
                    return res;
            }

            return null;
        }

        /**
         *
         */
        private static Throwable validate(
            GridCacheContext cctx,
            Object key,
            int partition,
            OperationType opType,
            PartitionLossPolicy lostPlc,
            Collection<Integer> lostParts
        ) {
            if (opType == WRITE) {
                if (lostPlc == READ_ONLY_SAFE || lostPlc == READ_ONLY_ALL) {
                    return new IgniteCheckedException(
                        "Failed to write to cache (cache is moved to a read-only state): " + cctx.name()
                    );
                }

                if (lostParts.contains(partition) && lostPlc == READ_WRITE_SAFE) {
                    return new CacheInvalidStateException("Failed to execute cache operation " +
                        "(all partition owners have left the grid, partition data has been lost) [" +
                        "cacheName=" + cctx.name() + ", part=" + partition + ", key=" + key + ']');
                }
            }

            if (opType == OperationType.READ) {
                if (lostParts.contains(partition) && (lostPlc == READ_ONLY_SAFE || lostPlc == READ_WRITE_SAFE))
                    return new CacheInvalidStateException("Failed to execute cache operation " +
                        "(all partition owners have left the grid, partition data has been lost) [" +
                        "cacheName=" + cctx.name() + ", part=" + partition + ", key=" + key + ']'
                    );
            }

            return null;
        }
    }
}
