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
package org.apache.ignite.internal.sql.optimizer.affinity;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Client context. Passed to partition resolver on thin clients.
 */
public class PartitionClientContext {
    /** Number of partitions. */
    private int parts;

    /** Mask to use in calculation when partitions count is power of 2. */
    private int mask;

    public PartitionClientContext(int parts) {
        assert parts <= CacheConfiguration.MAX_PARTITIONS_COUNT;
        assert parts > 0;

        this.parts = parts;

        mask = (parts & (parts - 1)) == 0 ? parts - 1 : -1;
    }

    /**
     * Resolve partition.
     *
     * @param arg Argument.
     * @param typ Type.
     * @return Partition or {@code null} if cannot be resolved.
     */
    @Nullable public Integer partition(Object arg, @Nullable PartitionParameterType typ) {
        Object key = PartitionDataTypeUtils.convert(arg, typ);

        if (mask >= 0) {
            int h;
            return ((h = key.hashCode()) ^ (h >>> 16)) & mask;
        }

        return U.safeAbs(key.hashCode() % parts);
    }
}