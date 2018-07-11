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
package org.apache.ignite.internal.processors.cache.verify;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskArg;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Task for collection checksums primary and backup partitions of specified caches.
 * <br> Argument: Set of cache names, 'null' will trigger verification for all caches.
 * <br> Result: {@link IdleVerifyDumpResult} with all found partitions.
 * <br> Works properly only on idle cluster - there may be false positive conflict reports if data in cluster is being
 * concurrently updated.
 */
@GridInternal
public class VerifyBackupPartitionsDumpTask extends ComputeTaskAdapter<VisorIdleVerifyTaskArg, String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Delegate for map execution */
    private final VerifyBackupPartitionsTask delegate = new VerifyBackupPartitionsTask();

    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid, VisorIdleVerifyTaskArg arg) throws IgniteException {
        return delegate.map(subgrid, arg.getCaches());
    }

    /** {@inheritDoc} */
    @Nullable @Override public String reduce(List<ComputeJobResult> results)
        throws IgniteException {
        Map<PartitionKey, List<PartitionHashRecord>> clusterHashes = new TreeMap<>(buildPartitionKeyComparator());

        for (ComputeJobResult res : results) {
            Map<PartitionKey, PartitionHashRecord> nodeHashes = res.getData();

            for (Map.Entry<PartitionKey, PartitionHashRecord    > e : nodeHashes.entrySet()) {
                clusterHashes
                    .computeIfAbsent(e.getKey(), k -> new ArrayList<>())
                    .add(e.getValue());
            }
        }

        Comparator<PartitionHashRecord> recordV2Comparator = buildRecordComparator().reversed();

        Map<PartitionKey, List<PartitionHashRecord>> partitions = new LinkedHashMap<>();

        for (Map.Entry<PartitionKey, List<PartitionHashRecord>> entry : clusterHashes.entrySet()) {
            entry.getValue().sort(recordV2Comparator);

            partitions.put(entry.getKey(), entry.getValue());
        }

        IdleVerifyDumpResult res = new IdleVerifyDumpResult(partitions);

        return writeHashes(res);
    }

    /**
     * @param res Dump result.
     * @return Path where results are written.
     * @throws IgniteException If failed to write the file.
     */
    private String writeHashes(IdleVerifyDumpResult res) throws IgniteException {
        File workDir = ignite.configuration().getWorkDirectory() == null ?
            new File("/tmp") :
            new File(ignite.configuration().getWorkDirectory());

        File out = new File(workDir, "idle-dump-" + System.currentTimeMillis() + ".txt");

        ignite.log().info("IdleVerifyDumpTask will write output to " + out.getAbsolutePath());

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(out))) {
            try {
                if (!F.isEmpty(res.clusterHashes())) {
                    writer.write("idle_verify check has finished, found " + res.clusterHashes().size() + " partitions\n");

                    writer.write("Cluster partitions:\n");

                    for (Map.Entry<PartitionKey, List<PartitionHashRecord>> entry : res.clusterHashes().entrySet()) {
                        writer.write("Partition: " + entry.getKey() + "\n");

                        writer.write("Partition instances: " + entry.getValue() + "\n");
                    }
                }
                else
                    writer.write("No partitions found.\n");
            }
            finally {
                writer.flush();
            }

            ignite.log().info("IdleVerifyDumpTask successfully written dump to '" + out.getAbsolutePath() + "'");
        }
        catch (IOException e) {
            ignite.log().error("Failed to write dump file: " + out.getAbsolutePath(), e);

            throw new IgniteException(e);
        }

        return out.getAbsolutePath();
    }

    /**
     * @return Comparator for {@link PartitionHashRecord}.
     */
    @NotNull private Comparator<PartitionHashRecord> buildRecordComparator() {
        return (o1, o2) -> {
            int compare = Boolean.compare(o1.isPrimary(), o2.isPrimary());

            if (compare != 0)
                return compare;

            return o1.consistentId().toString().compareTo(o2.consistentId().toString());
        };
    }

    /**
     * @return Comparator for {@link PartitionKey}.
     */
    @NotNull private Comparator<PartitionKey> buildPartitionKeyComparator() {
        return (o1, o2) -> {
            int compare = Integer.compare(o1.groupId(), o2.groupId());

            if (compare != 0)
                return compare;

            return Integer.compare(o1.partitionId(), o2.partitionId());
        };
    }
}
