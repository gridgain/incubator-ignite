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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.metadata.OptimisticPlanningException;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class QueryMappings {
    /** */
    private AffinityTopologyVersion version;

    /** */
    private Map<Long, NodesMapping> mappings;

    /** */
    public Map<Long, NodesMapping> map(MappingService mappingService, PlanningContext ctx, List<Fragment> fragments) {
        AffinityTopologyVersion version;
        Map<Long, NodesMapping> mappings;

        synchronized (this) {
            mappings = this.mappings;
            version = this.version;
        }

        if (mappings != null && Objects.equals(version, ctx.topologyVersion()))
            return mappings;

        mappings = U.newHashMap(fragments.size());

        RelMetadataQuery mq = F.first(fragments).root().getCluster().getMetadataQuery();

        boolean save = true;
        for (int i = 0, j = 0; i < fragments.size();) {
            Fragment fragment = fragments.get(i);

            try {
                mappings.put(fragment.fragmentId(), fragment.map(mappingService, ctx, mq));

                i++;
            }
            catch (OptimisticPlanningException e) {
                save = false; // we mustn't save mappings for mutated fragments

                if (++j > 3)
                    throw new IgniteSQLException("Failed to map query.", e);

                replace(fragments, fragment, new FragmentSplitter(e.node()).go(fragment));

                // restart init routine.
                mappings.clear();
                i = 0;
            }
        }

        if (save) {
            synchronized (this) {
                if (this.version == version) {
                    this.mappings = mappings;
                    this.version = ctx.topologyVersion();
                }
            }
        }

        return mappings;
    }

    /** */
    private void replace(List<Fragment> fragments, Fragment fragment, List<Fragment> replacement) {
        assert !F.isEmpty(replacement);

        Map<Long, Long> newTargets = new HashMap<>();

        for (Fragment fragment0 : replacement) {
            for (IgniteReceiver remote : fragment0.remotes())
                newTargets.put(remote.exchangeId(), fragment0.fragmentId());
        }

        for (int i = 0; i < fragments.size(); i++) {
            Fragment fragment0 = fragments.get(i);

            if (fragment0 == fragment)
                fragments.set(i, F.first(replacement));
            else if (!fragment0.local()) {
                IgniteSender sender = (IgniteSender)fragment0.root();
                Long newTargetId = newTargets.get(sender.exchangeId());

                if (newTargetId != null)
                    sender.targetFragmentId(newTargetId);
            }
        }

        fragments.addAll(replacement.subList(1, replacement.size()));
    }
}
