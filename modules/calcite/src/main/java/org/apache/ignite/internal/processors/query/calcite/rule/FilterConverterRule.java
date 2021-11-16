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

package org.apache.ignite.internal.processors.query.calcite.rule;

import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql2rel.DeduplicateCorrelateVariables;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class FilterConverterRule extends AbstractIgniteConverterRule<LogicalFilter> {
    /** */
    public static final RelOptRule INSTANCE = new FilterConverterRule();

    /** */
    public FilterConverterRule() {
        super(LogicalFilter.class, "FilterConverterRule");
    }

    /** {@inheritDoc} */
    @Override protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalFilter rel) {
        RelOptCluster cluster = rel.getCluster();
        RelTraitSet traits = rel.getTraitSet().replace(IgniteConvention.INSTANCE);

        Set<CorrelationId> corrIds = RexUtils.extractCorrelationIds(rel.getCondition());

        // TODO: remove all near 'if' scope after https://issues.apache.org/jira/browse/CALCITE-4673 will be merged.
        if (corrIds.size() > 1) {
            final List<CorrelationId> correlNames = U.arrayList(corrIds);

            RelNode rel0 = DeduplicateCorrelateVariables.go(cluster.getRexBuilder(), correlNames.get(0), Util.skip(correlNames), rel);

            corrIds = RelOptUtil.getVariablesUsed(rel0);

            assert corrIds.size() == 1 : "Multiple correlates are applied: " + corrIds;

            rel = (LogicalFilter)rel0;
        }

        corrIds = RexUtils.extractCorrelationIds(rel.getCondition());

        if (!corrIds.isEmpty())
            traits = traits.replace(CorrelationTrait.correlations(corrIds));

        return new IgniteFilter(cluster, traits, rel.getInput(), rel.getCondition());
    }
}
