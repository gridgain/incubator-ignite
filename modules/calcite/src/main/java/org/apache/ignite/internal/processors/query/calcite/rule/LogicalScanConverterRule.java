/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.rule;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;

/** */
public abstract class LogicalScanConverterRule<T extends ProjectableFilterableTableScan>
    extends AbstractIgniteConverterRule<ProjectableFilterableTableScan> {
    /** Instance. */
    public static final LogicalScanConverterRule<IgniteIndexScan> LOGICAL_TO_INDEX_SCAN =
        new LogicalScanConverterRule<IgniteIndexScan>(IgniteLogicalIndexScan.class, IgniteIndexScan.class,
            "LogicalConverterIndexScanRule") {
            /** {@inheritDoc} */
            @Override protected IgniteIndexScan createNode(ProjectableFilterableTableScan rel, RelTraitSet traits) {
                return new IgniteIndexScan(rel.getCluster(), traits, rel.getTable(), rel.indexName(),
                    rel.projects(), rel.condition(), rel.requiredColunms());
            }
        };

    /** Instance. */
    public static final LogicalScanConverterRule<IgniteTableScan> LOGICAL_TO_TABLE_SCAN =
        new LogicalScanConverterRule<IgniteTableScan>(IgniteLogicalTableScan.class, IgniteTableScan.class,
            "LogicalConverterTableScanRule") {
            /** {@inheritDoc} */
            @Override protected IgniteTableScan createNode(ProjectableFilterableTableScan rel, RelTraitSet traits) {
                return new IgniteTableScan(rel.getCluster(), traits, rel.getTable(), rel.projects(), rel.condition(),
                    rel.requiredColunms());
            }
        };

    /**
     * Constructor.
     *
     * @param clazz Class of relational expression to match.
     * @param desc Description, or null to guess description
     */
    private LogicalScanConverterRule(Class<? extends RelNode> clazz, Class<T> tableClass, String desc) {
        super(ProjectableFilterableTableScan.class);
    }

    /** */
    protected abstract PhysicalNode createNode(ProjectableFilterableTableScan rel, RelTraitSet traits);

    /** */
    protected LogicalScanConverterRule(Class<ProjectableFilterableTableScan> clazz) {
        super(clazz);
    }

    /** */
    @Override protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, ProjectableFilterableTableScan rel) {
        RelOptCluster cluster = rel.getCluster();

        RelTraitSet traitSet = rel.getTraitSet();

        RelDistribution distr = traitSet.getDistribution();

        RelCollation coll = traitSet.getCollation();

        RelTraitSet igniteTraitSet = cluster.traitSetOf(IgniteConvention.INSTANCE)
            .replace(distr)
            .replace(RewindabilityTrait.REWINDABLE)
            .replace(coll);

        return createNode(rel, igniteTraitSet);
    }
}
