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

import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

import static org.apache.ignite.internal.processors.query.calcite.util.RexUtils.isIdentity;

/** */
public abstract class ProjectScanMergeRule<T extends ProjectableFilterableTableScan> extends RelOptRule {
    /** Instance. */
    public static final ProjectScanMergeRule<IgniteIndexScan> INDEX_SCAN =
        new ProjectScanMergeRule<IgniteIndexScan>(LogicalProject.class, IgniteIndexScan.class, "ProjectIndexScanMergeRule") {
            /** {@inheritDoc} */
            @Override protected IgniteIndexScan createNode(RelOptCluster cluster, IgniteIndexScan scan,
                RelTraitSet traits, List<RexNode> projections, RexNode cond, ImmutableBitSet requiredColunms) {
                return new IgniteIndexScan(cluster, traits, scan.getTable(), scan.indexName(), projections,
                    cond, requiredColunms);
            }
        };

    /** Instance. */
    public static final ProjectScanMergeRule<IgniteTableScan> TABLE_SCAN =
        new ProjectScanMergeRule<IgniteTableScan>(LogicalProject.class, IgniteTableScan.class, "ProjectTableScanMergeRule") {
            /** {@inheritDoc} */
            @Override protected IgniteTableScan createNode(RelOptCluster cluster, IgniteTableScan scan,
                RelTraitSet traits, List<RexNode> projections, RexNode cond, ImmutableBitSet requiredColunms) {
                return new IgniteTableScan(cluster, traits, scan.getTable(), projections, cond, requiredColunms);
            }
        };

    /** */
    protected abstract T createNode(RelOptCluster cluster, T scan, RelTraitSet traits, List<RexNode> projections,
                                    RexNode cond, ImmutableBitSet requiredColunms);

    /**
     * Constructor.
     *
     * @param projectionClazz Projection class of relational expression to match.
     * @param tableClass Ignite scan class.
     * @param desc Description, or null to guess description
     */
    private ProjectScanMergeRule(Class<? extends RelNode> projectionClazz, Class<T> tableClass, String desc) {
        super(operand(projectionClazz,
                operand(tableClass, none())),
                    RelFactories.LOGICAL_BUILDER, desc);
    }

    /** {@inheritDoc} */
    @Override public boolean matches(RelOptRuleCall call) {
        T rel = call.rel(1);
        return rel.projects() == null;
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        LogicalProject relProject = call.rel(0);
        T scan = call.rel(1);

        RelOptCluster cluster = scan.getCluster();
        List<RexNode> projects = relProject.getProjects();
        RexNode cond = scan.condition();

        // projection changes input collation and distribution.
        RelTraitSet traits = scan.getTraitSet();

        traits = traits.replace(TraitUtils.projectCollation(
            TraitUtils.collation(traits), projects, scan.getRowType()));

        traits = traits.replace(TraitUtils.projectDistribution(
            TraitUtils.distribution(traits), projects, scan.getRowType()));

        IgniteTable tbl = scan.getTable().unwrap(IgniteTable.class);
        IgniteTypeFactory typeFactory = Commons.context(scan).typeFactory();
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();

        new RexShuttle() {
            @Override public RexNode visitInputRef(RexInputRef ref) {
                builder.set(ref.getIndex());
                return ref;
            }
        }.apply(projects);

        new RexShuttle() {
            @Override public RexNode visitLocalRef(RexLocalRef inputRef) {
                builder.set(inputRef.getIndex());
                return inputRef;
            }
        }.apply(cond);

        ImmutableBitSet requiredColunms = builder.build();

        Mappings.TargetMapping targetMapping = Mappings.create(MappingType.PARTIAL_FUNCTION,
            tbl.getRowType(typeFactory).getFieldCount(), requiredColunms.cardinality());

        for (Ord<Integer> ord : Ord.zip(requiredColunms))
            targetMapping.set(ord.e, ord.i);

        projects = new RexShuttle() {
            @Override public RexNode visitInputRef(RexInputRef ref) {
                return new RexLocalRef(targetMapping.getTarget(ref.getIndex()), ref.getType());
            }
        }.apply(projects);

        if (isIdentity(projects, tbl.getRowType(typeFactory, requiredColunms), true))
            projects = null;

        cond = new RexShuttle() {
            @Override public RexNode visitLocalRef(RexLocalRef ref) {
                return new RexLocalRef(targetMapping.getTarget(ref.getIndex()), ref.getType());
            }
        }.apply(cond);

        call.transformTo(createNode(cluster, scan, traits, projects, cond, requiredColunms));
    }
}
