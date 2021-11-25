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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql2rel.DeduplicateCorrelateVariables;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTrait;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.RexUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.checkerframework.checker.initialization.qual.NotOnlyInitialized;
import org.checkerframework.checker.initialization.qual.UnknownInitialization;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

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

/*    public static void main(String[] a) {
        ImmutableBitSet left = ImmutableBitSet.of(1, 3, 4, 5);
        ImmutableBitSet right = ImmutableBitSet.of(3, 5);

        Commons.mapping();
    }*/

    /** {@inheritDoc} */
    @Override protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, LogicalFilter rel) {
        RelOptCluster cluster = rel.getCluster();
        RelTraitSet traits = rel.getTraitSet().replace(IgniteConvention.INSTANCE);

        RexNode cond = rel.getCondition();

        Set<CorrelationId> corrIds = RexUtils.extractCorrelationIds(rel.getCondition());

        RelDataType[] condType0 = new RelDataType[1];
        RelDataType condType = null;

        if (!corrIds.isEmpty()) {
            cond = new RexShuttle() {
                @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                    condType0[0] = fieldAccess.getReferenceExpr().getType();
                    return super.visitFieldAccess(fieldAccess);
                }
            }.apply(cond);

            condType = condType0[0];
        }

        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();

        VariableUsedVisitor varUsed = new VariableUsedVisitor(null);

        varUsed.apply(cond);

        Map<CorrelationId, String> corrToCol = new HashMap<>();
        T2<Integer, String> posToCol;

        for (Map.Entry<CorrelationId, Collection<String>> ent : varUsed.variableFields.asMap().entrySet()) {
            assert ent.getValue().size() == 2;

            for (String name : ent.getValue())
                if (!name.isEmpty())
                    corrToCol.put(ent.getKey(), name);
        }

        posToCol = varUsed.colToName;

/*        cond = new RexShuttle() {
            @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                RexNode refExpr = fieldAccess.getReferenceExpr().accept(this);
                int fieldIndex = fieldAccess.getField().getIndex();
                String fieldName = fieldAccess.getField().getName();
                CorrelationId corrId = colToCorr.get(fieldName);
                return cluster.getRexBuilder().makeFieldAccess(
                    cluster.getRexBuilder().makeCorrel(rel.getRowType(), corrId), fieldIndex);
            }
        }.apply(cond);*/

/*        cond = new RexShuttle() {
            @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
                return cluster.getRexBuilder().makeCorrel(rel.getRowType(), variable.id);
            }
        }.apply(cond);*/

/*        cond = new RexShuttle() {
            @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
                CorrelationId corrId = variable.id;
                return super.visitCorrelVariable(variable);
            }

            @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                int idx = fieldAccess.getField().getIndex();
                builder.set(idx);

                return super.visitFieldAccess(fieldAccess);
            }
        }.apply(cond);

        ImmutableBitSet requiredColumns = builder.build();

        Mappings.TargetMapping targetMapping = Commons.mapping(requiredColumns, 100);

        final LogicalFilter rel0 = rel;
        final Set<CorrelationId> corrIds0 = corrIds;

        if (!corrIds.isEmpty())
            assert corrIds.size() == 1;
            cond = new RexShuttle() {
                @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                    RexNode refExpr = fieldAccess.getReferenceExpr().accept(this);
                    int fieldIndex = fieldAccess.getField().getIndex();
                    return cluster.getRexBuilder().makeFieldAccess(
                        cluster.getRexBuilder().makeCorrel(rel0.getRowType(), F.first(corrIds0)),
                        targetMapping.getTarget(fieldIndex));
                }
            }.apply(cond);

        corrIds = RexUtils.extractCorrelationIds(cond);

        if (!corrIds.isEmpty())
            traits = traits.replace(CorrelationTrait.correlations(corrIds));*/

        return new IgniteFilter(cluster, traits, rel.getInput(), cond, condType);
    }

    private static class VariableUsedVisitor extends RexShuttle {
        public final Set<CorrelationId> variables = new LinkedHashSet();
        public final Multimap<CorrelationId, String> variableFields = LinkedHashMultimap.create();
        public final T2<Integer, String> colToName = new T2<>();
        @NotOnlyInitialized
        @Nullable
        private final RelShuttle relShuttle;

        public VariableUsedVisitor(@UnknownInitialization @Nullable RelShuttle relShuttle) {
            this.relShuttle = relShuttle;
        }

        @Override public RexNode visitCorrelVariable(RexCorrelVariable p) {
            this.variables.add(p.id);
            this.variableFields.put(p.id, "");
            return p;
        }

        @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
                RexCorrelVariable v = (RexCorrelVariable)fieldAccess.getReferenceExpr();
                this.variableFields.put(v.id, fieldAccess.getField().getName());
                int idx = fieldAccess.getField().getIndex();
                if (colToName.getKey() == null || colToName.getKey() > idx)
                    colToName.put(idx, fieldAccess.getField().getName());
            }

            return super.visitFieldAccess(fieldAccess);
        }

        @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
            if (this.relShuttle != null)
                subQuery.rel.accept(this.relShuttle);

            return super.visitSubQuery(subQuery);
        }
    }
}
