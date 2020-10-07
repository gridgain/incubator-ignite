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

package org.apache.ignite.internal.processors.query.calcite.rel.logical;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static org.apache.calcite.rex.RexUtil.removeCast;
import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN;
import static org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.OR;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/** */
public class IgniteLogicalIndexScan extends ProjectableFilterableTableScan {
    /** Supported index operations. */
    public static final Set<SqlKind> TREE_INDEX_COMPARISON =
        EnumSet.of(
            EQUALS,
            LESS_THAN, GREATER_THAN,
            GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL);

    /** */
    private final String idxName;

    /** */
    private final RelCollation collation;

    /** */
    private final List<RexNode> lowerIdxCond;

    /** */
    private final List<RexNode> upperIdxCond;

    /** */
    private double idxSelectivity = 1.0;

    /** Creates a IgniteIndexScan. */
    public static IgniteIndexScan create(IgniteLogicalIndexScan logicalIdxScan, RelTraitSet traitSet) {
        RelOptCluster cluster = logicalIdxScan.getCluster();
        RelTraitSet traits = logicalIdxScan.getTraitSet();
        RelOptTable tbl = logicalIdxScan.getTable();
        List<RexNode> proj = logicalIdxScan.projects();
        RexNode cond = logicalIdxScan.condition();
        ImmutableBitSet reqColumns = logicalIdxScan.requiredColunms();
        String indexName = logicalIdxScan.indexName();

        List<RexNode> lowerBound = logicalIdxScan.lowerIndexCondition();
        List<RexNode> upperBound = logicalIdxScan.upperIndexCondition();

        IgniteIndexScan idxScan = new IgniteIndexScan(cluster, traits, tbl, indexName, proj, cond, reqColumns,
            lowerBound, upperBound);

        idxScan.indexSelectivity(logicalIdxScan.indexSelectivity());

        return idxScan;
    }

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteLogicalIndexScan(RelInput input) {
        //super(changeTraits(input, IgniteConvention.INSTANCE));
        super(input);
        idxName = input.getString("index");
        lowerIdxCond = input.get("lower") == null ? ImmutableList.of() : input.getExpressionList("lower");
        upperIdxCond = input.get("upper") == null ? ImmutableList.of() : input.getExpressionList("upper");
        collation = getTable().unwrap(IgniteTable.class).getIndex(idxName).collation();
    }

    /**
     * Creates a TableScan.
     * @param cluster Cluster that this relational expression belongs to
     * @param traits Traits of this relational expression
     * @param tbl Table definition.
     * @param idxName Index name.
     */
    public IgniteLogicalIndexScan(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable tbl,
        String idxName) {
        this(cluster, traits, tbl, idxName, null, null, null);
    }

    /**
     * Creates a TableScan.
     * @param cluster Cluster that this relational expression belongs to
     * @param traits Traits of this relational expression
     * @param tbl Table definition.
     * @param idxName Index name.
     * @param proj Projects.
     * @param cond Filters.
     * @param requiredColunms Participating colunms.
     */
    public IgniteLogicalIndexScan(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable tbl,
        String idxName,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColunms
    ) {
        super(cluster, traits, ImmutableList.of(), tbl, proj, cond, requiredColunms);

        this.idxName = idxName;
        RelCollation coll = TraitUtils.collation(traits);
        collation = coll == null ? RelCollationTraitDef.INSTANCE.getDefault() : coll;
        lowerIdxCond = new ArrayList<>(getRowType().getFieldCount());
        upperIdxCond = new ArrayList<>(getRowType().getFieldCount());
        buildIndexConditions();
    }

    /**
     * Builds index conditions.
     */
    private void buildIndexConditions() {
        if (!boundsArePossible())
            return;

        assert condition() != null;

        Map<Integer, List<RexCall>> fieldsToPredicates = mapPredicatesToFields();

        double selectivity = 1.0;

        for (int i = 0; i < collation.getFieldCollations().size(); i++) {
            RelFieldCollation fc = collation.getFieldCollations().get(i);

            int collFldIdx = fc.getFieldIndex();

            List<RexCall> collFldPreds = fieldsToPredicates.get(collFldIdx);

            if (F.isEmpty(collFldPreds))
                break;

            boolean lowerBoundBelow = !fc.getDirection().isDescending();

            RexNode bestUpper = null;
            RexNode bestLower = null;

            for (RexCall pred : collFldPreds) {
                RexNode cond = removeCast(pred.operands.get(1));

                assert supports(cond) : cond;

                SqlOperator op = pred.getOperator();
                switch (op.kind) {
                    case EQUALS:
                        bestUpper = pred;
                        bestLower = pred;
                        break;

                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                        lowerBoundBelow = !lowerBoundBelow;
                        // Fall through.

                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        if (lowerBoundBelow)
                            bestLower = pred;
                        else
                            bestUpper = pred;
                        break;

                    default:
                        throw new AssertionError("Unknown condition: " + cond);
                }

                if (bestUpper != null && bestLower != null)
                    break; // We've found either "=" condition or both lower and upper.
            }

            if (bestLower == null && bestUpper == null)
                break; // No bounds, so break the loop.

            if (i > 0 && bestLower != bestUpper)
                break; // Go behind the first index field only in the case of multiple "=" conditions on index fields.

            if (bestLower == bestUpper) { // "x=10"
                upperIdxCond.add(bestUpper);
                lowerIdxCond.add(bestLower);
                selectivity *= 0.1;
            }
            else if (bestLower != null && bestUpper != null) { // "x>5 AND x<10"
                upperIdxCond.add(bestUpper);
                lowerIdxCond.add(bestLower);
                selectivity *= 0.25;
                break;
            }
            else if (bestLower != null) { // "x>5"
                lowerIdxCond.add(bestLower);
                selectivity *= 0.35;
                break;
            }
            else { // "x<10"
                upperIdxCond.add(bestUpper);
                selectivity *= 0.35;
                break;
            }
        }
        idxSelectivity = selectivity;
    }

    /** */
    private static boolean supports(RexNode op) {
        return op instanceof RexLiteral
            || op instanceof RexDynamicParam
            || op instanceof RexFieldAccess;
    }

    /** */
    private Map<Integer, List<RexCall>> mapPredicatesToFields() {
        List<RexNode> predicatesConjunction = RelOptUtil.conjunctions(condition());

        Map<Integer, List<RexCall>> fieldsToPredicates = new HashMap<>(predicatesConjunction.size());

        for (RexNode rexNode : predicatesConjunction) {
            if (!isBinaryComparison(rexNode))
                continue;

            RexCall predCall = (RexCall)rexNode;
            RexLocalRef ref = (RexLocalRef)extractRef(predCall);

            if (ref == null)
                continue;

            int constraintFldIdx = ref.getIndex();

            List<RexCall> fldPreds = fieldsToPredicates
                .computeIfAbsent(constraintFldIdx, k -> new ArrayList<>(predicatesConjunction.size()));

            // Let RexLocalRef be on the left side.
            if (refOnTheRight(predCall))
                predCall = (RexCall)RexUtil.invert(getCluster().getRexBuilder(), predCall);

            fldPreds.add(predCall);
        }
        return fieldsToPredicates;
    }

    /** */
    private boolean boundsArePossible() {
        if (condition() == null)
            return false;

        RexCall dnf = (RexCall) RexUtil.toDnf(getCluster().getRexBuilder(), condition());

        if (dnf.isA(OR) && dnf.getOperands().size() > 1) // OR conditions are not supported yet.
            return false;

        return !collation.getFieldCollations().isEmpty();
    }

    /** */
    private static boolean isBinaryComparison(RexNode exp) {
        return TREE_INDEX_COMPARISON.contains(exp.getKind()) &&
            (exp instanceof RexCall) &&
            ((RexCall)exp).getOperands().size() == 2;
    }

    /** */
    private static RexNode extractRef(RexCall call) {
        assert isBinaryComparison(call);

        RexNode leftOp = call.getOperands().get(0);
        RexNode rightOp = call.getOperands().get(1);

        leftOp = removeCast(leftOp);
        rightOp = removeCast(rightOp);

        if (leftOp instanceof RexLocalRef && supports(rightOp))
            return leftOp;
        else if (supports(leftOp) && rightOp instanceof RexLocalRef)
            return rightOp;

        return null;
    }

    /** */
    private static boolean refOnTheRight(RexCall predCall) {
        RexNode rightOp = predCall.getOperands().get(1);

        rightOp = removeCast(rightOp);

        return rightOp.isA(SqlKind.LOCAL_REF);
    }

    /**
     * @return Index selectivity.
     */
    public double indexSelectivity() {
        return idxSelectivity;
    }

    /** */
    public String indexName() {
        return idxName;
    }

    /**
     * @return Lower index condition.
     */
    public List<RexNode> lowerIndexCondition() {
        return lowerIdxCond;
    }

    /**
     * @return Upper index condition.
     */
    public List<RexNode> upperIndexCondition() {
        return upperIdxCond;
    }
}
