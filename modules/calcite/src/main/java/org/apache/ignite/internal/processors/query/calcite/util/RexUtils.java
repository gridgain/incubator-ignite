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

package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/** */
public class RexUtils {
    /** */
    public static RexNode makeCast(RexBuilder builder, RelDataType toType, RexNode node) {
        return TypeUtils.needCast(builder.getTypeFactory(), node.getType(), toType)
            ? builder.makeCast(toType, node)
            : node;
    }

    /** */
    public static RexBuilder builder(RelNode rel) {
        return builder(rel.getCluster());
    }

    /** */
    public static RexBuilder builder(RelOptCluster cluster) {
        return cluster.getRexBuilder();
    }

    /** */
    public static RexExecutor executor(RelNode rel) {
        return executor(rel.getCluster());
    }

    /** */
    public static RexExecutor executor(RelOptCluster cluster) {
        return Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR);
    }

    /** */
    public static RexSimplify simplifier(RelOptCluster cluster) {
        return new RexSimplify(builder(cluster), RelOptPredicateList.EMPTY, executor(cluster));
    }

    /** */
    public static RexNode makeCase(RexBuilder builder, RexNode... operands) {
        if (U.assertionsEnabled()) {
            // each odd operand except last one has to return a boolean type
            for (int i = 0; i < operands.length; i += 2) {
                if (operands[i].getType().getSqlTypeName() != SqlTypeName.BOOLEAN && i < operands.length - 1) {
                    throw new AssertionError("Unexpected operand type. " +
                        "[operands=" + Arrays.toString(operands) + "]");
                }
            }
        }

        return builder.makeCall(SqlStdOperatorTable.CASE, operands);
    }

    /** Returns whether a list of expressions projects the incoming fields. */
    public static boolean isIdentity(List<? extends RexNode> projects, RelDataType inputRowType) {
        return isIdentity(projects, inputRowType, false);
    }

    /** Returns whether a list of expressions projects the incoming fields. */
    public static boolean isIdentity(List<? extends RexNode> projects, RelDataType inputRowType, boolean local) {
        if (inputRowType.getFieldCount() != projects.size())
            return false;

        final List<RelDataTypeField> fields = inputRowType.getFieldList();
        Class<? extends RexSlot> clazz = local ? RexLocalRef.class : RexInputRef.class;

        for (int i = 0; i < fields.size(); i++) {
            if (!clazz.isInstance(projects.get(i)))
                return false;

            RexSlot ref = (RexSlot) projects.get(i);

            if (ref.getIndex() != i)
                return false;

            if (!RelOptUtil.eq("t1", projects.get(i).getType(), "t2", fields.get(i).getType(), Litmus.IGNORE))
                return false;
        }

        return true;
    }

    /** Supported index operations. */
    private static final Set<SqlKind> TREE_INDEX_COMPARISON =
        EnumSet.of(
            EQUALS,
            LESS_THAN, GREATER_THAN,
            GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL);

    /**
     * Builds index conditions.
     */
    public static double buildIndexConditions(RexNode condition, RelCollation collation, RelOptCluster cluster,
                                            List<RexNode> lowerIdxCond, List<RexNode> upperIdxCond) {
        double selectivity = 1.0;

        if (!boundsArePossible(condition, collation, cluster))
            return selectivity;

        assert condition != null;

        Map<Integer, List<RexCall>> fieldsToPredicates = mapPredicatesToFields(condition, cluster);

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

                assert idxOpSupports(cond) : cond;

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
        return selectivity;
    }

    /** */
    private static boolean boundsArePossible(@Nullable RexNode cond, RelCollation collation, RelOptCluster cluster) {
        if (cond == null)
            return false;

        RexCall dnf = (RexCall) RexUtil.toDnf(builder(cluster), cond);

        if (dnf.isA(OR) && dnf.getOperands().size() > 1) // OR conditions are not supported yet.
            return false;

        return !collation.getFieldCollations().isEmpty();
    }

    /** */
    private static Map<Integer, List<RexCall>> mapPredicatesToFields(RexNode condition, RelOptCluster cluster) {
        List<RexNode> predicatesConjunction = RelOptUtil.conjunctions(condition);

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
                predCall = (RexCall)RexUtil.invert(builder(cluster), predCall);

            fldPreds.add(predCall);
        }
        return fieldsToPredicates;
    }

    /** */
    private static RexNode extractRef(RexCall call) {
        assert isBinaryComparison(call);

        RexNode leftOp = call.getOperands().get(0);
        RexNode rightOp = call.getOperands().get(1);

        leftOp = removeCast(leftOp);
        rightOp = removeCast(rightOp);

        if (leftOp instanceof RexLocalRef && idxOpSupports(rightOp))
            return leftOp;
        else if (idxOpSupports(leftOp) && rightOp instanceof RexLocalRef)
            return rightOp;

        return null;
    }

    /** */
    private static boolean refOnTheRight(RexCall predCall) {
        RexNode rightOp = predCall.getOperands().get(1);

        rightOp = removeCast(rightOp);

        return rightOp.isA(SqlKind.LOCAL_REF);
    }

    /** */
    private static boolean isBinaryComparison(RexNode exp) {
        return TREE_INDEX_COMPARISON.contains(exp.getKind()) &&
            (exp instanceof RexCall) &&
            ((RexCall)exp).getOperands().size() == 2;
    }

    /** */
    public static boolean idxOpSupports(RexNode op) {
        return op instanceof RexLiteral
            || op instanceof RexDynamicParam
            || op instanceof RexFieldAccess;
    }

    /** */
    public static List<RexNode> buildIndexCondition(Iterable<RexNode> idxCond, RelDataType rowType,
                                                 RelOptCluster cluster) {
        if (F.isEmpty(idxCond))
            return null;

        List<RexNode> res = makeListOfNullLiterals(rowType, cluster);
        List<RelDataType> fieldTypes = RelOptUtil.getFieldTypeList(rowType);
        RexBuilder rexBuilder = cluster.getRexBuilder();

        for (RexNode pred : idxCond) {
            assert pred instanceof RexCall;

            RexCall call = (RexCall)pred;
            RexLocalRef ref = (RexLocalRef)removeCast(call.operands.get(0));
            RexNode cond = removeCast(call.operands.get(1));

            assert idxOpSupports(cond) : cond;

            res.set(ref.getIndex(), makeCast(rexBuilder, fieldTypes.get(ref.getIndex()), cond));
        }

        return res;
    }

    /** */
    private static List<RexNode> makeListOfNullLiterals(RelDataType rowType, RelOptCluster cluster) {
        assert rowType.isStruct();

        RexBuilder builder = cluster.getRexBuilder();

        List<RexNode> list = new ArrayList<>(rowType.getFieldCount());
        for (RelDataTypeField field : rowType.getFieldList())
            list.add(builder.makeNullLiteral(field.getType()));

        return list;
    }
}
