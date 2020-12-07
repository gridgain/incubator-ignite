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

package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCost;

/** */
public class IgniteLimit extends SingleRel implements IgniteRel {
    /** Offset. */
    private final RexNode offset;

    /** Fetches rows expression (limit) */
    private final RexNode fetch;

    /**
     * Constructor.
     *
     * @param cluster Cluster.
     * @param traits Trait set.
     * @param child Input relational expression.
     * @param offset Offset.
     * @param fetch Limit.
     */
    public IgniteLimit(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RexNode offset,
        RexNode fetch
    ) {
        super(cluster, traits, child);
        this.offset = offset;
        this.fetch = fetch;
    }

    /** {@inheritDoc} */
    @Override public final IgniteLimit copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new IgniteLimit(getCluster(), traitSet, sole(inputs), offset, fetch);
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.itemIf("offset", offset, offset != null);
        pw.itemIf("fetch", fetch, fetch != null);
        return pw;
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        if (required.getConvention() != IgniteConvention.INSTANCE)
            return null;

        return Pair.of(required, ImmutableList.of(required));
    }

    /** {@inheritDoc} */
    @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        assert childId == 0;

        if (childTraits.getConvention() != IgniteConvention.INSTANCE)
            return null;

        return Pair.of(childTraits, ImmutableList.of(childTraits));
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double inputRowCount = mq.getRowCount(getInput());

        int lim = intFromRex(fetch);
        int off = intFromRex(offset);

        double rows = Math.min(lim + off, inputRowCount);

        return planner.getCostFactory().makeCost(rows, rows * IgniteCost.ROW_PASS_THROUGH_COST, 0);
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        double inputRowCount = mq.getRowCount(getInput());

        int lim = intFromRex(fetch);
        int off = intFromRex(offset);

        if (lim == 0 && off == 0)
            return inputRowCount;

        if (lim == 0)
            return Math.max(0, inputRowCount - off);

        // probably we can process DYNAMIC_PARAM here too.
        return Math.min(lim, inputRowCount - off);
    }

    /**
     * @return Integer value of the literal expression.
     */
    private int intFromRex(RexNode n) {
        try {
            if (n.isA(SqlKind.LITERAL))
                return ((RexLiteral)n).getValueAs(Integer.class);
            else
                return 0;
        }
        catch (Exception e) {
            return 0;
        }
    }

    /**
     * @return Offset.
     */
    public RexNode offset() {
        return offset;
    }

    /**
     * @return Fetches rows expression (limit)
     */
    public RexNode fetch() {
        return fetch;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel clone(RelOptCluster cluster, List<IgniteRel> inputs) {
        return new IgniteLimit(cluster, getTraitSet(), sole(inputs), offset, fetch);
    }
}
