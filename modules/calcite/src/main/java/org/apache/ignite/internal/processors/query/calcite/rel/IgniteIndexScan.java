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

package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils.changeTraits;

/**
 * Relational operator that returns the contents of a table.
 */
public class IgniteIndexScan extends ProjectableFilterableTableScan implements IgniteRel {
    /** */
    private final String idxName;

    /** */
    private final RelCollation collation;

    /** */
    private List<RexNode> lowerIdxCond;

    /** */
    private List<RexNode> upperIdxCond;

    /** */
    private double idxSelectivity = -1;

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteIndexScan(RelInput input) {
        super(changeTraits(input, IgniteConvention.INSTANCE));
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
    public IgniteIndexScan(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable tbl,
        String idxName) {
        this(cluster, traits, tbl, idxName, null, null, null, null, null);
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
    public IgniteIndexScan(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable tbl,
        String idxName,
        @Nullable List<RexNode> proj,
        @Nullable RexNode cond,
        @Nullable ImmutableBitSet requiredColunms,
        @Nullable List<RexNode> lowerIdxCond,
        @Nullable List<RexNode> upperIdxCond,
        double idxSelectivity
    ) {
        super(cluster, traits, ImmutableList.of(), tbl, proj, cond, requiredColunms);

        this.idxName = idxName;
        RelCollation coll = TraitUtils.collation(traits);
        collation = coll == null ? RelCollationTraitDef.INSTANCE.getDefault() : coll;
    }

    /** {@inheritDoc} */
    @Override protected RelWriter explainTerms0(RelWriter pw) {
        pw = pw
            .item("index", idxName)
            .item("collation", collation);
        pw = super.explainTerms0(pw);
        return pw
            .itemIf("lower", lowerIdxCond, !F.isEmpty(lowerIdxCond))
            .itemIf("upper", upperIdxCond, !F.isEmpty(upperIdxCond));
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double tableRows = table.getRowCount() * idxSelectivity;

        if (projects() != null)
            tableRows += tableRows * projects().size();

        tableRows = RelMdUtil.addEpsilon(tableRows);

        return planner.getCostFactory().makeCost(tableRows, 0, 0);
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        assert Double.compare(idxSelectivity, -1) != 0;

        double rows = table.getRowCount() * idxSelectivity;

        if (condition() != null)
            rows *= mq.getSelectivity(this, condition());

        return rows;
    }

    /**
     * @param idxSelectivity Index selectivity.
     */
    public void indexSelectivity(double idxSelectivity) {
        this.idxSelectivity = idxSelectivity;
    }
}
