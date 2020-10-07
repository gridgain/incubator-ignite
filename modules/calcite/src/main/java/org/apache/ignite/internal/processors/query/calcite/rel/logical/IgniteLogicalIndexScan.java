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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.apache.ignite.internal.processors.query.calcite.util.RexUtils.buildIndexConditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.ProjectableFilterableTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.TraitUtils;
import org.jetbrains.annotations.Nullable;

/** */
public class IgniteLogicalIndexScan extends ProjectableFilterableTableScan {
    /** */
    private String idxName;

    /** Creates a IgniteIndexScan. */
    public static IgniteIndexScan create(IgniteLogicalIndexScan logicalIdxScan, RelTraitSet traitSet) {
        RelOptCluster cluster = logicalIdxScan.getCluster();
        RelTraitSet traits = logicalIdxScan.getTraitSet();
        RelOptTable tbl = logicalIdxScan.getTable();
        List<RexNode> proj = logicalIdxScan.projects();
        RexNode cond = logicalIdxScan.condition();
        ImmutableBitSet reqColumns = logicalIdxScan.requiredColunms();
        String indexName = logicalIdxScan.indexName();

        RelCollation coll = TraitUtils.collation(traits);
        RelCollation collation = coll == null ? RelCollationTraitDef.INSTANCE.getDefault() : coll;

        List<RexNode> lowerIdxCond = new ArrayList<>(tbl.getRowType().getFieldCount());
        List<RexNode> upperIdxCond = new ArrayList<>(tbl.getRowType().getFieldCount());

        double idxSelectivity = buildIndexConditions(cond, collation, cluster, lowerIdxCond, upperIdxCond);

        IgniteIndexScan idxScan = new IgniteIndexScan(cluster, traits, tbl, indexName, proj, cond, reqColumns,
            lowerIdxCond, upperIdxCond, idxSelectivity);

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
    }

    /** */
    public String indexName() {
        return idxName;
    }
}
