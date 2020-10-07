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
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalTableScan;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTrait;

/** */
public abstract class LogicalTableScanConverterRule<T extends IgniteLogicalTableScan>
    extends AbstractIgniteConverterRule<IgniteLogicalTableScan> {
    /** Instance. */
    public static final LogicalTableScanConverterRule<IgniteLogicalTableScan> LOGICAL_TO_TABLE_SCAN =
        new LogicalTableScanConverterRule<IgniteLogicalTableScan>(IgniteLogicalTableScan.class) {
            /** {@inheritDoc} */
            @Override protected IgniteTableScan createNode(IgniteLogicalTableScan rel) {
                return IgniteLogicalTableScan.create(rel, rel.getTraitSet().replace(IgniteConvention.INSTANCE));
            }
        };

    /** */
    protected abstract IgniteTableScan createNode(IgniteLogicalTableScan rel);

    /** */
    protected LogicalTableScanConverterRule(Class<IgniteLogicalTableScan> clazz) {
        super(clazz);
    }

    /** */
    @Override protected PhysicalNode convert(RelOptPlanner planner, RelMetadataQuery mq, IgniteLogicalTableScan rel) {
        return createNode(rel);
    }
}
