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

package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import java.util.LinkedList;
import java.util.Queue;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class LimitPropagationRule extends RelRule<LimitPropagationRule.Config> {
    public static final RelOptRule INSTANCE = LimitPropagationRule.Config.DEFAULT.toRule();

    /**
     * Constructor.
     *
     */
    private LimitPropagationRule(Config cfg) {
        super(cfg);
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        IgniteLimit limit = call.rel(0);

        Queue<RelNode> nodes = new LinkedList<>();

        nodes.add(limit.getInput());

        while (!nodes.isEmpty()) {
            RelNode node = nodes.poll();

            if (node instanceof RelSubset) {
                RelSubset subset = (RelSubset)node;
                node = Util.first(subset.getBest(), subset.getOriginal());
            }

            if (node instanceof IgniteExchange) {
                IgniteExchange exchange = (IgniteExchange)node;

                RelNode input;
                if (exchange.getInput() instanceof RelSubset) {
                    RelSubset inputSubset = (RelSubset)exchange.getInput();

                    input = Util.first(inputSubset.getBest(), inputSubset.getOriginal());
                }
                else
                    input = exchange.getInput();

                IgniteRel newLimit = new IgniteLimit(exchange.getCluster(), input.getTraitSet(), input, limit.offset(), limit.fetch());

                IgniteExchange newExchange = (IgniteExchange)exchange.clone(exchange.getCluster(), F.asList(newLimit));

                call.getPlanner().ensureRegistered(newExchange, exchange);
            }
            else
                nodes.addAll(node.getInputs());
        }
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    public interface Config extends RelRule.Config {
        /** */
        LimitPropagationRule.Config DEFAULT = RelRule.Config.EMPTY
            .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
            .withDescription("TestRule")
            .as(LimitPropagationRule.Config.class)
            .withOperandFor(IgniteLimit.class);

        /** Defines an operand tree for the given classes. */
        default LimitPropagationRule.Config withOperandFor(Class<? extends IgniteLimit> joinClass) {
            return withOperandSupplier(b0 -> b0.operand(joinClass).anyInputs())
                .as(LimitPropagationRule.Config.class);
        }

        /** {@inheritDoc} */
        @Override default LimitPropagationRule toRule() {
            return new LimitPropagationRule(this);
        }
    }
}
