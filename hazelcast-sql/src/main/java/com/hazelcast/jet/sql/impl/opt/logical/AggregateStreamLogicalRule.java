/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.SlidingWindow;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

public final class AggregateStreamLogicalRule extends RelRule<Config> {

    private static final Config RULE_CONFIG = Config.EMPTY
            .withDescription(AggregateStreamLogicalRule.class.getSimpleName())
            .withOperandSupplier(b0 -> b0.operand(AggregateLogicalRel.class)
                    .trait(LOGICAL)
                    .predicate(OptUtils::isUnbounded)
                    .anyInputs()
            );


    private AggregateStreamLogicalRule() {
        super(RULE_CONFIG);
    }

    @SuppressWarnings("checkstyle:DeclarationOrder")
    public static final RelOptRule INSTANCE = new AggregateStreamLogicalRule();

    @Override
    public void onMatch(RelOptRuleCall call) {
        AggregateLogicalRel aggregate = call.rel(0);

        SlidingWindowDetectorVisitor visitor = new SlidingWindowDetectorVisitor();
        visitor.go(aggregate);
        if (!visitor.windowFound) {
            call.transformTo(
                    new NoExecuteRel(
                            aggregate.getCluster(),
                            aggregate.getTraitSet(),
                            aggregate.getRowType(),
                            "Aggregations over non-windowed, non-ordered streaming source not supported"));
        }

        if (aggregate.getGroupSet().isEmpty()) {
            call.transformTo(
                    new NoExecuteRel(
                            aggregate.getCluster(),
                            aggregate.getTraitSet().replace(LOGICAL),
                            aggregate.getRowType(),
                            "Streaming aggregation must be grouped by window_start/window_end"));
        }
//        TODO: to implement
//        RelNode input = aggregate.getInput();
//        if (input instanceof HepRelVertex) {
//            input = ((HepRelVertex) input).getCurrentRel();
//        }
//        Integer watermarkedField = findWatermarkedField(aggregate, input);
//        if (watermarkedField == null) {
//            call.transformTo(
//                    new NoExecuteRel(
//                            aggregate.getCluster(),
//                            aggregate.getTraitSet(),
//                            aggregate.getRowType(),
//                            "Can't find watermarked field for window function"));
//        }
    }

    static class SlidingWindowDetectorVisitor extends RelVisitor {
        @SuppressWarnings("checkstyle:visibilitymodifier")
        public boolean windowFound;

        @Override
        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
            if (node instanceof SlidingWindow) {
                windowFound = true;
            }

            // supposed to work with HepPlanner
            if (node instanceof HepRelVertex) {
                RelNode currentRel = ((HepRelVertex) node).getCurrentRel();
                if (currentRel instanceof SlidingWindow) {
                    windowFound = true;
                } else {
                    super.visit(currentRel, ordinal, parent);
                }
            }
            super.visit(node, ordinal, parent);
        }
    }
}
