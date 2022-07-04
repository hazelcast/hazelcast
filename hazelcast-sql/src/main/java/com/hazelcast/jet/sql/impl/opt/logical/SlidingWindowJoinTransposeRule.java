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

import com.hazelcast.jet.sql.impl.opt.SlidingWindow;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rules.TransformationRule;
import org.immutables.value.Value;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static java.util.Collections.singletonList;

@Value.Enclosing
public class SlidingWindowJoinTransposeRule extends RelRule<Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableSlidingWindowJoinTransposeRule.Config.builder()
                .description(SlidingWindowJoinTransposeRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(Join.class)
                        .trait(LOGICAL)
                        .inputs(b1 -> b1
                                .operand(SlidingWindow.class).anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new SlidingWindowJoinTransposeRule(this);
        }
    }

    public static final RelOptRule STREAMING_JOIN_TRANSPOSE = new SlidingWindowJoinTransposeRule(Config.DEFAULT);

    protected SlidingWindowJoinTransposeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        // TODO [sasha]: finish in follow-up PR
        final Join join = call.rel(0);
        final SlidingWindow sw = call.rel(1);

        boolean windowIsLeftInput = ((RelSubset) join.getLeft()).getBest() instanceof SlidingWindow;

        Join newJoin;
        if (windowIsLeftInput) {
            newJoin = join.copy(
                    join.getTraitSet(),
                    join.getCondition(),
                    sw.getInput(),
                    join.getRight(),
                    join.getJoinType(),
                    join.isSemiJoinDone()
            );
        } else {
            newJoin = join.copy(
                    join.getTraitSet(),
                    join.getCondition(),
                    join.getLeft(),
                    sw.getInput(),
                    join.getJoinType(),
                    join.isSemiJoinDone()
            );
        }

        final SlidingWindow topSW = (SlidingWindow) sw.copy(sw.getTraitSet(), singletonList(newJoin));
        call.transformTo(topSW);
    }
}
