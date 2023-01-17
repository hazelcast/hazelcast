/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.logical.CalcDropLateItemsTransposeRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.TransformationRule;
import org.immutables.value.Value;

import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;
import static org.apache.calcite.plan.volcano.HazelcastRelSubsetUtil.unwrapSubset;

/**
 * Physical rule that drops {@link DropLateItemsPhysicalRel} for inputs of
 * {@link StreamToStreamJoinPhysicalRel}.
 * <p>
 * Before:
 * <pre>
 * StreamToStreamJoin[...]
 *   DropLateItemsPhysicalRel[...]
 *     input
 * </pre>
 * After:
 * <pre>
 *  StreamToStreamJoin[...]
 *    input
 * </pre>
 * <p>
 * This transformation is valid, because s2sj processor also drops late items.
 * <p>
 * If Calc is present between s2sj and drop-late-rel, the {@link CalcDropLateItemsTransposeRule}
 * moves it out.
 */
@Value.Enclosing
public class StreamToStreamJoinDropLateItemsEliminateRule extends RelRule<RelRule.Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableStreamToStreamJoinDropLateItemsEliminateRule.Config.builder()
                .description(StreamToStreamJoinDropLateItemsEliminateRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(StreamToStreamJoinPhysicalRel.class)
                        .trait(PHYSICAL)
                        .unorderedInputs(b1 -> b1
                                .operand(DropLateItemsPhysicalRel.class)
                                .anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new StreamToStreamJoinDropLateItemsEliminateRule(this);
        }
    }

    public static final RelOptRule INSTANCE = new StreamToStreamJoinDropLateItemsEliminateRule(Config.DEFAULT);

    protected StreamToStreamJoinDropLateItemsEliminateRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        StreamToStreamJoinPhysicalRel join = call.rel(0);
        DropLateItemsPhysicalRel drop = call.rel(1);

        RelNode leftInput = join.getLeft();
        RelNode rightInput = join.getRight();

        StreamToStreamJoinPhysicalRel newJoin;

        if (drop == unwrapSubset(leftInput)) {
            newJoin = (StreamToStreamJoinPhysicalRel) join.copy(
                    join.getTraitSet(),
                    join.getCondition(),
                    drop.getInput(),
                    rightInput,
                    join.getJoinType(),
                    join.isSemiJoinDone()
            );
        } else if (drop == unwrapSubset(rightInput)) {
            newJoin = (StreamToStreamJoinPhysicalRel) join.copy(
                    join.getTraitSet(),
                    join.getCondition(),
                    leftInput,
                    drop.getInput(),
                    join.getJoinType(),
                    join.isSemiJoinDone()
            );
        } else {
            throw new AssertionError("Drop rel is neither left, nor right input of the Join rel");
        }

        call.transformTo(newJoin);
    }
}
