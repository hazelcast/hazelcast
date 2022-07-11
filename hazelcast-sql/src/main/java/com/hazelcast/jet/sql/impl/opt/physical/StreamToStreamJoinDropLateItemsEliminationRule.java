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

package com.hazelcast.jet.sql.impl.opt.physical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.TransformationRule;
import org.immutables.value.Value;

import java.util.Objects;

import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;

/**
 * Physical rule that drops {@link DropLateItemsPhysicalRel} for inputs
 * of {@link StreamToStreamJoinPhysicalRel}, specifically for {@link FullScanPhysicalRel}.
 * <p>
 * Before:
 * <pre>
 * StreamToStreamJoin[...]
 *   DropLateItemsPhysicalRel[...]
 *     FullScanPhysicalRel[...]
 * </pre>
 * After:
 * <pre>
 *  StreamToStreamJoin[...]
 *    FullScanPhysicalRel[...]
 * </pre>
 * <p>
 *
 * Note: we are allowed to do that. Even if Calc would be present in rel tree,
 * such transformations are suppose to happen:
 * Calc & DropRel transposition -> Calc pushdown into FullScan
 *
 */
@Value.Enclosing
public class StreamToStreamJoinDropLateItemsEliminationRule extends RelRule<RelRule.Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableStreamToStreamJoinDropLateItemsEliminationRule.Config.builder()
                .description(StreamToStreamJoinDropLateItemsEliminationRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(StreamToStreamJoinPhysicalRel.class)
                        .trait(PHYSICAL)
                        .unorderedInputs(b1 -> b1
                                .operand(DropLateItemsPhysicalRel.class)
                                .oneInput(b2 -> b2
                                        .operand(FullScanPhysicalRel.class)
                                        .noInputs())))
                .build();

        @Override
        default RelOptRule toRule() {
            return new StreamToStreamJoinDropLateItemsEliminationRule(this);
        }
    }

    public static final RelOptRule INSTANCE = new StreamToStreamJoinDropLateItemsEliminationRule(Config.DEFAULT);

    protected StreamToStreamJoinDropLateItemsEliminationRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        StreamToStreamJoinPhysicalRel join = call.rel(0);
        DropLateItemsPhysicalRel drop = call.rel(1);
        FullScanPhysicalRel scan = call.rel(2);

        RelNode leftInput = Objects.requireNonNull(((RelSubset) join.getLeft()).getBest());
        RelNode rightInput = Objects.requireNonNull(((RelSubset) join.getRight()).getBest());

        StreamToStreamJoinPhysicalRel newJoin;

        if (drop.equals(leftInput)) {
            newJoin = (StreamToStreamJoinPhysicalRel) join.copy(
                    join.getTraitSet(),
                    join.getCondition(),
                    scan,
                    rightInput,
                    join.getJoinType(),
                    join.isSemiJoinDone()
            );
        } else if (drop.equals(rightInput)) {
            newJoin = (StreamToStreamJoinPhysicalRel) join.copy(
                    join.getTraitSet(),
                    join.getCondition(),
                    leftInput,
                    scan,
                    join.getJoinType(),
                    join.isSemiJoinDone()
            );
        } else {
            throw new AssertionError("Drop rel is not equal to left or right JOIN input rel.");
        }

        call.transformTo(newJoin);
    }
}
