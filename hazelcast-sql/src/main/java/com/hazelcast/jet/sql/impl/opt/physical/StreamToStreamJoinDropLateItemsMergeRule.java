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
 * Physical rule that drops {@link DropLateItemsPhysicalRel} from <b>all</b> inputs
 * of {@link StreamToStreamJoinPhysicalRel}.
 * Note: <b>every</b> Join's input rel must be {@link DropLateItemsPhysicalRel}.
 * <p>
 * Before:
 * <pre>
 * StreamToStreamJoin[...]
 *   DropLateItemsPhysicalRel[...]
 *   ...
 *   DropLateItemsPhysicalRel[...]
 * </pre>
 * After:
 * <pre>
 *  StreamToStreamJoin[...]
 * </pre>
 * <p>
 */
@Value.Enclosing
public class StreamToStreamJoinDropLateItemsMergeRule extends RelRule<RelRule.Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableStreamToStreamJoinDropLateItemsMergeRule.Config.builder()
                .description(StreamToStreamJoinDropLateItemsMergeRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(StreamToStreamJoinPhysicalRel.class)
                        .trait(PHYSICAL)
                        .unorderedInputs(b1 -> b1
                                .operand(DropLateItemsPhysicalRel.class)
                                .anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new StreamToStreamJoinDropLateItemsMergeRule(this);
        }
    }

    public static final RelOptRule INSTANCE = new StreamToStreamJoinDropLateItemsMergeRule(Config.DEFAULT);

    protected StreamToStreamJoinDropLateItemsMergeRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        StreamToStreamJoinPhysicalRel join = call.rel(0);
        return join.getInputs()
                .stream()
                .map(rel -> (RelSubset) rel)
                .allMatch(rel -> rel.getBest() instanceof DropLateItemsPhysicalRel);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        StreamToStreamJoinPhysicalRel join = call.rel(0);

        RelNode leftInput = ((DropLateItemsPhysicalRel)
                Objects.requireNonNull(((RelSubset) join.getLeft()).getBest())).getInput();

        RelNode rightInput = ((DropLateItemsPhysicalRel)
                Objects.requireNonNull(((RelSubset) join.getRight()).getBest())).getInput();

        StreamToStreamJoinPhysicalRel newJoin = (StreamToStreamJoinPhysicalRel) join.copy(
                join.getTraitSet(),
                join.getCondition(),
                leftInput,
                rightInput,
                join.getJoinType(),
                join.isSemiJoinDone()
        );
        call.transformTo(newJoin);
    }
}
