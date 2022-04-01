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
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.rules.TransformationRule;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static java.util.Collections.singletonList;

/**
 * Logical rule that eliminates all input {@link DropLateItemsLogicalRel} for {@link Union},
 * which is an input for {@link SlidingWindow}.
 * {@link SlidingWindow} performs late items dropping by itself.
 * <p>
 * Before:
 * <pre>
 * SlidingWindow[...]
 *   Union[all=true]
 *     DropLateItemsLogicalRel[...]
 *     ...
 *     DropLateItemsLogicalRel[...]
 * </pre>
 * After:
 * <pre>
 * SlidingWindow[...]
 * </pre>
 */
@Value.Enclosing
public class SlidingWindowUnionDropLateItemsMergeRule extends RelRule<RelRule.Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableSlidingWindowUnionDropLateItemsMergeRule.Config.builder()
                .description(SlidingWindowUnionDropLateItemsMergeRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(SlidingWindow.class)
                        .trait(LOGICAL)
                        .inputs(b1 -> b1
                                .operand(Union.class)
                                .predicate(union -> union.all)
                                .anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new SlidingWindowUnionDropLateItemsMergeRule(this);
        }
    }

    public static final RelOptRule INSTANCE = new SlidingWindowUnionDropLateItemsMergeRule(Config.DEFAULT);

    protected SlidingWindowUnionDropLateItemsMergeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        SlidingWindow sw = call.rel(0);
        Union union = call.rel(1);

        List<RelNode> inputs = new ArrayList<>(union.getInputs().size());

        for (RelNode node : union.getInputs()) {
            if (node instanceof RelSubset) {
                RelNode best = ((RelSubset) node).getBest();
                if (best instanceof DropLateItemsLogicalRel) {
                    inputs.add(((DropLateItemsLogicalRel) best).getInput());
                } else {
                    // if at least one input is not watermarked - we can't apply that rule.
                    return;
                }
            }
        }

        assert inputs.size() == union.getInputs().size();

        Union newUnion = (Union) union.copy(union.getTraitSet(), inputs);
        SlidingWindow newSw = (SlidingWindow) sw.copy(sw.getTraitSet(), singletonList(newUnion));
        call.transformTo(newSw);
    }
}
