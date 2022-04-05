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
import org.apache.calcite.rel.rules.TransformationRule;
import org.immutables.value.Value;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static java.util.Collections.singletonList;

/**
 * Logical rule that eliminates a {@link DropLateItemsLogicalRel} as an input of {@link SlidingWindow},
 * because {@link SlidingWindow} performs late items dropping itself.
 * <p>
 * Before:
 * <pre>
 * SlidingWindow[...]
 *     DropLateItemsLogicalRel[...]
 * </pre>
 * After:
 * <pre>
 * SlidingWindow[...]
 * </pre>
 */
@Value.Enclosing
public class SlidingWindowDropLateItemsMergeRule extends RelRule<RelRule.Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableSlidingWindowDropLateItemsMergeRule.Config.builder()
                .description(SlidingWindowDropLateItemsMergeRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(SlidingWindow.class)
                        .trait(LOGICAL)
                        .inputs(b1 -> b1
                                .operand(DropLateItemsLogicalRel.class).anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new SlidingWindowDropLateItemsMergeRule(this);
        }
    }

    public static final RelOptRule INSTANCE = new SlidingWindowDropLateItemsMergeRule(Config.DEFAULT);

    protected SlidingWindowDropLateItemsMergeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        SlidingWindow sw = call.rel(0);
        DropLateItemsLogicalRel drop = call.rel(1);

        SlidingWindow newSw = (SlidingWindow) sw.copy(sw.getTraitSet(), singletonList(drop.getInput()));
        call.transformTo(newSw);
    }
}
