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
import com.hazelcast.sql.impl.QueryException;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.immutables.value.Value;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static java.util.Collections.singletonList;

/**
 * A `Filter` reading from a `SlidingWindow` will be moved before the sliding
 * window.
 */
@Value.Enclosing
public class SlidingWindowFilterTransposeRule extends RelRule<Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableSlidingWindowFilterTransposeRule.Config.builder()
                .description(SlidingWindowFilterTransposeRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(Filter.class)
                        .trait(LOGICAL)
                        .inputs(b1 -> b1
                                .operand(SlidingWindow.class).anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new SlidingWindowFilterTransposeRule(this);
        }
    }

    public static final RelOptRule STREAMING_FILTER_TRANSPOSE = new SlidingWindowFilterTransposeRule(Config.DEFAULT);

    protected SlidingWindowFilterTransposeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        final SlidingWindow sw = call.rel(1);

        RexVisitorImpl<Void> visitor = new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitInputRef(RexInputRef ref) {
                int index = ref.getIndex();
                if (index == sw.windowStartIndex() || index == sw.windowEndIndex()) {
                    throw QueryException.error("Can't apply filter criteria to window bounds");
                }
                return super.visitInputRef(ref);
            }
        };
        filter.getCondition().accept(visitor);

        final Filter newFilter = filter.copy(filter.getTraitSet(), sw.getInput(), filter.getCondition());
        final SlidingWindow topSW = (SlidingWindow) sw.copy(sw.getTraitSet(), singletonList(newFilter));
        call.transformTo(topSW);
    }
}
