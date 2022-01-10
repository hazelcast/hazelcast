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

import com.hazelcast.jet.sql.impl.opt.SlidingWindow;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.rules.TransformationRule;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static java.util.Collections.singletonList;

public class StreamingFilterTransposeRule extends RelRule<Config> implements TransformationRule {

    private static final Config CONFIG = Config.EMPTY
            .withDescription(StreamingFilterTransposeRule.class.getSimpleName())
            .withOperandSupplier(b0 -> b0
                    .operand(Filter.class)
                    .trait(LOGICAL)
                    .inputs(b1 -> b1
                            .operand(SlidingWindow.class).anyInputs()));

    public static final RelOptRule STREAMING_FILTER_TRANSPOSE = new StreamingFilterTransposeRule(CONFIG);

    protected StreamingFilterTransposeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        final SlidingWindow sw = call.rel(1);

        final Filter newFilter = filter.copy(filter.getTraitSet(), sw.getInput(), filter.getCondition());
        final SlidingWindow topSW = (SlidingWindow) sw.copy(sw.getTraitSet(), singletonList(newFilter));
        call.transformTo(topSW);
    }
}
