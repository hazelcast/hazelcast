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

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.AggregateLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.immutables.value.Value;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

/**
 * A rule that replaces any streaming aggregation with {@link ShouldNotExecuteRel}.
 * This is to handle cases when the aggregation isn't implemented by replacing
 * it with {@link ShouldNotExecuteRel}, which has huge cost. If no other rule
 * replaces the aggregation with something that can be executed, the error will
 * be thrown to the user.
 * <p>
 * Currently, there's only {@link AggregateSlidingWindowPhysicalRule} that
 * handles some streaming aggregation cases.
 */
@Value.Enclosing
public final class StreamAggregateCannotExecuteRule extends RelRule<Config> {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableStreamAggregateCannotExecuteRule.Config.builder()
                .description(StreamAggregateCannotExecuteRule.class.getSimpleName())
                .operandSupplier(b0 -> b0.operand(AggregateLogicalRel.class)
                        .trait(LOGICAL)
                        .predicate(OptUtils::isUnbounded)
                        .anyInputs()
                )
                .build();

        @Override
        default RelOptRule toRule() {
            return new StreamAggregateCannotExecuteRule(this);
        }
    }

    private StreamAggregateCannotExecuteRule(Config config) {
        super(config);
    }

    @SuppressWarnings("checkstyle:DeclarationOrder")
    public static final RelOptRule INSTANCE = new StreamAggregateCannotExecuteRule(Config.DEFAULT);

    @Override
    public void onMatch(RelOptRuleCall call) {
        AggregateLogicalRel aggr = call.rel(0);
        call.transformTo(
                new ShouldNotExecuteRel(aggr.getCluster(), OptUtils.toPhysicalConvention(aggr.getTraitSet()), aggr.getRowType(),
                        "Streaming aggregation is supported only for window aggregation, with imposed watermark order " +
                                "(see TUMBLE/HOP and IMPOSE_ORDER functions)"));
    }
}
