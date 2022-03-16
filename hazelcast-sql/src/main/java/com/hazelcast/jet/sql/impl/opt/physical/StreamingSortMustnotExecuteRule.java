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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.rules.TransformationRule;
import org.immutables.value.Value;

/**
 * A rule that replaces any streaming sort attempt with {@link MustnotExecuteRel}.
 */
@Value.Enclosing
public final class StreamingSortMustnotExecuteRule extends RelRule<Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableStreamingSortMustnotExecuteRule.Config.builder()
                .description(StreamingSortMustnotExecuteRule.class.getSimpleName())
                .operandSupplier(b0 -> b0.operand(SortPhysicalRel.class)
                        .predicate(OptUtils::isUnbounded)
                        .anyInputs()
                )
                .build();

        @Override
        default RelOptRule toRule() {
            return new StreamingSortMustnotExecuteRule(this);
        }
    }

    private StreamingSortMustnotExecuteRule(Config config) {
        super(config);
    }

    @SuppressWarnings("checkstyle:DeclarationOrder")
    public static final RelOptRule INSTANCE = new StreamingSortMustnotExecuteRule(Config.DEFAULT);

    @Override
    public void onMatch(RelOptRuleCall call) {
        Sort sort = call.rel(0);
        if (sort.fetch == null) {
            call.transformTo(
                    new MustnotExecuteRel(sort.getCluster(), sort.getTraitSet(), sort.getRowType(),
                            "Sorting is not supported for a streaming query"));
        }
    }
}
