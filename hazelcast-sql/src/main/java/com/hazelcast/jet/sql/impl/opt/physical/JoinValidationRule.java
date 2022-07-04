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

import com.hazelcast.jet.sql.impl.opt.logical.JoinLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rel.rules.TransformationRule;
import org.immutables.value.Value;

import static com.hazelcast.jet.sql.impl.opt.OptUtils.isUnbounded;
import static com.hazelcast.jet.sql.impl.opt.OptUtils.toPhysicalConvention;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;

@Value.Enclosing
public final class JoinValidationRule extends RelRule<Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableJoinValidationRule.Config.builder()
                .description(JoinValidationRule.class.getSimpleName())
                .operandSupplier(b0 -> b0.operand(JoinLogicalRel.class)
                        .anyInputs()
                )
                .build();

        @Override
        default RelOptRule toRule() {
            return new JoinValidationRule(this);
        }
    }

    private JoinValidationRule(Config config) {
        super(config);
    }

    @SuppressWarnings("checkstyle:DeclarationOrder")
    public static final RelOptRule INSTANCE = new JoinValidationRule(Config.DEFAULT);

    @Override
    public void onMatch(RelOptRuleCall call) {
        JoinLogicalRel join = call.rel(0);
        boolean rightInputIsStream = isUnbounded(join.getRight());

        if (join.getJoinType() == LEFT) {
            if (rightInputIsStream) {
                call.transformTo(
                        new MustNotExecutePhysicalRel(
                                join.getCluster(),
                                toPhysicalConvention(join.getTraitSet()),
                                join.getRowType(),
                                "The right side of a LEFT JOIN or the left side of RIGHT JOIN cannot be a streaming source"));
            }
        } else if (join.getJoinType() == INNER) {
            if (rightInputIsStream) {
                call.transformTo(
                        new MustNotExecutePhysicalRel(
                                join.getCluster(),
                                toPhysicalConvention(join.getTraitSet()),
                                join.getRowType(),
                                "The right side of an INNER JOIN cannot be a streaming source"));
            }
        }
    }
}
