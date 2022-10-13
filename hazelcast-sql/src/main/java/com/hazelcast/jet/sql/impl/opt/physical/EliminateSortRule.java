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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.immutables.value.Value;

import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;

@Value.Enclosing
final class EliminateSortRule extends RelRule<RelRule.Config> {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        EliminateSortRule.Config DEFAULT = ImmutableEliminateSortRule.Config.builder()
                .description(EliminateSortRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(SortPhysicalRel.class)
                        .trait(PHYSICAL)
                        .oneInput(b1 ->
                                b1.operand(RelNode.class)
                                        .predicate(EliminateSortRule::matches)
                                        .anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new EliminateSortRule(this);
        }

    }

    private EliminateSortRule(RelRule.Config config) {
        super(config);
    }

    static final RelOptRule INSTANCE = new EliminateSortRule(Config.DEFAULT);

    private static boolean matches(RelNode rel) {
        return rel.getTraitSet().getCollation() != null &&
                !rel.getTraitSet().getCollation().equals(RelCollations.EMPTY);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        SortPhysicalRel sort = call.rel(0);
        RelNode input = call.rel(1);

        return input.getTraitSet().satisfies(sort.getTraitSet());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        call.transformTo(call.rel(1));
    }
}
