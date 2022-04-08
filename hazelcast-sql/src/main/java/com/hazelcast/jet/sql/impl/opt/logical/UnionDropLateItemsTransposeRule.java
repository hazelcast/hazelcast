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
import java.util.Objects;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

/**
 * Logical rule that eliminates all input {@link DropLateItemsLogicalRel} for {@link Union}.
 * Note: <b>every</b> Union's input rel should be {@link DropLateItemsLogicalRel}.
 * <p>
 * Before:
 * <pre>
 * Union[all=true]
 *   DropLateItemsLogicalRel[...]
 *   ...
 *   DropLateItemsLogicalRel[...]
 * </pre>
 * After:
 * <pre>
 * DropLateItemsLogicalRel[...]
 *  Union[all=true]
 * </pre>
 */
@Value.Enclosing
public class UnionDropLateItemsTransposeRule extends RelRule<RelRule.Config> implements TransformationRule {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableUnionDropLateItemsTransposeRule.Config.builder()
                .description(UnionDropLateItemsTransposeRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(Union.class)
                        .trait(LOGICAL)
                        .predicate(union -> union.all)
                        .inputs(b1 -> b1
                                .operand(DropLateItemsLogicalRel.class)
                                .anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new UnionDropLateItemsTransposeRule(this);
        }
    }

    public static final RelOptRule INSTANCE = new UnionDropLateItemsTransposeRule(Config.DEFAULT);

    protected UnionDropLateItemsTransposeRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Union union = call.rel(0);
        return union.getInputs()
                .stream()
                .map(rel -> (RelSubset) rel)
                .allMatch(rel -> rel.getBest() instanceof DropLateItemsLogicalRel);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Union union = call.rel(0);
        DropLateItemsLogicalRel dropRel = call.rel(1);

        List<RelNode> inputs = new ArrayList<>(union.getInputs().size());

        for (RelNode node : union.getInputs()) {
            inputs.add(((DropLateItemsLogicalRel) Objects.requireNonNull(((RelSubset) node).getBest())).getInput());

        }

        Union newUnion = (Union) union.copy(union.getTraitSet(), inputs);
        DropLateItemsLogicalRel dropLateItemsRel = new DropLateItemsLogicalRel(
                dropRel.getCluster(),
                dropRel.getTraitSet(),
                newUnion,
                dropRel.wmField(),
                dropRel.allowedLagProvider()
        );
        call.transformTo(dropLateItemsRel);
    }


}
