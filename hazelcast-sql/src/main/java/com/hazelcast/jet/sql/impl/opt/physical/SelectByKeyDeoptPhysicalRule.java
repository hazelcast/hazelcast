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
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;

@Value.Enclosing
final class SelectByKeyDeoptPhysicalRule extends RelRule<RelRule.Config> {

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableSelectByKeyDeoptPhysicalRule.Config.builder()
                .description(SelectByKeyDeoptPhysicalRule.class.getSimpleName())
                .operandSupplier(b0 -> b0.operand(RelNode.class)
                        .trait(PHYSICAL)
                        .predicate(OptUtils::isBounded)
                        .unorderedInputs(
                                b1 -> b1.operand(SelectByKeyMapPhysicalRel.class)
                                        .anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new SelectByKeyDeoptPhysicalRule(this);
        }
    }

    @SuppressWarnings("checkstyle:DeclarationOrder")
    static final RelOptRule INSTANCE = new SelectByKeyDeoptPhysicalRule(Config.DEFAULT);

    private SelectByKeyDeoptPhysicalRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode parentNode = call.rel(0);
        SelectByKeyMapPhysicalRel selectRel = call.rel(1);

        // Note: current optimization cannot happen with streaming source.
        FullScanPhysicalRel fullScanRel = new FullScanPhysicalRel(
                selectRel.getCluster(),
                selectRel.getTraitSet(),
                selectRel.getTable(),
                null,
                -1
        );

        List<RelNode> inputs = new ArrayList<>(parentNode.getInputs());
        for (int i = 0; i < inputs.size(); ++i) {
            RelNode input = inputs.get(i);
            if (input.equals(selectRel)) {
                inputs.set(i, fullScanRel);
                break;
            }

            if (input instanceof RelSubset && ((RelSubset) input).getBest().equals(selectRel)) {
                inputs.set(i, fullScanRel);
                break;
            }
        }

        RelNode newParentNode = parentNode.copy(parentNode.getTraitSet(), inputs);
        call.transformTo(newParentNode);
    }
}
