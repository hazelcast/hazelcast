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
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexInputRef;
import org.immutables.value.Value;

import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;

/**
 * Eliminates {@link DropLateItemsPhysicalRel}  if watermarked field
 * was not projected by {@link FullScanPhysicalRel} below.
 */
@Value.Enclosing
public class DropLateItemEliminateRule extends RelRule<RelRule.Config> implements TransformationRule {
    @Value.Immutable
    public interface Config extends RelRule.Config {
        DropLateItemEliminateRule.Config DEFAULT = ImmutableDropLateItemEliminateRule.Config.builder()
                .description(DropLateItemEliminateRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(DropLateItemsPhysicalRel.class)
                        .trait(PHYSICAL)
                        .inputs(b1 -> b1
                                .operand(FullScanPhysicalRel.class)
                                .anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new DropLateItemEliminateRule(this);
        }
    }

    public static final RelOptRule INSTANCE = new DropLateItemEliminateRule(DropLateItemEliminateRule.Config.DEFAULT);

    protected DropLateItemEliminateRule(DropLateItemEliminateRule.Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        DropLateItemsPhysicalRel dropRel = call.rel(0);
        FullScanPhysicalRel scan = call.rel(1);

        return scan.getProjects().stream()
                .filter(r -> r instanceof RexInputRef)
                .map(r -> (RexInputRef) r)
                .noneMatch(r -> r.getIndex() == dropRel.wmField());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        DropLateItemsPhysicalRel dropRel = call.rel(0);
        FullScanPhysicalRel scan = call.rel(1);
        call.transformTo(scan);
    }
}
