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
import org.immutables.value.Value;

import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;

@Value.Enclosing
public final class AssignWatermarkKeyToScansRule extends RelRule<RelRule.Config> {
    private byte keyCounter = 0;

    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableAssignWatermarkKeyToScansRule.Config.builder()
                .description(AssignWatermarkKeyToScansRule.class.getSimpleName())
                .operandSupplier(
                        b -> b.operand(FullScanPhysicalRel.class)
                                .trait(PHYSICAL)
                                .predicate(scan -> OptUtils.isUnbounded(scan)
                                        && scan.watermarkedColumnIndex() >= 0
                                        && scan.getWatermarkKey() == null)
                                .noInputs())
                .build();

        @Override
        default RelOptRule toRule() {
            return new AssignWatermarkKeyToScansRule(this);
        }
    }

    public AssignWatermarkKeyToScansRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FullScanPhysicalRel scan = call.rel(0);
        FullScanPhysicalRel newScan = (FullScanPhysicalRel) scan.copy(scan.getTraitSet(), keyCounter);
        keyCounter++;
        call.transformTo(newScan);
    }
}
