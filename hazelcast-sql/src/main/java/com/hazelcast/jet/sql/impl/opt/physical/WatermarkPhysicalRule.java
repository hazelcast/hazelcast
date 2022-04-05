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

import com.hazelcast.jet.sql.impl.opt.logical.WatermarkLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.TransformationRule;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.OptUtils.toPhysicalConvention;

final class WatermarkPhysicalRule extends RelOptRule implements TransformationRule {

    static final RelOptRule INSTANCE = new WatermarkPhysicalRule();

    private WatermarkPhysicalRule() {
        super(
                operand(
                        WatermarkLogicalRel.class,
                        LOGICAL,
                        any()
                ),
                WatermarkPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        call.transformTo(
                new MustNotExecutePhysicalRel(
                        rel.getCluster(),
                        toPhysicalConvention(rel.getTraitSet()),
                        rel.getRowType(),
                        "Ordering function cannot be applied to input table"));
    }
}
