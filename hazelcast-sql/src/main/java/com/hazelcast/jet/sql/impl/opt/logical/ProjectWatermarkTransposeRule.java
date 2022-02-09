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
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.tools.RelBuilder;

import java.util.Collections;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

/**
 * A `Project` under `Watermark` will be moved below.
 */
public class ProjectWatermarkTransposeRule extends RelRule<Config> implements TransformationRule {

    private static final Config CONFIG = Config.EMPTY
            .withDescription(ProjectWatermarkTransposeRule.class.getSimpleName())
            .withOperandSupplier(b0 -> b0
                    .operand(WatermarkLogicalRel.class)
                    .trait(LOGICAL)
                    .inputs(b1 -> b1
                            .operand(ProjectLogicalRel.class).anyInputs()));

    public static final RelOptRule WATERMARK_PROJECT_TRANSPOSE = new ProjectWatermarkTransposeRule(CONFIG);

    protected ProjectWatermarkTransposeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final WatermarkLogicalRel wmRel = call.rel(0);
        final ProjectLogicalRel project = call.rel(1);

        final WatermarkLogicalRel newWatermarkRel = (WatermarkLogicalRel) wmRel.copy(
                wmRel.getTraitSet(),
                Collections.singletonList(project.getInput())
        );

        final RelBuilder relBuilder = call.builder();

        relBuilder.push(newWatermarkRel).project(project.getProjects(), project.getRowType().getFieldNames());

        call.transformTo(relBuilder.build());
    }
}
