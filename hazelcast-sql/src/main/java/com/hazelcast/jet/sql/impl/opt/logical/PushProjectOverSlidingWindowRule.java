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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;

import static org.apache.calcite.plan.Convention.NONE;

public class PushProjectOverSlidingWindowRule extends RelRule<RelRule.Config> {

    private static final RelRule.Config CONFIG = Config.EMPTY
            .withDescription(PushProjectOverSlidingWindowRule.class.getSimpleName())
            .withOperandSupplier(b0 -> b0
                    .operand(LogicalProject.class)
                    .trait(NONE)
                    .inputs(b1 -> b1
                            .operand(SlidingWindowLogicalRel.class)
                            .anyInputs()));

    static final RelOptRule INSTANCE = new PushProjectOverSlidingWindowRule();

    protected PushProjectOverSlidingWindowRule() {
        super(CONFIG);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        SlidingWindowLogicalRel window = call.rel(1);


    }
}
