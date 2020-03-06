/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.sql.impl.optimizer.OptimizerRuleCallTracker;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Base class for physical rules.
 */
public abstract class AbstractPhysicalRule extends RelOptRule {
    protected AbstractPhysicalRule(RelOptRuleOperand operand) {
        super(operand);
    }

    protected AbstractPhysicalRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    protected AbstractPhysicalRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public final void onMatch(RelOptRuleCall call) {
        HazelcastRelOptCluster cluster = (HazelcastRelOptCluster) call.getPlanner().getRoot().getCluster();

        OptimizerRuleCallTracker ruleCallTracker = cluster.getRuleCallTracker();

        if (ruleCallTracker != null) {
            ruleCallTracker.onRuleCall(this.getClass().getSimpleName());
        }

        onMatch0(call);
    }

    protected abstract void onMatch0(RelOptRuleCall call);
}
