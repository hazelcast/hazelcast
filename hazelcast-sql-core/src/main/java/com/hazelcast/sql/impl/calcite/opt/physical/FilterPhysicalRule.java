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

import com.hazelcast.sql.impl.calcite.opt.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.logical.FilterLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

import java.util.Collection;

/**
 * Converts logical filter to physical filter. Collation and distribution are inherited from the input.
 */
public final class FilterPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new FilterPhysicalRule();

    private FilterPhysicalRule() {
        super(
            OptUtils.parentChild(FilterLogicalRel.class, RelNode.class, HazelcastConventions.LOGICAL),
            FilterPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FilterLogicalRel filter = call.rel(0);
        RelNode input = filter.getInput();

        RelNode convertedInput = OptUtils.toPhysicalInput(input);

        Collection<RelNode> transformedInputs = getInputTransforms(convertedInput);

        for (RelNode transformedInput : transformedInputs) {
            FilterPhysicalRel newFilter = new FilterPhysicalRel(
                filter.getCluster(),
                transformedInput.getTraitSet(),
                transformedInput,
                filter.getCondition()
            );

            call.transformTo(newFilter);
        }
    }

    private Collection<RelNode> getInputTransforms(RelNode convertedInput) {
        return OptUtils.getPhysicalRelsFromSubset(convertedInput);
    }
}
