/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.physical.rule;

import com.hazelcast.sql.impl.calcite.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.RuleUtils;
import com.hazelcast.sql.impl.calcite.logical.rel.FilterLogicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.FilterPhysicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * This rule converts logical filter into physical filter. Physical projection inherits distribution of the
 * underlying operator.
 */
public class FilterPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new FilterPhysicalRule();

    private FilterPhysicalRule() {
        super(
            RuleUtils.parentChild(FilterLogicalRel.class, RelNode.class, HazelcastConventions.LOGICAL),
            FilterPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FilterLogicalRel filter = call.rel(0);
        RelNode input = filter.getInput();

        RelNode convertedInput = RuleUtils.toPhysicalInput(input);

        Collection<RelNode> transformedInputs = getTransformedInputs(convertedInput);

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

    /**
     * Get inputs which should be used for transformation.
     *
     * @param convertedInput Original input in physical convention.
     * @return Inputs which should be used for transformation.
     */
    private Collection<RelNode> getTransformedInputs(RelNode convertedInput) {
        Set<RelNode> res = Collections.newSetFromMap(new IdentityHashMap<>());

        if (convertedInput instanceof RelSubset) {
            Set<RelTraitSet> traitSets = RuleUtils.getPhysicalTraitSets((RelSubset) convertedInput);

            for (RelTraitSet traitSet : traitSets) {
                // Get an input with the given trait.
                RelNode convertedInput0 = convert(convertedInput, traitSet);

                res.add(convertedInput0);
            }
        }

        if (res.isEmpty())
            res.add(convertedInput);

        return res;
    }
}
