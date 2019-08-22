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

import com.hazelcast.sql.impl.calcite.HazelcastCalciteContext;
import com.hazelcast.sql.impl.calcite.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.RuleUtils;
import com.hazelcast.sql.impl.calcite.logical.rel.RootLogicalRel;
import com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionTrait;
import com.hazelcast.sql.impl.calcite.physical.rel.RootPhysicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.SingletonExchangePhysicalRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionTrait.REPLICATED;
import static com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionTrait.SINGLETON;

/**
 * Rule to convert the logical root node to physical root node.
 */
public class RootPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new RootPhysicalRule();

    private RootPhysicalRule() {
        super(
            RuleUtils.parentChild(RootLogicalRel.class, RelNode.class, HazelcastConventions.LOGICAL),
            RootPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RootLogicalRel logicalRoot = call.rel(0);
        RelNode input = call.rel(1);

        RelNode convertedInput = RuleUtils.toPhysicalInput(input);

        Collection<RelNode> transformInputs = getPhysicalInputs(convertedInput);

        assert transformInputs.size() > 0;

        for (RelNode transformInput : transformInputs) {
            RootPhysicalRel transformedRoot = new RootPhysicalRel(
                logicalRoot.getCluster(),
                transformInput.getTraitSet().plus(PhysicalDistributionTrait.SINGLETON),
                transformInput
            );

            call.transformTo(transformedRoot);
        }
    }

    /**
     * Get possible inputs for this relation.
     *
     * @param convertedInput Converted input.
     * @return Possible inputs.
     */
    private static Collection<RelNode> getPhysicalInputs(RelNode convertedInput) {
        RelOptCluster cluster = convertedInput.getCluster();

        Collection<RelNode> res = new ArrayList<>(1);

        // Iterate over possible physical inputs and try to
        for (RelNode physicalInput : RuleUtils.getPhysicalRelsFromSubset(convertedInput)) {
            PhysicalDistributionTrait dist = RuleUtils.getPhysicalDistribution(physicalInput);

            boolean needExchange = true;

            if (dist == SINGLETON) {
                // Underlying stream is local and contains the whole data set, no need for exchanges.
                needExchange = false;
            }
            else if (dist == REPLICATED) {
                // For REPLICATED distribution the exchange may be avoided only if local node is data node,
                // what implies that the data is available locally.
                if (HazelcastCalciteContext.get().isDataMember())
                    needExchange = false;
            }

            // In all other cases the exchange is needed, since input is not local to the root node.
            if (needExchange) {
                SingletonExchangePhysicalRel exchange = new SingletonExchangePhysicalRel(
                    cluster,
                    RuleUtils.toPhysicalConvention(cluster.getPlanner().emptyTraitSet(), SINGLETON),
                    physicalInput
                );

                res.add(exchange);
            }
            else
                res.add(physicalInput);
        }

        // If we failed to get any meaningful physical input, then just enforce SINGLETON distribution.
        if (res.isEmpty()) {
            RelNode physicalInput = RuleUtils.toPhysicalInput(convertedInput, SINGLETON);

            res.add(physicalInput);
        }

        return res;
    }
}
