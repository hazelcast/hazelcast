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

package com.hazelcast.sql.impl.calcite.physical.distribution;

import com.hazelcast.sql.impl.calcite.HazelcastCalciteContext;
import com.hazelcast.sql.impl.calcite.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.RuleUtils;
import com.hazelcast.sql.impl.calcite.physical.rel.SingletonExchangePhysicalRel;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;

public class PhysicalDistributionTraitDef extends RelTraitDef<PhysicalDistributionTrait> {
    public static final PhysicalDistributionTraitDef INSTANCE = new PhysicalDistributionTraitDef();

    @Override
    public Class<PhysicalDistributionTrait> getTraitClass() {
        return PhysicalDistributionTrait.class;
    }

    @Override
    public String getSimpleName() {
        return getClass().getSimpleName();
    }

    @SuppressWarnings("checkstyle:WhitespaceAround")
    @Override
    public RelNode convert(
        RelOptPlanner planner,
        RelNode rel,
        PhysicalDistributionTrait targetTrait,
        boolean allowInfiniteCostConverters
    ) {
        PhysicalDistributionTrait currentTrait = RuleUtils.getPhysicalDistribution(rel);

        // Do nothing if input is already converted.
        if (currentTrait.equals(targetTrait)) {
            return rel;
        }

        // TODO: What is the reason of having this RelSubset check?
//        if (currentTrait.equals(ANY))
        if (currentTrait.equals(PhysicalDistributionTrait.ANY) && !(rel instanceof RelSubset)) {
            return null;
        }

        // Only physical nodes could be converted.
        if (rel.getConvention() != HazelcastConventions.PHYSICAL) {
            return null;
        }

        switch (targetTrait.getType()){
            case SINGLETON:
                if (currentTrait == PhysicalDistributionTrait.REPLICATED && HazelcastCalciteContext.get().isDataMember()) {
                    return rel;
                }

                // TODO: Do we really need this kind of conversions? Can't this be handled on rule level?
                // TODO: E.g. RootPhyicalRule could set proper exchanges on its own.

                return new SingletonExchangePhysicalRel(
                    rel.getCluster(),
                    RuleUtils.toPhysicalConvention(planner.emptyTraitSet(), targetTrait),
                    rel
                );

            case ANY:
                return rel;

            default:
                return null;
        }
    }

    @Override
    public boolean canConvert(
        RelOptPlanner planner,
        PhysicalDistributionTrait fromTrait,
        PhysicalDistributionTrait toTrait
    ) {
        return true;
    }

    @Override
    public PhysicalDistributionTrait getDefault() {
        return PhysicalDistributionTrait.ANY;
    }
}
