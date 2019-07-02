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

import com.hazelcast.sql.impl.calcite.HazelcastConventions;
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

    @Override
    public RelNode convert(
        RelOptPlanner planner,
        RelNode rel,
        PhysicalDistributionTrait targetTrait,
        boolean allowInfiniteCostConverters
    ) {
        PhysicalDistributionTrait currentTrait = rel.getTraitSet().getTrait(PhysicalDistributionTraitDef.INSTANCE);

        if (currentTrait.equals(targetTrait))
            return rel;

        if (currentTrait.equals(PhysicalDistributionTrait.ANY) && !(rel instanceof RelSubset) )
            return null;

        if (rel.getConvention() != HazelcastConventions.PHYSICAL)
            return null;

        switch (targetTrait.getType()){
            case SINGLETON:
                return new SingletonExchangePhysicalRel(
                    rel.getCluster(),
                    planner.emptyTraitSet().plus(HazelcastConventions.PHYSICAL).plus(targetTrait),
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
