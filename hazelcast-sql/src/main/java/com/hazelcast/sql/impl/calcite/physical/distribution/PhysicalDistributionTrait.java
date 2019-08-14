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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;

public class PhysicalDistributionTrait implements RelTrait {
    /** Data is distributed in partitioned map. */
    public static final PhysicalDistributionTrait PARTITIONED =
        new PhysicalDistributionTrait(PhysicalDistributionType.PARTITIONED);

    /** Data is distributed in replicated map. */
    public static final PhysicalDistributionTrait REPLICATED =
        new PhysicalDistributionTrait(PhysicalDistributionType.REPLICATED);

    /** Consume the whole stream on a single node. */
    public static final PhysicalDistributionTrait SINGLETON =
        new PhysicalDistributionTrait(PhysicalDistributionType.SINGLETON);

    /** Distribution without any restriction. */
    public static final PhysicalDistributionTrait ANY = new PhysicalDistributionTrait(PhysicalDistributionType.ANY);

    /** Distribution type. */
    private final PhysicalDistributionType type;

    public PhysicalDistributionTrait(PhysicalDistributionType type) {
        this.type = type;
    }

    public PhysicalDistributionType getType() {
        return type;
    }

    @Override
    public RelTraitDef getTraitDef() {
        return PhysicalDistributionTraitDef.INSTANCE;
    }

    @Override
    public boolean satisfies(RelTrait targetTrait) {
        if (targetTrait instanceof PhysicalDistributionTrait) {
            PhysicalDistributionType targetType = ((PhysicalDistributionTrait)targetTrait).getType();

            if (targetType == PhysicalDistributionType.ANY)
                return true;
        }

        return this.equals(targetTrait);
    }

    @Override
    public void register(RelOptPlanner planner) {
        // No-op.
    }

    @Override
    public String toString() {
        return type.name();
    }
}
