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

package com.hazelcast.jet.sql.impl.opt.distribution;

import com.hazelcast.jet.sql.impl.opt.Conventions;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;

import static com.hazelcast.jet.sql.impl.opt.distribution.DistributionType.ANY;
import static com.hazelcast.jet.sql.impl.opt.distribution.DistributionType.PARTITIONED;
import static com.hazelcast.jet.sql.impl.opt.distribution.DistributionType.REPLICATED;
import static com.hazelcast.jet.sql.impl.opt.distribution.DistributionType.ROOT;

/**
 * Distribution trait definition.
 */
public class DistributionTraitDef extends RelTraitDef<DistributionTrait> {
    /**
     * Partitioned trait with unknown partitioning columns.
     */
    private final DistributionTrait traitPartitionedUnknown;

    /**
     * Every node has the same data set locally.
     */
    private final DistributionTrait traitReplicated;

    /**
     * Consume the whole stream on a single node.
     */
    private final DistributionTrait traitRoot;

    /**
     * Distribution without any restriction.
     */
    private final DistributionTrait traitAny;

    /**
     * Number of members.
     */
    private final int memberCount;

    public DistributionTraitDef(int memberCount) {
        this.memberCount = memberCount;

        traitPartitionedUnknown = createTrait(PARTITIONED);
        traitReplicated = createTrait(REPLICATED);
        traitRoot = createTrait(ROOT);
        traitAny = createTrait(ANY);
    }

    public int getMemberCount() {
        return memberCount;
    }

    public DistributionTrait getTraitPartitionedUnknown() {
        return traitPartitionedUnknown;
    }

    public DistributionTrait getTraitReplicated() {
        return traitReplicated;
    }

    public DistributionTrait getTraitRoot() {
        return traitRoot;
    }

    @Override
    public Class<DistributionTrait> getTraitClass() {
        return DistributionTrait.class;
    }

    @Override
    public String getSimpleName() {
        return getClass().getSimpleName();
    }

    @Override
    public RelNode convert(
            RelOptPlanner planner,
            RelNode rel,
            DistributionTrait targetTrait,
            boolean allowInfiniteCostConverters
    ) {
        DistributionTrait currentTrait = OptUtils.getDistribution(rel);

        if (currentTrait.satisfies(targetTrait)) {
            return rel;
        }

        DistributionType currentType = currentTrait.getType();
        DistributionType targetType = targetTrait.getType();

        if (currentType == ANY) {
            // Abstract ANY distribution cannot be converted to anything.
            return null;
        }

        if (rel.getConvention() != Conventions.PHYSICAL) {
            // Only physical nodes could be converted.
            return null;
        }

        switch (targetType) {
            case ANY:
                return rel;

            // ROOT target type also was supposed to be supported for root node exchange.
            // Can be reusable if we'd like to support ReplicatedMap (and use DistributionTrait).
            case ROOT:
            default:
                return null;
        }
    }

    @Override
    public boolean canConvert(
            RelOptPlanner planner,
            DistributionTrait fromTrait,
            DistributionTrait toTrait
    ) {
        return true;
    }

    @Override
    public DistributionTrait getDefault() {
        return traitAny;
    }

    private DistributionTrait createTrait(DistributionType type) {
        return new DistributionTrait(this, type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DistributionTraitDef that = (DistributionTraitDef) o;

        return memberCount == that.memberCount;
    }

    @Override
    public int hashCode() {
        return memberCount;
    }

    @Override
    public String toString() {
        return "DistributionTraitDef {memberCount=" + memberCount + '}';
    }
}
