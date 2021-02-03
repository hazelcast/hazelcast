/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.opt.distribution;

import com.hazelcast.sql.impl.calcite.opt.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.RootExchangePhysicalRel;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;

import static com.hazelcast.sql.impl.calcite.opt.distribution.DistributionType.ANY;
import static com.hazelcast.sql.impl.calcite.opt.distribution.DistributionType.PARTITIONED;
import static com.hazelcast.sql.impl.calcite.opt.distribution.DistributionType.REPLICATED;
import static com.hazelcast.sql.impl.calcite.opt.distribution.DistributionType.ROOT;

/**
 * Distribution trait definition.
 */
public class DistributionTraitDef extends RelTraitDef<DistributionTrait> {
    /** Partitioned trait with unknown partitioning columns. */
    private final DistributionTrait traitPartitionedUnknown;

    /** Every node has the same data set locally. */
    private final DistributionTrait traitReplicated;

    /** Consume the whole stream on a single node. */
    private final DistributionTrait traitRoot;

    /** Distribution without any restriction. */
    private final DistributionTrait traitAny;

    /** Number of members. */
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

        if (rel.getConvention() != HazelcastConventions.PHYSICAL) {
            // Only physical nodes could be converted.
            return null;
        }

        switch (targetType) {
            case ANY:
                return rel;

            case ROOT:
                return convertToRoot(planner, rel, currentTrait);

            default:
                return null;
        }
    }

    /**
     * Convert to singleton node by adding {@link RootExchangePhysicalRel}.
     * Collation is lost as a result of this transform.
     *
     * @param planner Planner.
     * @param rel Node.
     * @param currentTrait Current distribution trait.
     * @return Converted node.
     */
    private RelNode convertToRoot(RelOptPlanner planner, RelNode rel, DistributionTrait currentTrait) {
        // ANY already handled before, ROOT and REPLICATED do not require further conversions.
        assert currentTrait.getType() == PARTITIONED;

        RelCollation collation = rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
        RelTraitSet traitSet = OptUtils.traitPlus(planner.emptyTraitSet(), getTraitRoot(), collation);

        return new RootExchangePhysicalRel(
            rel.getCluster(),
            OptUtils.toPhysicalConvention(traitSet),
            rel
        );
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
