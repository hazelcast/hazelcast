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

package com.hazelcast.sql.impl.calcite.opt.physical.agg;

import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTrait;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTraitDef;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionType;
import com.hazelcast.sql.impl.calcite.opt.logical.AggregateLogicalRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Collections;
import java.util.List;

/**
 * Distribution of the local aggregate, inherited from its input.
 */
public final class AggregateDistribution {
    /** Whether the aggregate is collocated. */
    private final boolean collocated;

    /** Distribution which should be applied to the local aggregate. */
    private final DistributionTrait distribution;

    private AggregateDistribution(boolean collocated, DistributionTrait distribution) {
        this.collocated = collocated;
        this.distribution = distribution;
    }

    public static AggregateDistribution of(AggregateLogicalRel agg, RelNode physicalInput) {
        ImmutableBitSet groupSet = agg.getGroupSet();
        DistributionTrait inputDistribution = OptUtils.getDistribution(physicalInput);

        return of(groupSet, inputDistribution, OptUtils.getDistributionDef(agg));
    }

    private static AggregateDistribution of(
        ImmutableBitSet aggGroupSet,
        DistributionTrait inputDistribution,
        DistributionTraitDef distributionTraitDef) {
        switch (inputDistribution.getType()) {
            case ROOT:
                // Always collocated for ROOT, since there is only one stream of data.
                return new AggregateDistribution(true, distributionTraitDef.getTraitRoot());

            case REPLICATED:
                // Always collocated for REPLICATED, since the same stream is present on all members.
                return new AggregateDistribution(true, distributionTraitDef.getTraitReplicated());

            case PARTITIONED:
                return ofDistributed(aggGroupSet, inputDistribution.getFieldGroups(), distributionTraitDef);

            default:
                // Default (ANY) distribution - not collocated, output is distributed, but distribution columns are unknown.
                assert inputDistribution.getType() == DistributionType.ANY;

                return new AggregateDistribution(false, distributionTraitDef.getTraitPartitionedUnknown());
        }
    }

    /**
     * Get aggregate distribution for the distributed input.
     *
     * @param aggGroupSet Aggregate group set.
     * @param inputFieldGroups Input field groups.
     * @return Aggregate distribution.
     */
    private static AggregateDistribution ofDistributed(
        ImmutableBitSet aggGroupSet,
        List<List<Integer>> inputFieldGroups,
        DistributionTraitDef distributionTraitDef
    ) {
        for (List<Integer> inputFieldGroup : inputFieldGroups) {
            if (isCollocated(aggGroupSet, inputFieldGroup)) {
                DistributionTrait distribution = distributionTraitDef.createPartitionedTrait(
                    Collections.singletonList(inputFieldGroup)
                );

                return new AggregateDistribution(true, distribution);
            }
        }

        // No collocated inputs were found. Input distribution is lost.
        return new AggregateDistribution(false, distributionTraitDef.getTraitPartitionedUnknown());
    }

    /**
     * Check whether the given group set could be executed in collocated mode for the given distribution fields of
     * partitioned input. This is the case iff input distribution fields are a prefix of the group set.
     *
     * @param aggGroupSet Group set of original aggregate.
     * @param inputFieldGroup Field group.
     * @return {@code true} if this aggregate could be processed in collocated mode.
     */
    private static boolean isCollocated(ImmutableBitSet aggGroupSet, List<Integer> inputFieldGroup) {
        // If group set size is less than the number of input distribution fields, then dist fields could not be a
        // prefix of group by definition.
        if (aggGroupSet.length() < inputFieldGroup.size()) {
            // GROUP BY has less attributes than the number of distribution fields. It means that at least one distribution field
            // will be lost, no need to continue.
            return false;
        }

        for (int i = 0; i < inputFieldGroup.size(); i++) {
            if (!aggGroupSet.get(i)) {
                // GROUP BY doesn't contain input distribution field. Distribution is lost.
                return false;
            }
        }

        return true;
    }

    public boolean isCollocated() {
        return collocated || distribution.getMemberCount() == 1;
    }

    public DistributionTrait getDistribution() {
        return distribution;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{collocated=" + collocated + ", distribution=" + distribution + '}';
    }
}
