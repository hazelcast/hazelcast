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

package com.hazelcast.sql.impl.calcite.distribution;

import com.hazelcast.sql.impl.calcite.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.physical.ReplicatedToDistributedPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.BroadcastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.RootExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.UnicastExchangePhysicalRel;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.distribution.DistributionType.ANY;
import static com.hazelcast.sql.impl.calcite.distribution.DistributionType.PARTITIONED;
import static com.hazelcast.sql.impl.calcite.distribution.DistributionType.REPLICATED;
import static com.hazelcast.sql.impl.calcite.distribution.DistributionType.ROOT;

public class DistributionTraitDef extends RelTraitDef<DistributionTrait> {
    public static final DistributionTraitDef INSTANCE = new DistributionTraitDef();

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

        if (currentTrait.equals(targetTrait)) {
            // Input is already converted to the given distribution. Do nothing.
            return rel;
        }

        DistributionType currentType = currentTrait.getType();
        DistributionType targetType = targetTrait.getType();

        // TODO: What is the reason of having this RelSubset check?
//        if (currentTrait.equals(DistributionTrait.ANY_DIST) && !(rel instanceof RelSubset)) {
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

            case PARTITIONED:
                return convertToDistributed(planner, rel, currentTrait, targetTrait);

            case REPLICATED:
                return convertToReplicated(planner, rel, currentTrait);

            case ROOT:
                return convertToRoot(planner, rel, currentTrait);

            default:
                return null;
        }
    }

    /**
     * Convert to {@link DistributionType#PARTITIONED} distribution. Resulting conversion depend on the input distribution.
     *
     * @param planner Planner.
     * @param rel Node.
     * @param currentTrait Current trait.
     * @param targetTrait Target trait.
     * @return Converted node.
     */
    private static RelNode convertToDistributed(RelOptPlanner planner, RelNode rel, DistributionTrait currentTrait,
        DistributionTrait targetTrait) {
        assert targetTrait.getType() == PARTITIONED;

        List<Integer> hashFields = getHashFieldsForDistributedInput(targetTrait);

        if (currentTrait.getType() == REPLICATED) {
            // Since REPLICATED data set is present on all nodes, we do not need to do an exchange. Instead, we partition
            // REPLICATED input by partition columns. Collation is preserved.
            RelCollation collation = OptUtils.getCollation(rel);

            RelTraitSet traitSet = OptUtils.traitPlus(planner.emptyTraitSet(), targetTrait, collation);

            return new ReplicatedToDistributedPhysicalRel(
                rel.getCluster(),
                OptUtils.toPhysicalConvention(traitSet),
                rel,
                hashFields
            );
        } else {
            // TODO: See question about RelSubset above.
            // This should be either ROOT or PARTITIONED, since REPLICATED and ANY types are already handled.
            assert currentTrait.getType() == ROOT || currentTrait.getType() == PARTITIONED;

            // Any PARTITIONED satisfies PARTITIONED without specific fields, so if conversion is required, then
            // we expect distribution fields to exist.
            assert !targetTrait.getFieldGroups().isEmpty();

            // Collation is destroyed.
            RelTraitSet traitSet = OptUtils.traitPlus(planner.emptyTraitSet(), targetTrait);

            return new UnicastExchangePhysicalRel(
                rel.getCluster(),
                OptUtils.toPhysicalConvention(traitSet),
                rel,
                hashFields
            );
        }
    }

    private static List<Integer> getHashFieldsForDistributedInput(DistributionTrait targetTrait) {
        if (!targetTrait.hasFieldGroups()) {
            // No distribution field, return null, what means "all fields".
            return null;
        } else {
            // Pick fields from the first group. It is safe, since all the groups in the given trait are equivalent.
            List<DistributionField> fields = targetTrait.getFieldGroups().get(0);

            List<Integer> res = new ArrayList<>(fields.size());

            for (DistributionField field : fields) {
                // Nested fields should be eliminated at this point.
                assert field.getNestedField() == null;

                res.add(field.getIndex());
            }

            assert res.size() > 0;

            return res;
        }
    }

    /**
     * Convert to {@link DistributionType#REPLICATED} node by adding {@link BroadcastExchangePhysicalRel}.
     * Collation is lost as a result of this transform.
     *
     * @param planner Planner.
     * @param rel Node.
     * @param currentTrait Current distribution trait.
     * @return Converted node.
     */
    private static RelNode convertToReplicated(RelOptPlanner planner, RelNode rel, DistributionTrait currentTrait) {
        assert currentTrait.getType() == PARTITIONED || currentTrait.getType() == ROOT;

        RelTraitSet traitSet = OptUtils.traitPlus(planner.emptyTraitSet(), DistributionTrait.REPLICATED_DIST);

        return new BroadcastExchangePhysicalRel(
            rel.getCluster(),
            OptUtils.toPhysicalConvention(traitSet),
            rel
        );
    }

    /**
     * Convert to singleton node by adding {@link UnicastExchangePhysicalRel}.
     * Collation is lost as a result of this transform.
     *
     * @param planner Planner.
     * @param rel Node.
     * @param currentTrait Current distribution trait.
     * @return Converted node.
     */
    private static RelNode convertToRoot(RelOptPlanner planner, RelNode rel, DistributionTrait currentTrait) {
        // ANY already handler before, ROOT and REPLICATED do not require further conversions.
        assert currentTrait.getType() == PARTITIONED;

        RelTraitSet traitSet = OptUtils.traitPlus(planner.emptyTraitSet(), DistributionTrait.ROOT_DIST);

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
        return DistributionTrait.ANY_DIST;
    }
}
