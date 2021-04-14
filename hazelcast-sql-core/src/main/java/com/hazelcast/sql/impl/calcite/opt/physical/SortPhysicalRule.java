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

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.sql.impl.calcite.opt.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTrait;
import com.hazelcast.sql.impl.calcite.opt.logical.SortLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.SortMergeExchangePhysicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The rule that converts logical sort into its physical counterpart. In the current release
 * we support only a limited case that relies on the locally pre-sorted indexes to avoid local sorting.
 * <p>
 * However, if the input is located on a few nodes, and extra merge sort {@link SortMergeExchangePhysicalRel}
 * is created to sort locally pre-sorted entries.
 */
public final class SortPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new SortPhysicalRule();

    private SortPhysicalRule() {
        super(
                OptUtils.parentChild(SortLogicalRel.class, RelNode.class, HazelcastConventions.LOGICAL),
                SortPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        SortLogicalRel sort = call.rel(0);
        RelNode input = sort.getInput();

        Collection<RelNode> transforms = getTransforms(sort, input);

        for (RelNode transform : transforms) {
            call.transformTo(transform);
        }
    }

    /**
     * Get possible transformations for the given input.
     *
     * @param logicalSort    logical sort.
     * @param convertedInput Converted input.
     * @return Possible transforms.
     */
    private Collection<RelNode> getTransforms(SortLogicalRel logicalSort, RelNode convertedInput) {
        List<RelNode> requiresLocalSortRels = new ArrayList<>(1);
        List<RelNode> noLocalSortRels = new ArrayList<>(1);

        for (RelNode physicalInput : OptUtils.getPhysicalRelsFromSubset(convertedInput)) {
            // Check whether local sorting is needed
            boolean requiresLocalSort = requiresLocalSort(
                    logicalSort.getCollation(),
                    physicalInput.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE)
            );

            RelNode rel;

            DistributionTrait physicalInputDist = OptUtils.getDistribution(physicalInput);
            boolean isFullResultOnAll = physicalInputDist.isFullResultSetOnAllParticipants();

            if (requiresLocalSort || isFullResultOnAll) {
                // If the input is pre-sorted, the SortPhysicalRel is doing no-op
                rel = createLocalSort(logicalSort, physicalInput, requiresLocalSort, isFullResultOnAll);
            } else {
                rel = physicalInput;
            }

            // Add merge phase if needed.
            if (!isFullResultOnAll) {
                rel = createMerge(rel, logicalSort);
            }

            // Add to the list of transformations.
            if (requiresLocalSort) {
                requiresLocalSortRels.add(rel);
            } else {
                noLocalSortRels.add(rel);
            }
        }

        // Pick up a plan that doesn't require local sorting,
        // otherwise an exception will be thrown when local sort
        // is touched in runtime.
        if (noLocalSortRels.size() > 0) {
            return noLocalSortRels;
        } else {
            return requiresLocalSortRels;
        }
    }

    /**
     * Check if local sorting phase is needed. It could be avoided iff sort collation is a prefix of input collation.
     *
     * @return {@code true} if local sorting is needed, {@code false} otherwise.
     */
    private static boolean requiresLocalSort(RelCollation sortCollation, RelCollation inputCollation) {
        if (sortCollation.getFieldCollations().isEmpty()) {
            // No need for sorting
            return false;
        }

        List<RelFieldCollation> sortFields = sortCollation.getFieldCollations();
        List<RelFieldCollation> inputFields = inputCollation.getFieldCollations();

        if (sortFields.size() <= inputFields.size()) {
            for (int i = 0; i < sortFields.size(); i++) {
                RelFieldCollation sortField = sortFields.get(i);
                RelFieldCollation inputField = inputFields.get(i);

                // Different collation, local sorting is needed.
                if (!sortField.equals(inputField)) {
                    return true;
                }
            }

            // Prefix is confirmed, no local sorting is needed.
            return false;
        } else {
            // Input has less collated fields than sort. Definitely not a prefix => local sorting is needed.
            return true;
        }
    }

    private static SortPhysicalRel createLocalSort(SortLogicalRel logicalSort, RelNode physicalInput, boolean requiresLocalSort,
                                                   boolean isFullResultOnAll) {
        // Input traits are propagated, but new collation is used.
        RelTraitSet traitSet = OptUtils.traitPlus(physicalInput.getTraitSet(),
                logicalSort.getCollation()
        );

        // Don't push down the fetch/offset operators if merging phase is needed
        return new SortPhysicalRel(
                logicalSort.getCluster(),
                traitSet,
                physicalInput,
                logicalSort.getCollation(),
                requiresLocalSort,
                isFullResultOnAll ? logicalSort.offset : null,
                isFullResultOnAll ? logicalSort.fetch : null
        );
    }

    private static RelNode createMerge(RelNode physicalInput, SortLogicalRel logicalSort) {
        RelTraitSet traitSet = OptUtils.traitPlus(physicalInput.getTraitSet(),
                logicalSort.getCollation(),
                OptUtils.getDistributionDef(physicalInput).getTraitRoot()
        );

        // Empty collation means the fetch-only operation
        return new SortMergeExchangePhysicalRel(
                logicalSort.getCluster(),
                traitSet,
                physicalInput,
                logicalSort.getCollation(),
                logicalSort.fetch,
                logicalSort.offset
        );
    }
}
