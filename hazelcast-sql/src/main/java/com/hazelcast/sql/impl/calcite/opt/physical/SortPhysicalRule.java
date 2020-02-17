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

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.sql.impl.calcite.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.distribution.DistributionTrait;
import com.hazelcast.sql.impl.calcite.distribution.DistributionType;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.logical.SortLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.RootSingletonSortMergeExchangePhysicalRel;
import org.apache.calcite.plan.HazelcastRelOptCluster;
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

import static com.hazelcast.sql.impl.calcite.distribution.DistributionType.REPLICATED;
import static com.hazelcast.sql.impl.calcite.distribution.DistributionType.ROOT;

/**
 * Rule which converts logical sort into it's physical counterpart. There are several forms of physical implementations:
 * <ul>
 *     <li><b>Local</b> - in case the whole input is available locally (ROOT, REPLICATED)</li>
 *     <li><b>Two-phase (local + merge)</b> - in case the input is located on several nodes. In this case a
 *     {@link RootSingletonSortMergeExchangePhysicalRel} is created on top of local sort</li>
 * </ul>
 * <p>
 * Local component may be removed altogether in case the input is already sorted on required attributes.
 */
public final class SortPhysicalRule extends AbstractPhysicalRule {
    public static final RelOptRule INSTANCE = new SortPhysicalRule();

    private SortPhysicalRule() {
        super(
            OptUtils.parentChild(SortLogicalRel.class, RelNode.class, HazelcastConventions.LOGICAL),
            SortPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch0(RelOptRuleCall call) {
        SortLogicalRel sort = call.rel(0);
        RelNode input = sort.getInput();

        RelNode convertedInput = OptUtils.toPhysicalInput(input);

        Collection<RelNode> transforms = getTransforms(sort, convertedInput);

        for (RelNode transform : transforms) {
            call.transformTo(transform);
        }
    }

    /**
     * Get possible transformations for the given input.
     *
     * @param logicalSort Sort.
     * @param convertedInput Converted input.
     * @return Possible transforms.
     */
    private Collection<RelNode> getTransforms(SortLogicalRel logicalSort, RelNode convertedInput) {
        List<RelNode> res = new ArrayList<>(1);

        boolean singleMember = HazelcastRelOptCluster.isSingleMember();

        for (RelNode physicalInput : OptUtils.getPhysicalRelsFromSubset(convertedInput)) {
            DistributionTrait physicalInputDist = OptUtils.getDistribution(physicalInput);

            boolean requiresLocalPhase = requiresLocalPhase(logicalSort.getCollation(), physicalInput);
            boolean requiresDistributedPhase = requiresDistributedPhase(physicalInputDist);

            RelNode rel;

            if (requiresDistributedPhase && !singleMember) {
                RelNode relInput;

                if (requiresLocalPhase) {
                    // Both distributed and local phases are needed.
                    relInput = createLocalSort(logicalSort, physicalInput);
                } else {
                    // Only distributed phase is needed, since input is already sorted locally.
                    relInput = physicalInput;
                }

                rel = createDistributedSort(logicalSort, relInput);
            } else {
                if (requiresLocalPhase) {
                    // Only local sorting is needed.
                    rel = createLocalSort(logicalSort, physicalInput);
                } else {
                    // The best case - sorting is eliminated completely.
                    rel = physicalInput;
                }
            }

            res.add(rel);
        }

        // If no physical inputs were found, declare desired transformation on the input.
        if (res.isEmpty()) {
            boolean requiresLocalPhase = requiresLocalPhase(logicalSort.getCollation(), convertedInput);

            RelNode rel;

            if (requiresLocalPhase) {
                rel = createLocalSort(logicalSort, convertedInput);
            } else {
                rel = convertedInput;
            }

            if (!singleMember) {
                rel = createDistributedSort(logicalSort, rel);
            }

            res.add(rel);
        }

        return res;
    }

    /**
     * Check if local sorting phase is needed. It could be avoided iff sort collation is a prefix of input collation.
     *
     * @param sortCollation Sort collation.
     * @param input Input.
     * @return {@code True} if local sorting is needed, {@code false} otherwise.
     */
    private static boolean requiresLocalPhase(RelCollation sortCollation, RelNode input) {
        RelCollation inputCollation = input.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

        List<RelFieldCollation> sortFields = sortCollation.getFieldCollations();
        List<RelFieldCollation> inputFields = inputCollation.getFieldCollations();

        if (sortFields.size() <= inputFields.size()) {
            for (int i = 0; i < sortFields.size(); i++) {
                RelFieldCollation sortField = sortFields.get(i);
                RelFieldCollation inputField = inputFields.get(i);

                // Different collation, not a prefix => local sorting is needed.
                if (!sortField.equals(inputField)) {
                    return true;
                }
            }

            // Prefix is confirmed => no local sorting is needed.
            return false;
        } else {
            // Input has less collated fields than sort. Definitely not a prefix => local sorting is needed.
            return true;
        }
    }

    /**
     * Check if distributed sorting is needed for the given input. Not the case for inputs having the full data
     * set readily available.
     *
     * @param inputDist Input distribution.
     * @return {@code True} if distributed sorting is needed.
     */
    private static boolean requiresDistributedPhase(DistributionTrait inputDist) {
        DistributionType inputDistType = inputDist.getType();

        return !(inputDistType == ROOT || inputDistType == REPLICATED);
    }

    /**
     * Create local sort. Input traits are preserved except of collation, which is taken from the logical sort.
     *
     * @param logicalSort Logical sort.
     * @param physicalInput Physical input.
     * @return Local sort.
     */
    private static SortPhysicalRel createLocalSort(SortLogicalRel logicalSort, RelNode physicalInput) {
        // Input traits are propagated, but new collation is used.
        RelTraitSet traitSet = OptUtils.traitPlus(physicalInput.getTraitSet(),
            logicalSort.getCollation()
        );

        return new SortPhysicalRel(
            logicalSort.getCluster(),
            traitSet,
            physicalInput,
            logicalSort.getCollation(),
            logicalSort.offset,
            logicalSort.fetch
        );
    }

    /**
     * Create distributed sort. Only physical nature of the input is preserved. Collation is taken from the sort,
     * distribution is always SINGLETON by definition of {@link RootSingletonSortMergeExchangePhysicalRel}.
     *
     * @param logicalSort Logical sort.
     * @param physicalInput Physical input.
     * @return Physical distributed sort.
     */
    private static RootSingletonSortMergeExchangePhysicalRel createDistributedSort(SortLogicalRel logicalSort,
                                                                                   RelNode physicalInput) {
        RelTraitSet traitSet = OptUtils.traitPlus(physicalInput.getTraitSet(),
            logicalSort.getCollation(),
            DistributionTrait.ROOT_DIST
        );

        return new RootSingletonSortMergeExchangePhysicalRel(
            logicalSort.getCluster(),
            traitSet,
            physicalInput,
            logicalSort.getCollation()
        );
    }
}
