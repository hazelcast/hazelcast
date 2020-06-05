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

import com.hazelcast.sql.impl.calcite.opt.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTrait;
import com.hazelcast.sql.impl.calcite.opt.logical.SortLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.SortMergeExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.UnicastExchangePhysicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Rule which converts logical sort into its physical counterpart. There are several forms of physical implementations:
 * <ul>
 *     <li><b>Local</b> - in case the whole input is available locally (ROOT, REPLICATED)</li>
 *     <li><b>Two-phase (local + merge)</b> - in case the input is located on several nodes. In this case a
 *     {@link SortMergeExchangePhysicalRel} is created on top of local sort</li>
 * </ul>
 * <p>
 * Local component may be removed altogether in case the input is already sorted on required attributes.
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

        for (RelNode physicalInput : OptUtils.getPhysicalRelsFromSubset(convertedInput)) {
            // Add local sorting if needed.
            boolean requiresLocalSort = requiresLocalSort(
                logicalSort.getCollation(),
                physicalInput.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE)
            );

            RelNode rel;

            if (requiresLocalSort) {
                rel = createLocalSort(logicalSort, physicalInput);
            } else {
                rel = createLocalNoSort(physicalInput, logicalSort.fetch, logicalSort.offset);
            }

            // Add merge phase if needed.
            DistributionTrait physicalInputDist = OptUtils.getDistribution(physicalInput);

            if (!physicalInputDist.isFullResultSetOnAllParticipants()) {
                rel = createMerge(rel, logicalSort);
            }

            // Add to the list of transformations.
            res.add(rel);
        }

        return res;
    }

    /**
     * Check if local sorting phase is needed. It could be avoided iff sort collation is a prefix of input collation.
     *
     * @return {@code true} if local sorting is needed, {@code false} otherwise.
     */
    private static boolean requiresLocalSort(RelCollation sortCollation, RelCollation inputCollation) {
        if (sortCollation.getFieldCollations().isEmpty()) {
            return false;
        }

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

    private RelNode createLocalNoSort(RelNode input, RexNode fetch, RexNode offset) {
        if (fetch == null) {
            assert offset == null;

            return input;
        } else {
            return new FetchPhysicalRel(
                input.getCluster(),
                input.getTraitSet(),
                input,
                fetch,
                offset
            );
        }
    }

    private static RelNode createMerge(RelNode physicalInput, SortLogicalRel logicalSort) {
        // TODO: Do not use root here? Instead, we should set either PARTITIONED with 1 node or possibly REPLICATED?
        RelTraitSet traitSet = OptUtils.traitPlus(physicalInput.getTraitSet(),
            logicalSort.getCollation(),
            OptUtils.getDistributionDef(physicalInput).getTraitRoot()
        );

        boolean fetchOnly = logicalSort.getCollation().getFieldCollations().isEmpty();

        if (fetchOnly) {
            // Perform merge without sorting.
            RelNode rel = new UnicastExchangePhysicalRel(
                logicalSort.getCluster(),
                traitSet,
                physicalInput,
                Collections.emptyList()
            );

            boolean limit = logicalSort.fetch != null;

            if (limit) {
                rel = new FetchPhysicalRel(
                    logicalSort.getCluster(),
                    rel.getTraitSet(),
                    rel,
                    logicalSort.fetch,
                    logicalSort.offset
                );
            }

            return rel;
        } else {
            // Perform merge with sorting.
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
}
