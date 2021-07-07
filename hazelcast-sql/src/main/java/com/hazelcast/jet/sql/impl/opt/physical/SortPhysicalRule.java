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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.SortLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.JetConventions.LOGICAL;
import static java.util.Collections.singletonList;

final class SortPhysicalRule extends RelOptRule {

    static final RelOptRule INSTANCE = new SortPhysicalRule();

    private SortPhysicalRule() {
        super(
                operand(SortLogicalRel.class, LOGICAL, some(operand(RelNode.class, any()))),
                SortPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        SortLogicalRel logicalSort = call.rel(0);

        List<RelNode> transforms = toTransforms(logicalSort);
        for (RelNode transform : transforms) {
            call.transformTo(transform);
        }
    }

    private static List<RelNode> toTransforms(SortLogicalRel logicalSort) {
        List<RelNode> sortTransforms = new ArrayList<>(1);
        List<RelNode> nonSortTransforms = new ArrayList<>(1);

        for (RelNode physicalInput : OptUtils.extractPhysicalRelsFromAllSubsets(logicalSort.getInput())) {
            boolean requiresSort = requiresLocalSort(
                    logicalSort.getCollation(),
                    physicalInput.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE)
            );
            if (requiresSort) {
                sortTransforms.add(convert(logicalSort));
            } else {
                nonSortTransforms.add(physicalInput);
            }
        }
        List<RelNode> transforms = nonSortTransforms.isEmpty() ? sortTransforms : nonSortTransforms;
        return !transforms.isEmpty() ? transforms : singletonList(convert(logicalSort));
    }

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

    private static RelNode convert(RelNode rel) {
        SortLogicalRel logicalSort = (SortLogicalRel) rel;

        return new SortPhysicalRel(
                logicalSort.getCluster(),
                OptUtils.toPhysicalConvention(logicalSort.getTraitSet()),
                OptUtils.toPhysicalInput(logicalSort.getInput()),
                logicalSort.getCollation(),
                logicalSort.offset,
                logicalSort.getFetch(),
                logicalSort.getRowType()
        );
    }
}
