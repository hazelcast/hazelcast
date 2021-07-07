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
import org.apache.calcite.rel.convert.ConverterRule;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.JetConventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.JetConventions.PHYSICAL;

final class SortPhysicalRule extends ConverterRule {

    static final RelOptRule INSTANCE = new SortPhysicalRule();

    private SortPhysicalRule() {
        super(
                SortLogicalRel.class, LOGICAL, PHYSICAL,
                SortPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        SortLogicalRel logicalRel = (SortLogicalRel) rel;

        return new SortPhysicalRel(
                logicalRel.getCluster(),
                OptUtils.toPhysicalConvention(logicalRel.getTraitSet()),
                OptUtils.toPhysicalInput(logicalRel.getInput()),
                logicalRel.getCollation(),
                logicalRel.offset,
                logicalRel.getFetch(),
                logicalRel.getRowType()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        SortLogicalRel sortRel = call.rel(0);
        RelNode input = sortRel.getInput();
        List<RelNode> sortTransforms = new ArrayList<>(1);
        List<RelNode> nonSortTransforms = new ArrayList<>(1);

        for (RelNode physicalInput : OptUtils.extractPhysicalRelsFromAllSubsets(input)) {
            boolean requiresSort = requiresLocalSort(sortRel.getCollation(),
                    physicalInput.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE));
            if (requiresSort) {
                sortTransforms.add(convert(rel));
            } else {
                nonSortTransforms.add(physicalInput);
            }
        }
        List<RelNode> transforms = nonSortTransforms.isEmpty() ? sortTransforms : nonSortTransforms;
        if (transforms.isEmpty()) {
            transforms.add(convert(rel));
        }
        for (RelNode transform : transforms) {
            call.transformTo(transform);
        }
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

}
