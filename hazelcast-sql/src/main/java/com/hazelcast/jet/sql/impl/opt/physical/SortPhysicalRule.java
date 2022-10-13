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

import com.hazelcast.jet.sql.impl.opt.Conventions;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.SortLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.immutables.value.Value;

import java.util.List;


@Value.Enclosing
final class SortPhysicalRule extends RelRule<RelRule.Config> {

    /**
     * Rule configuration.
     */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        SortPhysicalRule.Config DEFAULT = ImmutableSortPhysicalRule.Config.builder()
                .description(SortPhysicalRule.class.getSimpleName())
                .operandSupplier(b0 -> b0
                        .operand(SortLogicalRel.class)
                        .oneInput(b1 -> b1.operand(RelNode.class)
                                .anyInputs()))
                .build();

        @Override
        default RelOptRule toRule() {
            return new SortPhysicalRule(this);
        }
    }

    private SortPhysicalRule(SortPhysicalRule.Config config) {
        super(config);
    }

    static final RelOptRule INSTANCE = new SortPhysicalRule(Config.DEFAULT);

    @Override
    public void onMatch(RelOptRuleCall call) {
        SortLogicalRel sort = call.rel(0);
        RelNode physicalInput = OptUtils.toPhysicalInput(sort.getInput());
        RelTraitSet traits = sort.getCluster()
                .traitSetOf(Conventions.PHYSICAL)
                .replace(sort.getCollation());
        RelNode transformTo;

        boolean requiresSort = requiresSort(sort.getCollation(), OptUtils.getCollation(physicalInput));

        if (!requiresSort && (sort.offset != null || sort.fetch != null)) {
            transformTo = new LimitPhysicalRel(
                    sort.offset,
                    sort.fetch,
                    sort.getCluster(),
                    traits,
                    physicalInput);
        } else if (!requiresSort) {
            transformTo = physicalInput;
        } else {
            transformTo = new SortPhysicalRel(
                    sort.getCluster(),
                    traits,
                    physicalInput,
                    sort.getCollation(),
                    null,
                    null,
                    sort.getRowType());
        }
        call.transformTo(transformTo);
    }

    // TODO: rewrite with top-down-style
    private static boolean requiresSort(RelCollation sortCollation, RelCollation inputCollation) {
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
