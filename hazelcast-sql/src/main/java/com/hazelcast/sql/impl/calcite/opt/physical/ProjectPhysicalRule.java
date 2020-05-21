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
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTraitDef;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionType;
import com.hazelcast.sql.impl.calcite.opt.logical.ProjectLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.calcite.opt.distribution.DistributionType.PARTITIONED;

/**
 * This rule converts logical projection into physical projection. Physical projection inherits distribution of the
 * underlying operator.
 */
public final class ProjectPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new ProjectPhysicalRule();

    private ProjectPhysicalRule() {
        super(
            OptUtils.parentChild(ProjectLogicalRel.class, RelNode.class, HazelcastConventions.LOGICAL),
            ProjectPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        ProjectLogicalRel logicalProject = call.rel(0);
        RelNode input = logicalProject.getInput();

        RelNode convertedInput = OptUtils.toPhysicalInput(input);

        Collection<InputAndTraitSet> transforms = getTransforms(logicalProject, convertedInput);

        for (InputAndTraitSet transform : transforms) {
            ProjectPhysicalRel newProject = new ProjectPhysicalRel(
                logicalProject.getCluster(),
                transform.getTraitSet(),
                transform.getInput(),
                logicalProject.getProjects(),
                logicalProject.getRowType()
            );

            call.transformTo(newProject);
        }
    }

    /**
     * Get conversions which will be applied to the given logical project.
     *
     * @param logicalProject Logical project.
     * @param convertedInput Input.
     * @return Conversions (converted input + trait set).
     */
    private static Collection<InputAndTraitSet> getTransforms(ProjectLogicalRel logicalProject, RelNode convertedInput) {
        // TODO: Handle collation properly.
        List<InputAndTraitSet> res = new ArrayList<>(1);

        // Get mapping of project input fields to an index of related expression in the projection.
        Map<Integer, Integer> candDistFields = getCandidateDistributionFields(logicalProject);
        Map<Integer, Integer> candCollationFields = getCandidateCollationFields(logicalProject);

        Collection<RelNode> physicalInputs = OptUtils.getPhysicalRelsFromSubset(convertedInput);

        for (RelNode physicalInput : physicalInputs) {
            RelTraitSet finalTraitSet = createPhysicalTraitSet(physicalInput, candDistFields, candCollationFields);

            if (finalTraitSet != null) {
                res.add(new InputAndTraitSet(physicalInput, finalTraitSet));
            }
        }

//        if (res.isEmpty()) {
//            // If there were no physical inputs, then just propagate the default distribution.
//            RelTraitSet finalTraitSet = createPhysicalTraitSet(convertedInput, candDistFields, candCollationFields);
//
//            res.add(new InputAndTraitSet(convertedInput, finalTraitSet));
//        }

        return res;
    }

    /**
     * Create a trait set for physical project.
     *
     * @param physicalInput Project's input.
     * @param candDistFields Candidate distribution fields.
     * @param candCollationFields Candidate collation fields.
     * @return Trait set.
     */
    private static RelTraitSet createPhysicalTraitSet(
        RelNode physicalInput,
        Map<Integer, Integer> candDistFields,
        Map<Integer, Integer> candCollationFields
    ) {
        DistributionTrait finalDist = deriveDistribution(physicalInput, candDistFields);
        RelCollation finalCollation = deriveCollation(physicalInput, candCollationFields);

        return OptUtils.traitPlus(physicalInput.getTraitSet(), finalDist, finalCollation);
    }

    /**
     * Get distribution trait which should be used by project based on the distribution of its input.
     *
     * @param physicalInput Physical input.
     * @param projectFieldMap Project field map.
     * @return Distribution which should be used by project.
     */
    @SuppressWarnings("checkstyle:RegexpSingleline")
    private static DistributionTrait deriveDistribution(RelNode physicalInput, Map<Integer, Integer> projectFieldMap) {
        DistributionTrait physicalInputDist = OptUtils.getDistribution(physicalInput);
        DistributionTraitDef distributionTraitDef = (DistributionTraitDef) physicalInputDist.getTraitDef();

        DistributionType type = physicalInputDist.getType();

        switch (type) {
            case ROOT:
                // Singleton remains singleton.
                return physicalInputDist;

            case REPLICATED:
                // Replicated remains replicated.
                return physicalInputDist;

            case ANY:
                // Unknown distribution -> do not convert.
                return null;

            default:
                assert type == PARTITIONED;
        }

        // If PARTITIONED distribution doesn't have distribution fields, then do early exit, since there is nothing to loose.
        if (!physicalInputDist.hasFieldGroups()) {
            return physicalInputDist;
        }

        // Partitioned distribution has partition fields. We need to check whether they are preserved after the
        // projection or not. If not, i.e. original distribution is lost, project distribution becomes arbitrary.
        // Consider the following tree: scan(a,b,c) -> group by (a,b) -> project (a). After the GROUP BY is performed
        // an input is partitioned by fields [a, b]. However, when one of this fields are lost during projection,
        // the input is no longer partitioned on any attribute. E.g MEMBER_1([a1, b1]), MEMBER_2([a1, b2]) becomes
        // MEMBER_1([a1]), MEMBER_2([a1]) after projection, so both members may contains the same value.
        List<List<Integer>> newFieldGroups = new ArrayList<>(1);

        for (List<Integer> fieldGroup : physicalInputDist.getFieldGroups()) {
            boolean valid = true;
            List<Integer> newFieldGroup = new ArrayList<>(fieldGroup.size());

            for (Integer field : fieldGroup) {
                Integer idx = projectFieldMap.get(field);

                if (idx != null) {
                    // The field is still in the project, continue.
                    newFieldGroup.add(idx);
                } else {
                    // Distribution field is not in the project. We cannot use this field group any more.
                    valid = false;

                    break;
                }
            }

            if (valid) {
                newFieldGroups.add(newFieldGroup);
            }
        }

        return distributionTraitDef.createPartitionedTrait(newFieldGroups);
    }

    /**
     * Derive collation of physical projection from the logical projection and physical input. The condition is that
     * logical projection should have at least some prefix of input fields.
     *
     * @param physicalInput Physical input.
     * @param candCollationFields Candidate collation fields.
     * @return Collation which should be used for physical projection.
     */
    private static RelCollation deriveCollation(RelNode physicalInput, Map<Integer, Integer> candCollationFields) {
        RelCollation inputCollation = physicalInput.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);

        List<RelFieldCollation> fields = null;

        for (RelFieldCollation field : inputCollation.getFieldCollations()) {
            Integer projectFieldIndex = candCollationFields.get(field.getFieldIndex());

            if (projectFieldIndex != null) {
                if (fields == null) {
                    fields = new ArrayList<>(1);
                }

                fields.add(new RelFieldCollation(projectFieldIndex, field.getDirection(), field.nullDirection));
            } else {
                // No more prefixes. We are done.
                break;
            }
        }

        if (fields == null) {
            fields = Collections.emptyList();
        }

        return RelCollations.of(fields);
    }

    /**
     * Get a map of candidate collation fields for the given logical project. Key of the map is the index of the field
     * in its input, value of the map is the index of that field in projection.
     *
     * @param project Project.
     * @return Map of candidate collation fields.
     */
    private static Map<Integer, Integer> getCandidateCollationFields(ProjectLogicalRel project) {
        Map<Integer, Integer> res = new HashMap<>();

        int idx = 0;

        for (RexNode node : project.getProjects()) {
            // Only direct field references are capable of maintaining collation.
            // Nested fields are not supported, since they loss the collation. E.g. a stream of data sorted on "a"
            // is not guaranteed to be sorted on "a.b".
            // CAST function is not applicable here as wel, because collation rules may be different for different
            // data types, and concrete data type is only resolved in runtime. For example, integer input of values
            // 1, 2, 11 is sorted in that order. But if the field is converted to VARCHAR, then sort order is
            // different: "1", "11", "2".

            // TODO: Some functions may preserve monotonicity, e.g. "x + 1" is always sorted in the same way as "x"
            // TODO: provided that precision loss is prohibited. Investigate Calcite's SqlMonotonicity.

            // TODO:
            if (node instanceof RexInputRef) {
                RexInputRef node0 = (RexInputRef) node;

                res.put(node0.getIndex(), idx);
            }

            idx++;
        }

        return res;
    }

    /**
     * Get a map from potential input distribution field to its index in the projection.
     *
     * @param project Projection.
     * @return Result.
     */
    private static Map<Integer, Integer> getCandidateDistributionFields(ProjectLogicalRel project) {
        Map<Integer, Integer> res = new HashMap<>();

        int idx = 0;

        for (RexNode node : project.getProjects()) {
            if (node instanceof RexInputRef) {
                RexInputRef node0 = (RexInputRef) node;

                res.put(node0.getIndex(), idx);
            }

            idx++;
        }

        return res;
    }

    /**
     * A pair of input and trait set which should be used for transformation.
     */
    private static final class InputAndTraitSet {
        /** Input of the projection. */
        private final RelNode input;

        /** Trait set of the projection. */
        private final RelTraitSet traitSet;

        private InputAndTraitSet(RelNode input, RelTraitSet traitSet) {
            this.input = input;
            this.traitSet = traitSet;
        }

        public RelNode getInput() {
            return input;
        }

        public RelTraitSet getTraitSet() {
            return traitSet;
        }
    }
}
