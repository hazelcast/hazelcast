/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.physical.rule;

import com.hazelcast.sql.impl.calcite.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.RuleUtils;
import com.hazelcast.sql.impl.calcite.logical.rel.ProjectLogicalRel;
import com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionField;
import com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionTrait;
import com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionType;
import com.hazelcast.sql.impl.calcite.physical.rel.ProjectPhysicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.calcite.physical.distribution.PhysicalDistributionType.DISTRIBUTED_PARTITIONED;

/**
 * This rule converts logical projection into physical projection. Physical projection inherits distribution of the
 * underlying operator.
 */
public class ProjectPhysicalRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new ProjectPhysicalRule();

    private ProjectPhysicalRule() {
        super(
            RuleUtils.parentChild(ProjectLogicalRel.class, RelNode.class, HazelcastConventions.LOGICAL),
            ProjectPhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        ProjectLogicalRel logicalProject = call.rel(0);
        RelNode input = logicalProject.getInput();

        RelNode convertedInput = RuleUtils.toPhysicalInput(input);

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
    private static Collection<InputAndTraitSet> getTransforms(ProjectLogicalRel logicalProject,
        RelNode convertedInput) {
        // TODO: Handle collation properly.
        List<InputAndTraitSet> res = new ArrayList<>();

                // Get mapping of project input fields to an index of related expression in the projection.
        Map<PhysicalDistributionField, Integer> projectFieldMap = getProjectFieldMap(logicalProject);

        Collection<RelNode> physicalInputs = RuleUtils.getPhysicalRelsFromSubset(convertedInput);

        for (RelNode physicalInput : physicalInputs) {
            PhysicalDistributionTrait transformedInputDist = RuleUtils.getPhysicalDistribution(physicalInput);

            PhysicalDistributionTrait projectDist =
                getProjectTraitForInputTrait(transformedInputDist, projectFieldMap);

            res.add(new InputAndTraitSet(physicalInput, physicalInput.getTraitSet().plus(projectDist)));
        }

        if (res.isEmpty())
            // If there were no physical inputs, then just propagate the default distribution.
            res.add(new InputAndTraitSet(convertedInput, convertedInput.getTraitSet()));

        return res;
    }

    /**
     * Get distribution trait which should be used by project based on the distribution of it's input.
     *
     * @param inputDist Input distribution.
     * @param projectFieldMap Project field map.
     * @return Distribution which should be used by project.
     */
    private static PhysicalDistributionTrait getProjectTraitForInputTrait(
        PhysicalDistributionTrait inputDist,
        Map<PhysicalDistributionField, Integer> projectFieldMap
    ) {
        PhysicalDistributionType type = inputDist.getType();

        switch (type) {
            case SINGLETON:
                // Singleton remains singleton. Fall-through.

            case DISTRIBUTED:
                // Arbitrary distribution remains arbitrary. Fall-through.

            case REPLICATED:
                // Replicated distribution is still replicated anyway.
                return inputDist;

            case ANY:
                // Unknown distribution -> convert to arbitrary.
                return PhysicalDistributionTrait.DISTRIBUTED;

            default:
                assert type == DISTRIBUTED_PARTITIONED;
        }

        // Partitioned distribution has partition fields. We need to check whether they are preserved after the
        // projection or not. If not, i.e. original distribution is lost, project distribution becomes arbitrary.
        // Consider the following tree: scan(a,b,c) -> group by (a,b) -> project (a). After the GROUP BY is performed
        // an input is partitioned by fields [a, b]. However, when one of this fields are lost during projection,
        // the input is no longer partitioned on any attribute. E.g MEMBER_1([a1, b1]), MEMBER_2([a1, b2]) becomes
        // MEMBER_1([a1]), MEMBER_2([a1]) after projection, so both members may containt the same value.
        List<PhysicalDistributionField> inputDistFields = inputDist.getFields();
        List<PhysicalDistributionField> projectDistFields = new ArrayList<>();

        for (PhysicalDistributionField inputDistField : inputDistFields) {
            Integer idx = projectFieldMap.get(inputDistField);

            if (idx != null)
                // Input distribution field mapped to project field. Continue.
                projectDistFields.add(new PhysicalDistributionField(idx));
            else
                // Input distribution column is lost during project. Fallback to arbitrary distribution.
                return PhysicalDistributionTrait.DISTRIBUTED;
        }

        // All input distribution fields have been mapped to project fields. Propagate input distribution to project.
        return new PhysicalDistributionTrait(inputDist.getType(), projectDistFields);
    }

    /**
     * Get a map from potential input distribution field to it's index in the projection.
     *
     * @param project Projection.
     * @return Result.
     */
    private static Map<PhysicalDistributionField, Integer> getProjectFieldMap(ProjectLogicalRel project) {
        Map<PhysicalDistributionField, Integer> res = new HashMap<>();

        int idx = 0;

        for (RexNode node : project.getProjects()) {
            if (node instanceof RexInputRef) {
                RexInputRef node0 = (RexInputRef)node;

                res.put(new PhysicalDistributionField(node0.getIndex()), idx);
            }
            else if (node instanceof RexFieldAccess) {
                RexFieldAccess node0 = (RexFieldAccess) node;

                if (node0.getReferenceExpr() instanceof RexInputRef) {
                    RexInputRef nestedNode = (RexInputRef) node0.getReferenceExpr();

                    String nestedFieldName = node0.getField().getName();

                    res.put(new PhysicalDistributionField(nestedNode.getIndex(), nestedFieldName), idx);
                }
            }

            idx++;
        }

        // TODO: Cast without precision loss may also preserve partition info.

        return res;
    }

    /**
     * A pair of input and trait set which should be used for transformation.
     */
    private static class InputAndTraitSet {
        /** Input of the projection. */
        private final RelNode input;

        /** Trait set of the projection. */
        private final RelTraitSet traitSet;

        public InputAndTraitSet(RelNode input, RelTraitSet traitSet) {
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
