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
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTrait;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionType;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.logical.ProjectLogicalRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.opt.distribution.DistributionType.PARTITIONED;

/**
 * This rule converts logical projection into physical projection. Physical projection inherits distribution and collation of
 * the input provided that input columns responsible for distribution/collation are present in the project operator.
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

        Collection<InputAndTraitSet> transforms = getTransforms(convertedInput);

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
     * @param convertedInput Input.
     * @return Conversions (converted input + trait set).
     */
    private static Collection<InputAndTraitSet> getTransforms(RelNode convertedInput) {
        List<InputAndTraitSet> res = new ArrayList<>(1);

        Collection<RelNode> physicalInputs = OptUtils.getPhysicalRelsFromSubset(convertedInput);

        for (RelNode physicalInput : physicalInputs) {
            RelTraitSet finalTraitSet = createPhysicalTraitSet(physicalInput);

            res.add(new InputAndTraitSet(physicalInput, finalTraitSet));
        }

        return res;
    }

    /**
     * Create a trait set for physical project.
     *
     * @param physicalInput Project's input.
     * @return Trait set.
     */
    private static RelTraitSet createPhysicalTraitSet(RelNode physicalInput) {
        return OptUtils.traitPlus(physicalInput.getTraitSet(), deriveDistribution(physicalInput));
    }

    /**
     * Get distribution trait which should be used by project based on the distribution of its input.
     *
     * @param physicalInput Physical input.
     * @return Distribution which should be used by project.
     */
    @SuppressWarnings({"checkstyle:RegexpSingleline", "SwitchStatementWithTooFewBranches"})
    private static DistributionTrait deriveDistribution(RelNode physicalInput) {
        DistributionTrait physicalInputDist = OptUtils.getDistribution(physicalInput);

        DistributionType type = physicalInputDist.getType();

        switch (type) {
            case ROOT:
                // Singleton remains singleton.
                return physicalInputDist;

            default:
                assert type == PARTITIONED;

                return physicalInputDist;
        }
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
