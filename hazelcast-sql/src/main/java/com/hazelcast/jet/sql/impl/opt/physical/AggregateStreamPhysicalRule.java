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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.AggregateLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.SlidingWindowLogicalRel;
import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMetadataQuery;
import com.hazelcast.jet.sql.impl.opt.metadata.WatermarkedFields;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

final class AggregateStreamPhysicalRule extends AggregateAbstractPhysicalRule {

    private static final Config CONFIG_PROJECT = Config.EMPTY
            .withDescription(AggregateStreamPhysicalRule.class.getSimpleName())
            .withOperandSupplier(b0 -> b0.operand(AggregateLogicalRel.class)
                    .trait(LOGICAL)
                    .predicate(OptUtils::isUnbounded)
                    .inputs(b1 -> b1
                            .operand(LogicalProject.class)
                            .inputs(b2 -> b2.operand(SlidingWindowLogicalRel.class).anyInputs())));

    private static final Config CONFIG_NO_PROJECT = Config.EMPTY
            .withDescription(AggregateStreamPhysicalRule.class.getSimpleName())
            .withOperandSupplier(b0 -> b0.operand(AggregateLogicalRel.class)
                    .trait(LOGICAL)
                    .predicate(OptUtils::isUnbounded)
                    .inputs(b1 -> b1.operand(SlidingWindowLogicalRel.class).anyInputs()));

    static final RelOptRule NO_PROJECT_INSTANCE = new AggregateStreamPhysicalRule(CONFIG_NO_PROJECT, false);
    static final RelOptRule PROJECT_INSTANCE = new AggregateStreamPhysicalRule(CONFIG_PROJECT, true);

    private final boolean hasProject;

    private AggregateStreamPhysicalRule(Config config, boolean hasProject) {
        super(config);
        this.hasProject = hasProject;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        AggregateLogicalRel logicalAggregate = call.rel(0);
        assert logicalAggregate.getGroupType() == Group.SIMPLE;

        List<RexNode> projections;
        SlidingWindowLogicalRel windowRel;
        if (hasProject) {
            projections = call.<Project>rel(1).getProjects();
            windowRel = call.rel(2);
        } else {
            windowRel = call.rel(1);
            // create an identity projection
            List<RelDataTypeField> fields = windowRel.getRowType().getFieldList();
            projections = new ArrayList<>(fields.size());
            for (int i = 0; i < fields.size(); i++) {
                RelDataTypeField field = fields.get(i);
                projections.add(call.builder().getRexBuilder().makeInputRef(field.getType(), i));
            }
        }

        // TODO [viliam] check that one of the columns we're grouping by has watermark order trait

        RelNode input = windowRel.getInput();
        RelNode convertedInput = OptUtils.toPhysicalInput(input);
        Collection<RelNode> transformedInputs = OptUtils.extractPhysicalRelsFromSubset(convertedInput);
        for (RelNode transformedInput : transformedInputs) {
            RelNode transformedRel = optimize(logicalAggregate, transformedInput);
            if (transformedRel != null) {
                call.transformTo(transformedRel);
            }
        }
    }

    private RelNode optimize(AggregateLogicalRel logicalAggregate, RelNode physicalInput) {
        Entry<Integer, RexNode> watermarkedGroupedField = findWatermarkedGroupedField(logicalAggregate, physicalInput);
        if (watermarkedGroupedField == null) {
            // there's no field that we group by, that also has watermarks
            return null;
        }

        AggregateOperation<?, Object[]> aggrOp = aggregateOperation(
                physicalInput.getRowType(),
                logicalAggregate.getGroupSet().clear(watermarkedGroupedField.getKey()),
                logicalAggregate.getAggCallList());

        if (logicalAggregate.containsDistinctCall()) {
            return new SlidingWindowAggregateByKeyPhysicalRel(
                    physicalInput.getCluster(),
                    physicalInput.getTraitSet(),
                    physicalInput,
                    logicalAggregate.getGroupSet(),
                    logicalAggregate.getGroupSets(),
                    logicalAggregate.getAggCallList(),
                    aggrOp,
                    watermarkedGroupedField.getValue());
        } else {
            RelNode rel = new SlidingWindowAggregateAccumulateByKeyPhysicalRel(
                    physicalInput.getCluster(),
                    physicalInput.getTraitSet(),
                    physicalInput,
                    logicalAggregate.getGroupSet(),
                    aggrOp,
                    watermarkedGroupedField.getValue());

            return new SlidingWindowAggregateCombineByKeyPhysicalRel(
                    rel.getCluster(),
                    rel.getTraitSet(),
                    rel,
                    logicalAggregate.getGroupSet(),
                    logicalAggregate.getGroupSets(),
                    logicalAggregate.getAggCallList(),
                    aggrOp,
                    watermarkedGroupedField.getValue());
        }
    }

    @Nullable
    private static Entry<Integer, RexNode> findWatermarkedGroupedField(
            AggregateLogicalRel logicalAggregate,
            RelNode input
    ) {
        HazelcastRelMetadataQuery query = OptUtils.metadataQuery(input);
        WatermarkedFields watermarkedFields = query.extractWatermarkedFields(input);
        if (watermarkedFields == null) {
            return null;
        }
        return watermarkedFields.findFirst(logicalAggregate.getGroupSet());
    }
}
