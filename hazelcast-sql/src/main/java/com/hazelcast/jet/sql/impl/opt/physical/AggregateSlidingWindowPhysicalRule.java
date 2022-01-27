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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.AggregateLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.ProjectLogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.SlidingWindowLogicalRel;
import com.hazelcast.jet.sql.impl.opt.metadata.HazelcastRelMetadataQuery;
import com.hazelcast.jet.sql.impl.opt.metadata.WatermarkedFields;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.OptUtils.hasInputRef;

public final class AggregateSlidingWindowPhysicalRule extends AggregateAbstractPhysicalRule {

    private static final Config CONFIG_PROJECT = Config.EMPTY
            .withDescription(AggregateSlidingWindowPhysicalRule.class.getSimpleName() + "-project")
            .withOperandSupplier(b0 -> b0
                    .operand(AggregateLogicalRel.class)
                    .trait(LOGICAL)
                    .predicate(OptUtils::isUnbounded)  // Input must be unbounded (streaming)
                    .inputs(b1 -> b1
                            .operand(ProjectLogicalRel.class)
                            .inputs(b2 -> b2
                                    .operand(SlidingWindowLogicalRel.class).anyInputs())));

    private static final Config CONFIG_NO_PROJECT = Config.EMPTY
            .withDescription(AggregateSlidingWindowPhysicalRule.class.getSimpleName() + "-no-project")
            .withOperandSupplier(b0 -> b0
                    .operand(AggregateLogicalRel.class)
                    .trait(LOGICAL)
                    .predicate(OptUtils::isUnbounded)
                    .inputs(b1 -> b1
                            .operand(SlidingWindowLogicalRel.class).anyInputs()));

    static final RelOptRule NO_PROJECT_INSTANCE = new AggregateSlidingWindowPhysicalRule(CONFIG_NO_PROJECT, false);
    static final RelOptRule PROJECT_INSTANCE = new AggregateSlidingWindowPhysicalRule(CONFIG_PROJECT, true);

    private final boolean hasProject;

    private AggregateSlidingWindowPhysicalRule(Config config, boolean hasProject) {
        super(config);
        this.hasProject = hasProject;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        AggregateLogicalRel logicalAggregate = call.rel(0);
        assert logicalAggregate.getGroupType() == Group.SIMPLE;
        assert logicalAggregate.getGroupSets().size() == 1 &&
                (logicalAggregate.getGroupSet() == null
                        || logicalAggregate.getGroupSet().equals(logicalAggregate.getGroupSets().get(0)));

        List<RexNode> projections;
        RelDataType projectRowType;
        SlidingWindowLogicalRel windowRel;
        if (hasProject) {
            Project projectRel = call.rel(1);
            projections = new ArrayList<>(projectRel.getProjects());
            projectRowType = projectRel.getRowType();
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
            projectRowType = windowRel.getRowType();
        }

        // Our input hierarchy is, for example:
        // -Aggregate(group=[$0], EXPR$1=[AVG($1)])
        // --Project(rowType=[window_start, field1])
        // ---SlidingWindowRel(rowType=[field1, field2, timestamp, window_start, window_end])
        //
        // We need to preserve the column used to generate window bounds and remove the window
        // bounds from the projection to get input projection such as this:
        // -SlidingWindowAggregatePhysicalRel(group=[$0], EXPR$1=[AVG($1)])
        // --Project(rowType=[timestamp, field1])
        //
        // The group=[$0] we pass to SlidingWindowAggregatePhysicalRel' superclass isn't correct,
        // but it works for us for now - the superclass uses it only to calculate the output type.
        // And the timestamp and the window bound have the same type.

        int timestampIndex = windowRel.orderingFieldIndex();
        int windowStartIndex = windowRel.windowStartIndex();
        int windowEndIndex = windowRel.windowEndIndex();

        // Replace references to either window bound to timestamp in projection
        List<Integer> windowStartIndexes = new ArrayList<>();
        List<Integer> windowEndIndexes = new ArrayList<>();
        for (int i = 0; i < projections.size(); i++) {
            RexNode projection = projections.get(i);
            // we don't support any transformation of the window bound using an expression, it must be a direct input reference.
            if (projection instanceof RexInputRef) {
                int index = ((RexInputRef) projection).getIndex();
                if (index == windowStartIndex || index == windowEndIndex) {
                    // todo [viliam] avoid multiple projections of the timestamp
                    projection = call.builder().getRexBuilder().makeInputRef(projection.getType(), timestampIndex);
                    projections.set(i, projection);
                    if (index == windowStartIndex) {
                        windowStartIndexes.add(i);
                    } else {
                        windowEndIndexes.add(i);
                    }
                }
            } else if (hasInputRef(projection, windowStartIndex, windowEndIndex)) {
                throw QueryException.error(SqlErrorCode.PARSING,
                        "In window aggregation, the window_start and window_end fields must be used" +
                                " directly, without any transformation"
                );
            }
        }

        RelNode input = windowRel.getInput();
        RelNode convertedInput = OptUtils.toPhysicalInput(input);
        Collection<RelNode> transformedInputs = OptUtils.extractPhysicalRelsFromSubset(convertedInput);

        for (RelNode transformedInput : transformedInputs) {
            // todo [viliam] change the name for window bound replaced with timestamps
            RelDataType newRowType = projectRowType;
            RelNode newProject = new ProjectPhysicalRel(transformedInput.getCluster(), transformedInput.getTraitSet(),
                    transformedInput, projections, newRowType);

            RelNode transformedRel = transform(newProject, logicalAggregate, windowStartIndexes, windowEndIndexes,
                    windowRel.windowPolicyProvider());
            if (transformedRel != null) {
                call.transformTo(transformedRel);
            }
        }
    }

    private RelNode transform(
            RelNode physicalInput,
            AggregateLogicalRel logicalAggregate,
            List<Integer> windowStartIndexes,
            List<Integer> windowEndIndexes,
            FunctionEx<ExpressionEvalContext, SlidingWindowPolicy> windowPolicyProvider
    ) {
        Integer watermarkedField = findWatermarkedField(logicalAggregate, physicalInput);
        if (watermarkedField == null) {
            return null;
        }

        RexNode timestampExpression = logicalAggregate.getCluster().getRexBuilder().makeInputRef(
                physicalInput, watermarkedField);

        return new SlidingWindowAggregatePhysicalRel(
                physicalInput.getCluster(),
                physicalInput.getTraitSet(),
                physicalInput,
                logicalAggregate.getGroupSet(),
                logicalAggregate.getGroupSets(),
                logicalAggregate.getAggCallList(),
                timestampExpression,
                windowPolicyProvider,
                logicalAggregate.containsDistinctCall() ? 1 : 2,
                windowStartIndexes,
                windowEndIndexes);
    }

    /**
     * Extract watermarked column index
     * (possibly) enforced by IMPOSE_ORDER.
     *
     * @return watermarked column index.
     */
    @Nullable
    private static Integer findWatermarkedField(
            AggregateLogicalRel logicalAggregate,
            RelNode input
    ) {
        // TODO: [viliam, sasha] besides watermark order, we can also use normal collation
        HazelcastRelMetadataQuery query = OptUtils.metadataQuery(input);
        WatermarkedFields watermarkedFields = query.extractWatermarkedFields(input);
        if (watermarkedFields == null) {
            return null;
        }
        Entry<Integer, RexNode> watermarkedField = watermarkedFields.findFirst(logicalAggregate.getGroupSet());
        return watermarkedField != null ? watermarkedField.getKey() : null;
    }
}
