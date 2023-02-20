/*
 * Copyright 2023 Hazelcast Inc.
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
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.function.KeyedWindowResultFunction;
import com.hazelcast.jet.impl.util.ConstantFunctionEx;
import com.hazelcast.jet.sql.impl.ObjectArrayKey;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.metadata.WatermarkedFields;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.jet.sql.impl.aggregate.WindowUtils.insertWindowBound;
import static com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeUtils.toHazelcastType;

public class SlidingWindowAggregatePhysicalRel extends Aggregate implements PhysicalRel {

    private final int timestampFieldIndex;
    private final FunctionEx<ExpressionEvalContext, SlidingWindowPolicy> windowPolicyProvider;
    private final int numStages;
    private final List<Integer> windowStartIndexes;
    private final List<Integer> windowEndIndexes;

    @SuppressWarnings("checkstyle:ParameterNumber")
    SlidingWindowAggregatePhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            int timestampFieldIndex,
            FunctionEx<ExpressionEvalContext, SlidingWindowPolicy> windowPolicyProvider,
            int numStages,
            List<Integer> windowStartIndexes,
            List<Integer> windowEndIndexes
    ) {
        super(cluster, traits, new ArrayList<>(), input, groupSet, groupSets, aggCalls);

        this.timestampFieldIndex = timestampFieldIndex;
        this.windowPolicyProvider = windowPolicyProvider;
        this.numStages = numStages;
        this.windowStartIndexes = windowStartIndexes;
        this.windowEndIndexes = windowEndIndexes;
    }

    public FunctionEx<JetSqlRow, ?> groupKeyFn() {
        ImmutableBitSet groupSet = getGroupSetReduced();
        if (groupSet.isEmpty()) {
            // if there's no grouping, group by a random constant value
            return new ConstantFunctionEx<>(ThreadLocalRandom.current().nextInt());
        }
        return ObjectArrayKey.projectFn(groupSet.toArray());
    }

    public AggregateOperation<?, JetSqlRow> aggrOp() {
        return AggregateAbstractPhysicalRule.aggregateOperation(getInput().getRowType(), getGroupSetReduced(), getAggCallList());
    }

    private ImmutableBitSet getGroupSetReduced() {
        ImmutableBitSet reducedGroupSet = getGroupSet();
        for (Integer index : windowStartIndexes) {
            reducedGroupSet = reducedGroupSet.clear(index);
        }
        for (Integer index : windowEndIndexes) {
            reducedGroupSet = reducedGroupSet.clear(index);
        }
        return reducedGroupSet;
    }

    public int timestampFieldIndex() {
        return timestampFieldIndex;
    }

    public Expression<?> timestampExpression() {
        RelDataTypeField timestampField = getInput().getRowType().getFieldList().get(timestampFieldIndex);
        return ColumnExpression.create(timestampFieldIndex, toHazelcastType(timestampField.getType()));
    }

    public FunctionEx<ExpressionEvalContext, SlidingWindowPolicy> windowPolicyProvider() {
        return windowPolicyProvider;
    }

    public int numStages() {
        return numStages;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    public KeyedWindowResultFunction<? super Object, ? super JetSqlRow, ?> outputValueMapping() {
        int[] windowBoundsIndexMask = new int[getRowType().getFieldCount()];

        QueryDataType descriptorType = timestampExpression().getType();

        for (Integer index : windowStartIndexes) {
            windowBoundsIndexMask[index] = -1;
        }
        for (Integer index : windowEndIndexes) {
            windowBoundsIndexMask[index] = -2;
        }

        for (int i = 0, j = 0; i < windowBoundsIndexMask.length; i++) {
            if (windowBoundsIndexMask[i] >= 0) {
                windowBoundsIndexMask[i] = j++;
            }
        }

        return (start, end, ignoredKey, result, isEarly) ->
                insertWindowBound(result, start, end, descriptorType, windowBoundsIndexMask);
    }

    public WatermarkedFields watermarkedFields() {
        // Backlog: also support windowStartIndexes
        return new WatermarkedFields(new HashSet<>(windowEndIndexes));
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        return OptUtils.schema(getRowType());
    }

    @Override
    public <V> V accept(CreateDagVisitor<V> visitor) {
        return visitor.onSlidingWindowAggregate(this);
    }

    @Override
    public final Aggregate copy(
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls
    ) {
        return new SlidingWindowAggregatePhysicalRel(
                getCluster(),
                traitSet,
                input,
                groupSet,
                groupSets,
                aggCalls,
                timestampFieldIndex,
                windowPolicyProvider,
                numStages,
                windowStartIndexes,
                windowEndIndexes);
    }
}
