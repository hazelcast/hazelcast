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
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.ObjectArrayKey;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

public class SlidingWindowAggregatePhysicalRel extends Aggregate implements PhysicalRel {

    private final AggregateOperation<?, Object[]> aggrOp;
    private final RexNode timestampExpression;
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
            AggregateOperation<?, Object[]> aggrOp,
            RexNode timestampExpression,
            FunctionEx<ExpressionEvalContext, SlidingWindowPolicy> windowPolicyProvider,
            int numStages,
            List<Integer> windowStartIndexes,
            List<Integer> windowEndIndexes
    ) {
        super(cluster, traits, new ArrayList<>(), input, groupSet, groupSets, aggCalls);

        this.aggrOp = aggrOp;
        this.timestampExpression = timestampExpression;
        this.windowPolicyProvider = windowPolicyProvider;
        this.numStages = numStages;
        this.windowStartIndexes = windowStartIndexes;
        this.windowEndIndexes = windowEndIndexes;
    }

    public FunctionEx<Object[], ObjectArrayKey> groupKeyFn() {
        return ObjectArrayKey.projectFn(getGroupSet().toArray());
    }

    public AggregateOperation<?, Object[]> aggrOp() {
        return aggrOp;
    }

    public Expression<?> timestampExpression() {
        QueryParameterMetadata parameterMetadata = ((HazelcastRelOptCluster) getCluster()).getParameterMetadata();
        RexVisitor<Expression<?>> visitor = OptUtils.createRexToExpressionVisitor(schema(parameterMetadata), parameterMetadata);
        return timestampExpression.accept(visitor);
    }

    public FunctionEx<ExpressionEvalContext, SlidingWindowPolicy> windowPolicyProvider() {
        return windowPolicyProvider;
    }

    public int numStages() {
        return numStages;
    }

    public List<Integer> windowStartIndexes() {
        return windowStartIndexes;
    }

    public List<Integer> windowEndIndexes() {
        return windowEndIndexes;
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        return OptUtils.schema(getRowType());
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
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
                aggrOp(),
                timestampExpression,
                windowPolicyProvider,
                numStages,
                windowStartIndexes, windowEndIndexes);
    }
}
