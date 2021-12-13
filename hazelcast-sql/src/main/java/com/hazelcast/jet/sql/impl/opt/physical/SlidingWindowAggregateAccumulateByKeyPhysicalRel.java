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
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.ObjectArrayKey;
import com.hazelcast.jet.sql.impl.opt.metadata.WindowProperties;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public class SlidingWindowAggregateAccumulateByKeyPhysicalRel extends SingleRel implements PhysicalRel {

    private final ImmutableBitSet groupSet;
    private final AggregateOperation<?, Object[]> aggrOp;
    private final WindowProperties.WindowProperty windowProperty;

    SlidingWindowAggregateAccumulateByKeyPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            ImmutableBitSet groupSet,
            AggregateOperation<?, Object[]> aggrOp,
            WindowProperties.WindowProperty windowProperty
    ) {
        super(cluster, traits, input);

        this.groupSet = groupSet;
        this.aggrOp = aggrOp;
        this.windowProperty = windowProperty;
    }

    public FunctionEx<Object[], ObjectArrayKey> groupKeyFn() {
        return ObjectArrayKey.projectFn(groupSet.toArray());
    }

    public AggregateOperation<?, Object[]> aggrOp() {
        return aggrOp;
    }

    public WindowProperties.WindowProperty windowProperty() {
        return windowProperty;
    }

    @Override
    public PlanNodeSchema schema(QueryParameterMetadata parameterMetadata) {
        // intermediate operator, schema should not be ever needed
        throw new UnsupportedOperationException();
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onSlidingWindowAccumulateByKey(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("group", groupSet);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new SlidingWindowAggregateAccumulateByKeyPhysicalRel(
                getCluster(),
                traitSet,
                sole(inputs),
                groupSet,
                aggrOp,
                windowProperty
        );
    }
}
