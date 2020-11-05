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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.aggregate.SqlAggregations;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

public class AggregateCombinePhysicalRel extends Aggregate implements PhysicalRel {

    private final AggregateOperation<SqlAggregations, Object[]> aggrOp;

    AggregateCombinePhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            AggregateOperation<SqlAggregations, Object[]> aggrOp
    ) {
        super(cluster, traits, new ArrayList<>(), input, groupSet, groupSets, aggCalls);

        this.aggrOp = aggrOp;
    }

    public AggregateOperation<SqlAggregations, Object[]> aggrOp() {
        return aggrOp;
    }

    @Override
    public PlanNodeSchema schema() {
        return OptUtils.schema(getRowType());
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onCombine(this);
    }

    @Override
    public final Aggregate copy(
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls
    ) {
        return new AggregateCombinePhysicalRel(getCluster(), traitSet, input, groupSet, groupSets, aggCalls, aggrOp);
    }
}
