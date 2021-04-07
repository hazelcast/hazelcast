/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.List;

public class AggregateAccumulatePhysicalRel extends SingleRel implements PhysicalRel {

    private final AggregateOperation<?, Object[]> aggrOp;

    AggregateAccumulatePhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            AggregateOperation<?, Object[]> aggrOp

    ) {
        super(cluster, traits, input);

        this.aggrOp = aggrOp;
    }

    public AggregateOperation<?, Object[]> aggrOp() {
        return aggrOp;
    }

    @Override
    public PlanNodeSchema schema() {
        // intermediate operator, schema should not be ever needed
        throw new UnsupportedOperationException();
    }

    @Override
    public Vertex accept(CreateDagVisitor visitor) {
        return visitor.onAccumulate(this);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new AggregateAccumulatePhysicalRel(getCluster(), traitSet, sole(inputs), aggrOp);
    }
}
