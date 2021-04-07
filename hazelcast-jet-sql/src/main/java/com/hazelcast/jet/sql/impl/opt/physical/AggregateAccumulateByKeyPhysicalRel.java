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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.aggregate.ObjectArrayKey;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public class AggregateAccumulateByKeyPhysicalRel extends SingleRel implements PhysicalRel {

    private final ImmutableBitSet groupSet;
    private final AggregateOperation<?, Object[]> aggrOp;

    AggregateAccumulateByKeyPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            ImmutableBitSet groupSet,
            AggregateOperation<?, Object[]> aggrOp
    ) {
        super(cluster, traits, input);

        this.groupSet = groupSet;
        this.aggrOp = aggrOp;
    }

    public FunctionEx<Object[], ObjectArrayKey> groupKeyFn() {
        return ObjectArrayKey.projectFn(groupSet.toArray());
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
        return visitor.onAccumulateByKey(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                    .item("group", groupSet);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new AggregateAccumulateByKeyPhysicalRel(getCluster(), traitSet, sole(inputs), groupSet, aggrOp);
    }
}
