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

package com.hazelcast.sql.impl.calcite.physical.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Collocated physical aggregation is performed locally on top of locally available data set. This includes:
 * - SINGLETON and REPLICATED inputs, because the whole data set is available on the local member;
 * - DISTRIBUTED_PARTITIONED when distribution fields are a prefix of grouping fields.
 *
 * That is, {@code SELECT a FROM t GROUP BY a, b} is collocated if {@code a} is the distribution field, while
 * {@code SELECT a FROM t GROUP BY b} is not.
 */
public class CollocatedAggregatePhysicalRel extends AbstractAggregatePhysicalRel {
    public CollocatedAggregatePhysicalRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        boolean indicator,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls,
        boolean sorted
    ) {
        super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls, sorted);
    }

    @Override
    public final Aggregate copy(
        RelTraitSet traitSet,
        RelNode input,
        boolean indicator,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls
    ) {
        return new CollocatedAggregatePhysicalRel(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls, sorted);
    }

    @Override
    public void visit(PhysicalRelVisitor visitor) {
        ((PhysicalRel) input).visit(visitor);

        visitor.onCollocatedAggregate(this);
    }
}
