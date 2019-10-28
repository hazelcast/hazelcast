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

import com.hazelcast.sql.impl.calcite.logical.rel.AggregateLogicalRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Base class for physical aggregations.
 */
public abstract class AbstractAggregatePhysicalRel extends Aggregate implements PhysicalRel {
    /**
     * Whether the input is already sorted on group key. When this is the case, the aggregation could be performed
     * in non-blocking fashion.
     */
    protected final boolean sorted;

    protected AbstractAggregatePhysicalRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        boolean indicator,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls,
        boolean sorted
    ) {
        super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);

        this.sorted = sorted;
    }

    public boolean isSorted() {
        return sorted;
    }

    @Override
    public final RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("sorted", sorted);
    }
}
