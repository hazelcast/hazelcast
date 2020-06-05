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

package com.hazelcast.sql.impl.calcite.opt.physical.agg;

import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PhysicalRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for physical aggregations.
 */
public class AggregatePhysicalRel extends Aggregate implements PhysicalRel {
    /** Sorted prefix of a group set.  */
    private final ImmutableBitSet sortedGroupSet;

    public AggregatePhysicalRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls,
        ImmutableBitSet sortedGroupSet
    ) {
        super(cluster, traits, new ArrayList<>(), child, groupSet, groupSets, aggCalls);

        this.sortedGroupSet = sortedGroupSet;
    }

    public ImmutableBitSet getSortedGroupSet() {
        return sortedGroupSet;
    }

    @Override
    public final Aggregate copy(
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls
    ) {
        return new AggregatePhysicalRel(
            getCluster(),
            traitSet,
            input,
            groupSet,
            groupSets,
            aggCalls,
            sortedGroupSet
        );
    }

    @Override
    public void visit(PhysicalRelVisitor visitor) {
        ((PhysicalRel) input).visit(visitor);

        visitor.onAggregate(this);
    }

    @Override
    public final RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw);
    }

    // TODO: Cost estimates: sorted, unsorted
}
