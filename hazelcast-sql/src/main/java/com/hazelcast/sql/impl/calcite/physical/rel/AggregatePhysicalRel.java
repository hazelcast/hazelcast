package com.hazelcast.sql.impl.calcite.physical.rel;

import com.hazelcast.sql.impl.calcite.logical.rel.AggregateLogicalRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Physical aggregation.
 */
public class AggregatePhysicalRel extends Aggregate implements PhysicalRel {
    public AggregatePhysicalRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        boolean indicator,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls
    ) {
        super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new AggregatePhysicalRel(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls);
    }

    @Override
    public void visit(PhysicalRelVisitor visitor) {
        ((PhysicalRel)input).visit(visitor);

        visitor.onAggregate(this);
    }
}
