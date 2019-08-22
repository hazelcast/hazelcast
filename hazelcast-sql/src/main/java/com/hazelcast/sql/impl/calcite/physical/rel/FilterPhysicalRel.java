package com.hazelcast.sql.impl.calcite.physical.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

/**
 * Physical projection.
 * <p>
 * Traits:
 * <ul>
 *     <li><b>Collation</b>: inherited from the input</li>
 *     <li><b>Distribution</b>: inherited form the input</li>
 * </ul>
 */
public class FilterPhysicalRel extends Filter implements PhysicalRel {
    public FilterPhysicalRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input,
        RexNode condition
    ) {
        super(cluster, traits, input, condition);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new FilterPhysicalRel(getCluster(), traitSet, input, condition);
    }

    @Override
    public void visit(PhysicalRelVisitor visitor) {
        ((PhysicalRel)input).visit(visitor);

        visitor.onFilter(this);
    }
}
