package com.hazelcast.sql.impl.calcite.rels;

import com.hazelcast.sql.impl.calcite.SqlCacitePlanVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

public class HazelcastFilterRel extends Filter implements HazelcastRel {
    public HazelcastFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
        super(cluster, traits, child, condition);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new HazelcastFilterRel(getCluster(), traitSet, input, condition);
    }

    @Override
    public void visitForPlan(SqlCacitePlanVisitor visitor) {
        throw new UnsupportedOperationException();
    }
}
