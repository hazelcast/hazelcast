package com.hazelcast.sql.impl.calcite.rels;

import com.hazelcast.sql.impl.calcite.SqlCacitePlanVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

public class HazelcastRootRel extends SingleRel implements HazelcastRel  {
    public HazelcastRootRel(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }

    @Override
    public void visitForPlan(SqlCacitePlanVisitor visitor) {
        ((HazelcastRel)getInput()).visitForPlan(visitor);

        visitor.visit(this);
    }
}
