package com.hazelcast.sql.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

public class HazelcastRootRel extends SingleRel implements HazelcastRel  {
    public HazelcastRootRel(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }

    @Override
    public void visitForPlan(PhysicalPlanVisitor visitor) {
        visitor.visit((HazelcastRel)getInput());
        visitor.visit(this);
    }
}
