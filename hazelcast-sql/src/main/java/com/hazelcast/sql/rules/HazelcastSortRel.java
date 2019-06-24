package com.hazelcast.sql.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;

public class HazelcastSortRel extends Sort implements HazelcastRel {
    public HazelcastSortRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelCollation collation
    ) {
        super(cluster, traits, child, collation);
    }

    public HazelcastSortRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelCollation collation,
        RexNode offset,
        RexNode fetch
    ) {
        super(cluster, traits, child, collation, offset, fetch);
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new HazelcastSortRel(getCluster(), traitSet, input, collation, offset, fetch);
    }

    @Override
    public void visitForPlan(PhysicalPlanVisitor visitor) {
        visitor.visit((HazelcastRel)getInput());

        visitor.visit(this);
    }
}
