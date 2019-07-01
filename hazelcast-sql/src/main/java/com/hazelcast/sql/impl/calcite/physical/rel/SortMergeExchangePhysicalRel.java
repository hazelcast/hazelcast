package com.hazelcast.sql.impl.calcite.physical.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.List;

public class SortMergeExchangePhysicalRel extends SingleRel implements PhysicalRel {

    private final RelCollation collation;

    public SortMergeExchangePhysicalRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        RelCollation collation
    ) {
        super(cluster, traitSet, input);

        this.collation = collation;
    }

    public RelCollation getCollation() {
        return collation;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new SortMergeExchangePhysicalRel(getCluster(), traitSet, sole(inputs), collation);
    }
}
