package com.hazelcast.sql.impl.calcite.physical.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.List;

public class HazelcastSingletonExchangePhysicalRel extends SingleRel implements HazelcastPhysicalRel {
    public HazelcastSingletonExchangePhysicalRel(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new HazelcastSingletonExchangePhysicalRel(getCluster(), traitSet, sole(inputs));
    }
}
