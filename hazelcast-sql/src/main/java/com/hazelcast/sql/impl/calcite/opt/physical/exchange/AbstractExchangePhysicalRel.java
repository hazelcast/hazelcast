package com.hazelcast.sql.impl.calcite.opt.physical.exchange;

import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

/**
 * Base class for exchanges.
 */
public abstract class AbstractExchangePhysicalRel extends SingleRel implements PhysicalRel {
    public AbstractExchangePhysicalRel(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);

        assert HazelcastRelOptCluster.cast(cluster).getMemberCount() > 1 :
            "Exchange should not be instantiated on single-member topology.";
    }
}
