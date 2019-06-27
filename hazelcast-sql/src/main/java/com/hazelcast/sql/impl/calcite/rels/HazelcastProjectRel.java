package com.hazelcast.sql.impl.calcite.rels;

import com.hazelcast.sql.impl.calcite.SqlCacitePlanVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class HazelcastProjectRel extends Project implements HazelcastRel {
    public HazelcastProjectRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input,
        List<? extends RexNode> projects,
        RelDataType rowType
    ) {
        super(cluster, traits, input, projects, rowType);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new HazelcastProjectRel(getCluster(), traitSet, input, exps, rowType);
    }

    @Override
    public void visitForPlan(SqlCacitePlanVisitor visitor) {
        throw new UnsupportedOperationException();
    }
}
