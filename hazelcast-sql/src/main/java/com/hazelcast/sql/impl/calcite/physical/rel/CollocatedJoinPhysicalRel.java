package com.hazelcast.sql.impl.calcite.physical.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

// TODO: JavaDoc: describe traits propagation
public class CollocatedJoinPhysicalRel extends AbstractJoinPhysicalRel implements PhysicalRel {
    public CollocatedJoinPhysicalRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        List<Integer> leftKeys,
        List<Integer> rightKeys
    ) {
        super(cluster, traitSet, left, right, condition, joinType, leftKeys, rightKeys);
    }

    @Override
    public Join copy(
        RelTraitSet traitSet,
        RexNode conditionExpr,
        RelNode left,
        RelNode right,
        JoinRelType joinType,
        boolean semiJoinDone
    ) {
        return new CollocatedJoinPhysicalRel(
            getCluster(),
            traitSet,
            left,
            right,
            condition,
            joinType,
            leftKeys,
            rightKeys
        );
    }

    @Override
    protected void visitAfterInputs(PhysicalRelVisitor visitor) {
        visitor.onCollocatedJoin(this);
    }
}
