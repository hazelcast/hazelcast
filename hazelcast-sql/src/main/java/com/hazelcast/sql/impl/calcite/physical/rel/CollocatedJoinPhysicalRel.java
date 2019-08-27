package com.hazelcast.sql.impl.calcite.physical.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;

public class CollocatedJoinPhysicalRel extends Join implements PhysicalRel {
    /** Indexes of left keys. */
    protected final List<Integer> leftKeys;

    /** Indexes of right keys. */
    protected final List<Integer> rightKeys;

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
        super(cluster, traitSet, left, right, condition, Collections.emptySet(), joinType);

        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
    }

    public List<Integer> getLeftKeys() {
        return leftKeys;
    }

    public List<Integer> getRightKeys() {
        return rightKeys;
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
    public void visit(PhysicalRelVisitor visitor) {
        ((PhysicalRel)right).visit(visitor);
        ((PhysicalRel)left).visit(visitor);

        visitor.onCollocatedJoin(this);
    }
}
