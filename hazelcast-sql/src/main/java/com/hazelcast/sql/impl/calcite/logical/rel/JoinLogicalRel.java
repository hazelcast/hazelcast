package com.hazelcast.sql.impl.calcite.logical.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;

public class JoinLogicalRel extends Join implements LogicalRel {
    /** Indexes of left keys. */
    protected final List<Integer> leftKeys;

    /** Indexes of right keys. */
    protected final List<Integer> rightKeys;

    public JoinLogicalRel(
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
        RexNode condition,
        RelNode left,
        RelNode right,
        JoinRelType joinType,
        boolean semiJoinDone
    ) {
        return new JoinLogicalRel(getCluster(), traitSet, left, right, condition, joinType, leftKeys, rightKeys);
    }
}
