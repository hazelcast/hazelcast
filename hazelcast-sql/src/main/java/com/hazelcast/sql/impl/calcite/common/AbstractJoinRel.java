package com.hazelcast.sql.impl.calcite.common;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;

/**
 * Common class for join relations.
 */
public abstract class AbstractJoinRel extends Join {
    /** Indexes of left keys with equi-join conditions. */
    protected final List<Integer> leftKeys;

    /** Indexes of right keys with equi-join conditions. */
    protected final List<Integer> rightKeys;

    protected AbstractJoinRel(
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
}
