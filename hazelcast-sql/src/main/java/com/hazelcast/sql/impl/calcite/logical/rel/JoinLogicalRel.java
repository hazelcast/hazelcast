package com.hazelcast.sql.impl.calcite.logical.rel;

import com.hazelcast.sql.impl.calcite.common.AbstractJoinRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;

public class JoinLogicalRel extends AbstractJoinRel implements LogicalRel {
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
        super(cluster, traitSet, left, right, condition, joinType, leftKeys, rightKeys);
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
