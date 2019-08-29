package com.hazelcast.sql.impl.calcite.physical.rel;

import com.hazelcast.sql.impl.calcite.common.AbstractJoinRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Common class for all physical joins.
 */
public abstract class AbstractJoinPhysicalRel extends AbstractJoinRel implements PhysicalRel {
    public AbstractJoinPhysicalRel(
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
    public void visit(PhysicalRelVisitor visitor) {
        // Visit inputs in right-left order, so that they are retrieved later from the stack in left-right order.
        ((PhysicalRel)right).visit(visitor);
        ((PhysicalRel)left).visit(visitor);

        visitAfterInputs(visitor);
    }

    /**
     * Visit the join after the inputs are processed.
     *
     * @param visitor Visitor.
     */
    protected abstract void visitAfterInputs(PhysicalRelVisitor visitor);
}
