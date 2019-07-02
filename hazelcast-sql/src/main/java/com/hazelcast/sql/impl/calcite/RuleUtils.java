package com.hazelcast.sql.impl.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;

/**
 * Static utility classes for rules.
 */
public class RuleUtils {
    /**
     * Get operand matching a single node.
     *
     * @param cls Node class.
     * @param convention Convention.
     * @return Operand.
     */
    public static <R extends RelNode> RelOptRuleOperand single(Class<R> cls, Convention convention) {
        return RelOptRule.operand(cls, convention, RelOptRule.any());
    }

    /**
     * Get operand matching a node with specific child node.
     *
     * @param cls Node class.
     * @param childCls Child node class.
     * @param convention Convention.
     * @return Operand.
     */
    public static <R1 extends RelNode, R2 extends RelNode> RelOptRuleOperand parentChild(Class<R1> cls,
        Class<R2> childCls, Convention convention) {
        RelOptRuleOperand childOperand = RelOptRule.operand(childCls, RelOptRule.any());

        return RelOptRule.operand(cls, convention, RelOptRule.some(childOperand));
    }

    private RuleUtils() {
        // No-op.
    }
}
