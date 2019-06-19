package com.hazelcast.sql.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;

public class HazelcastFilterRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new HazelcastFilterRule();

    private HazelcastFilterRule() {
        super(
            // TODO: Why convention NONE is used in Drill?
            RelOptRule.operand(LogicalFilter.class, RelOptRule.any()),
            RelFactories.LOGICAL_BUILDER,
            "HazelcastFilterRule"
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalFilter filter = call.rel(0);
        final RelNode input = filter.getInput();

        // TODO: What is this?
        final RelNode convertedInput = convert(input, input.getTraitSet().plus(HazelcastRel.LOGICAL).simplify());

        call.transformTo(new HazelcastFilterRel(
            filter.getCluster(),
            convertedInput.getTraitSet(),
            convertedInput,
            filter.getCondition()
        ));
    }
}
