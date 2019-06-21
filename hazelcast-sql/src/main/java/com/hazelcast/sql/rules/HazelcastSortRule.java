package com.hazelcast.sql.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;

public class HazelcastSortRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new HazelcastSortRule();

    private HazelcastSortRule() {
        super(
            // TODO: Why Convention.NONE is used in Drill?
            RelOptRule.operand(LogicalSort.class, RelOptRule.any()),
            RelFactories.LOGICAL_BUILDER,
            "HazelcastSortRule"
        );
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        // TODO: Sort with LIMIT/OFFSET will require different treatment>
        return super.matches(call);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);

        final RelNode input = sort.getInput();
        final RelTraitSet traits = sort.getTraitSet().plus(HazelcastRel.LOGICAL);

        // TODO: Why this call is needed?
        RelNode convertedInput = convert(input, input.getTraitSet().plus(HazelcastRel.LOGICAL).simplify());

        call.transformTo(new HazelcastSortRel(
            sort.getCluster(),
            sort.getTraitSet().plus(HazelcastRel.LOGICAL),
            convertedInput,
            sort.getCollation(),
            sort.offset,
            sort.fetch
        ));
    }
}
