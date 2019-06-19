package com.hazelcast.sql.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class HazelcastTableScanRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new HazelcastTableScanRule();

    private HazelcastTableScanRule() {
        super(
            RelOptRule.operand(LogicalTableScan.class, RelOptRule.any()),
            RelFactories.LOGICAL_BUILDER,
            "HazelcastTableScanRule"
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalTableScan access = call.rel(0);

        RelTraitSet traits = access.getTraitSet().plus(HazelcastRel.LOGICAL);

        call.transformTo(new HazelcastTableScanRel(access.getCluster(), traits, access.getTable()));
    }
}
