package com.hazelcast.sql.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class HazelcastProjectIntoScanRule extends RelOptRule {
    public static final RelOptRule INSTANCE = new HazelcastProjectIntoScanRule();

    private HazelcastProjectIntoScanRule() {
        super(
            RelOptRule.operand(LogicalProject.class, RelOptRule.some(RelOptRule.operand(LogicalTableScan.class, RelOptRule.any()))),
            RelFactories.LOGICAL_BUILDER,
            "HazelcastFilterRule"
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        TableScan scan = call.rel(1);

        return;
    }
}
