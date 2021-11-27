package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.JoinLogicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;
import static com.hazelcast.jet.sql.impl.opt.Conventions.PHYSICAL;

public class JoinPhysicalRule extends RelRule<RelRule.Config> {

    private static final Config RULE_CONFIG = Config.EMPTY
            .withDescription(JoinPhysicalRule.class.getSimpleName())
            .withOperandSupplier(b0 -> b0.operand(JoinLogicalRel.class)
                    .trait(LOGICAL)
                    .inputs(
                            b1 -> b1.operand(RelNode.class).anyInputs(),
                            b2 -> b2.operand(RelNode.class)
                                    .trait(LOGICAL)
                                    .noInputs()));

    @SuppressWarnings("checkstyle:DeclarationOrder")
    static final RelOptRule INSTANCE = new JoinPhysicalRule();

    private JoinPhysicalRule() {
        super(RULE_CONFIG);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        JoinLogicalRel logicalJoin = call.rel(0);

        JoinRelType joinType = logicalJoin.getJoinType();
        if (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT) {
            throw new RuntimeException("Unexpected joinType: " + joinType);
        }

        RelNode leftInput = call.rel(1);
        RelNode rightInput = call.rel(2);
        RelNode leftInputConverted = RelRule.convert(leftInput, leftInput.getTraitSet().replace(PHYSICAL));
        RelNode rightInputConverted = RelRule.convert(rightInput, rightInput.getTraitSet().replace(PHYSICAL));
        HazelcastTable rightHzTable = rightInput.getTable().unwrap(HazelcastTable.class);

        // only use hash join if there's some equi-condition
        if (!logicalJoin.analyzeCondition().leftKeys.isEmpty()) {
            RelNode rel = new JoinHashPhysicalRel(
                    logicalJoin.getCluster(),
                    logicalJoin.getTraitSet().replace(PHYSICAL),
                    leftInputConverted,
                    rightInputConverted,
                    logicalJoin.getCondition(),
                    logicalJoin.getJoinType());
            call.transformTo(rel);
        }

        if (rightInput instanceof TableScan
                && OptUtils.hasTableType(rightInput, PartitionedMapTable.class)
                && SqlConnectorUtil.getJetSqlConnector(rightHzTable.getTarget()).isNestedLoopReaderSupported()) {
            RelNode rel = new JoinNestedLoopPhysicalRel(
                    logicalJoin.getCluster(),
                    OptUtils.toPhysicalConvention(logicalJoin.getTraitSet()),
                    leftInputConverted,
                    rightInputConverted,
                    logicalJoin.getCondition(),
                    logicalJoin.getJoinType()
            );
            call.transformTo(rel);
        }
    }
}
