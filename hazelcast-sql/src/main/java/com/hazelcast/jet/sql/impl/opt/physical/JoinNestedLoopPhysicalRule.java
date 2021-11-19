/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
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

public final class JoinNestedLoopPhysicalRule extends RelRule<RelRule.Config> {

    private static final Config RULE_CONFIG = Config.EMPTY
            .withDescription(JoinNestedLoopPhysicalRule.class.getSimpleName())
            .withOperandSupplier(b0 -> b0.operand(JoinLogicalRel.class)
                    .trait(LOGICAL)
                    .inputs(
                            b1 -> b1.operand(RelNode.class).anyInputs(),
                            b2 -> b2.operand(TableScan.class)
                                    .trait(LOGICAL)
                                    .predicate(scan -> OptUtils.hasTableType(scan, PartitionedMapTable.class))
                                    .noInputs()));

    @SuppressWarnings("checkstyle:DeclarationOrder")
    static final RelOptRule INSTANCE = new JoinNestedLoopPhysicalRule();

    private JoinNestedLoopPhysicalRule() {
        super(RULE_CONFIG);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        JoinLogicalRel logicalJoin = call.rel(0);

        JoinRelType joinType = logicalJoin.getJoinType();
        assert joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT;

        RelNode leftInput = call.rel(1);
        TableScan rightScan = call.rel(2);

        RelNode leftInputConverted = convert(leftInput, leftInput.getTraitSet().replace(PHYSICAL));
        RelNode rightScanConverted = convert(rightScan, rightScan.getTraitSet().replace(PHYSICAL));

        HazelcastTable rightHzTable = rightScan.getTable().unwrap(HazelcastTable.class);
        SqlConnector rightSqlConnector = SqlConnectorUtil.getJetSqlConnector(rightHzTable.getTarget());
        if (!rightSqlConnector.isNestedLoopReaderSupported()) {
            return;
        }

        RelNode rel = new JoinNestedLoopPhysicalRel(
                logicalJoin.getCluster(),
                OptUtils.toPhysicalConvention(logicalJoin.getTraitSet()),
                leftInputConverted,
                rightScanConverted,
                logicalJoin.getCondition(),
                logicalJoin.getJoinType()
        );
        call.transformTo(rel);
    }
}
