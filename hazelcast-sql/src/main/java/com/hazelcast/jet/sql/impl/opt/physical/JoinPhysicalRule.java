/*
package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.JoinLogicalRel;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import static com.hazelcast.jet.sql.impl.opt.Conventions.LOGICAL;

public class JoinPhysicalRule extends RelRule<RelRule.Config> {

    private static final Config RULE_CONFIG = Config.EMPTY
            .withDescription(JoinPhysicalRule.class.getSimpleName())
            .withOperandSupplier(b0 -> b0.operand(JoinLogicalRel.class)
                    .trait(LOGICAL)
                    .inputs(
                            b1 -> b1.operand(RelNode.class).anyInputs(),
                            b2 -> b2.operand(TableScan.class)
                                    .trait(LOGICAL)
                                    .noInputs()));

    @SuppressWarnings("checkstyle:DeclarationOrder")
    static final RelOptRule INSTANCE = new JoinPhysicalRule();

    private JoinPhysicalRule() {
        super(RULE_CONFIG);
    }
}
*/
