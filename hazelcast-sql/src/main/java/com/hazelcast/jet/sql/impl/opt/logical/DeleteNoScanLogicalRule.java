package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.immutables.value.Value;

/**
 * A rule that matches a TableModify[operation=delete], _without_ a TableScan as an input
 */
@Value.Enclosing
class DeleteNoScanLogicalRule extends RelRule<RelRule.Config> {

    static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    @Value.Immutable
    interface Config extends RelRule.Config {
        RelRule.Config DEFAULT = ImmutableDeleteNoScanLogicalRule.Config.builder()
                .description(DeleteNoScanLogicalRule.class.getSimpleName())
                .operandSupplier(b0 -> b0.operand(TableModify.class)
                        .predicate(TableModify::isDelete)
                        .inputs(b1 -> b1.operand(RelNode.class)
                                // DELETE with TableScan input is matched by the other rule
                                .predicate(input -> !(input instanceof TableScan))
                                .anyInputs())
                ).build();

        @Override
        default RelOptRule toRule() {
            return new DeleteNoScanLogicalRule(this);
        }
    }

    DeleteNoScanLogicalRule(RelRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        TableModify delete = call.rel(0);

        DeleteLogicalRel logicalDelete = new DeleteLogicalRel(
                delete.getCluster(),
                OptUtils.toLogicalConvention(delete.getTraitSet()),
                delete.getTable(),
                delete.getCatalogReader(),
                OptUtils.toLogicalInput(delete.getInput()),
                delete.isFlattened(),
                null
        );
        call.transformTo(logicalDelete);
    }
}
