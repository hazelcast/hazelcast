package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.TableModify;
import org.immutables.value.Value;

/**
 * A rule to match a TableModify[operation=update], with a Values as input
 */
@Value.Enclosing
class UpdateWithValuesLogicalRule extends RelRule<RelRule.Config> {

    static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    @Value.Immutable
    interface Config extends RelRule.Config {
        RelRule.Config DEFAULT = ImmutableUpdateWithValuesLogicalRule.Config.builder()
                .description(UpdateWithValuesLogicalRule.class.getSimpleName())
                .operandSupplier(b0 -> b0.operand(TableModifyLogicalRel.class)
                        .predicate(TableModify::isUpdate)
                        .inputs(b1 -> b1.operand(ValuesLogicalRel.class)
                                .noInputs())
                ).build();

        @Override
        default RelOptRule toRule() {
            return new UpdateWithValuesLogicalRule(this);
        }
    }

    UpdateWithValuesLogicalRule(RelRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        TableModifyLogicalRel update = call.rel(0);
        ValuesLogicalRel values = call.rel(1);

        UpdateLogicalRel rel = new UpdateLogicalRel(
                update.getCluster(),
                OptUtils.toLogicalConvention(update.getTraitSet()),
                update.getTable(),
                update.getCatalogReader(),
                OptUtils.toLogicalInput(values),
                update.getUpdateColumnList(),
                update.getSourceExpressionList(),
                update.isFlattened(),
                null
        );
        call.transformTo(rel);
    }
}
