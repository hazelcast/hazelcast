/*
 * Copyright 2023 Hazelcast Inc.
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
 * A rule that matches a TableModify[operation=delete] with an input other than
 * a {@link TableScan} (that's handled by {@link DeleteWithScanLogicalRule}).
 * <p>
 * For an overall description, see {@link UpdateNoScanLogicalRule}.
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
