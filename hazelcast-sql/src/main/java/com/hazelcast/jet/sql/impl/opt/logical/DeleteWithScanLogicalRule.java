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

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

import static com.hazelcast.jet.sql.impl.connector.HazelcastRexNode.wrap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil.getJetSqlConnector;

/**
 * A rule that matches a TableModify[operation=delete], _with_ a {@link
 * TableScan} as an input.
 * <p>
 * For an overall description, see {@link UpdateNoScanLogicalRule}.
 */
@Value.Enclosing
class DeleteWithScanLogicalRule extends RelRule<RelRule.Config> {

    static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    @Value.Immutable
    interface Config extends RelRule.Config {
        RelRule.Config DEFAULT = ImmutableDeleteWithScanLogicalRule.Config.builder()
                .description(DeleteWithScanLogicalRule.class.getSimpleName())
                .operandSupplier(b0 -> b0.operand(TableModify.class)
                        .predicate(TableModify::isDelete)
                        .inputs(b1 -> b1.operand(TableScan.class)
                                .noInputs())
                ).build();

        @Override
        default RelOptRule toRule() {
            return new DeleteWithScanLogicalRule(this);
        }
    }

    DeleteWithScanLogicalRule(RelRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        TableModify delete = call.rel(0);
        TableScan scan = call.rel(1);

        // IMap optimization to execute IMap.delete() directly
        if (!OptUtils.requiresJob(delete) && OptUtils.hasTableType(scan, PartitionedMapTable.class)) {
            RexNode keyCondition =
                    OptUtils.extractKeyConstantExpression(scan.getTable(), delete.getCluster().getRexBuilder());
            if (keyCondition != null) {
                DeleteByKeyMapLogicalRel rel = new DeleteByKeyMapLogicalRel(
                        delete.getCluster(),
                        OptUtils.toLogicalConvention(delete.getTraitSet()),
                        scan.getTable(),
                        keyCondition
                );
                call.transformTo(rel);
                return;
            }
        }

        HazelcastTable hzTable = OptUtils.extractHazelcastTable(scan);
        SqlConnector connector = getJetSqlConnector(hzTable.getTarget());
        if (connector.dmlSupportsPredicates()
                && (hzTable.getFilter() == null || connector.supportsExpression(wrap(hzTable.getFilter())))) {
            // push the predicate down to the DeleteLogicalRel and remove the scan
            DeleteLogicalRel rel = new DeleteLogicalRel(
                    delete.getCluster(),
                    OptUtils.toLogicalConvention(delete.getTraitSet()),
                    delete.getTable(),
                    delete.getCatalogReader(),
                    null,
                    delete.isFlattened(),
                    hzTable.getFilter()
            );
            call.transformTo(rel);
            return;
        }

        // keep the scan as is, convert the TableModify[delete] to DeleteLogicalRel
        DeleteLogicalRel logicalDelete = new DeleteLogicalRel(
                delete.getCluster(),
                OptUtils.toLogicalConvention(delete.getTraitSet()),
                delete.getTable(),
                delete.getCatalogReader(),
                OptUtils.toLogicalInput(delete.getInput()),
                delete.isFlattened(),
                null
        );
        RelNode newRel = call.builder().push(scan).push(logicalDelete).build();
        call.transformTo(newRel);
    }
}
