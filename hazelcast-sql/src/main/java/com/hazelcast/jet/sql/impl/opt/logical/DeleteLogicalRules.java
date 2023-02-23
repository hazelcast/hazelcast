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
import com.hazelcast.jet.sql.impl.opt.logical.DeleteLogicalRules.DeleteNoScanRule.NoScanConfig;
import com.hazelcast.jet.sql.impl.opt.logical.DeleteLogicalRules.DeleteWithScanRule.WithScanConfig;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

import java.util.List;

import static com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil.getJetSqlConnector;

@Value.Enclosing
public final class DeleteLogicalRules {

    static final RelOptRule SCAN_INSTANCE = WithScanConfig.DEFAULT.toRule();
    static final RelOptRule NO_SCAN_INSTANCE = NoScanConfig.DEFAULT.toRule();

    static class DeleteWithScanRule extends RelRule<RelRule.Config> {
        @Value.Immutable
        interface WithScanConfig extends RelRule.Config {
            DeleteWithScanRule.Config DEFAULT = ImmutableDeleteLogicalRules.WithScanConfig.builder()
                    .description(DeleteWithScanRule.class.getSimpleName())
                    .operandSupplier(b0 -> b0.operand(TableModify.class)
                            .predicate(TableModify::isDelete)
                            .inputs(b1 -> b1.operand(TableScan.class)
                                    .noInputs())
                    ).build();

            @Override
            default RelOptRule toRule() {
                return new DeleteWithScanRule(this);
            }
        }

        DeleteWithScanRule(RelRule.Config config) {
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
            if (getJetSqlConnector(hzTable.getTarget()).dmlSupportsPredicates()) {
                // TODO do we need this check?
                List<RexNode> projects = hzTable.getProjects();
                SqlConnector connector = getJetSqlConnector(hzTable.getTarget());
                List<String> primaryKey = connector.getPrimaryKey(hzTable.getTarget());
                assert primaryKey.size() == projects.size() : "the projection isn't just for the primary key";
                for (int i = 0; i < primaryKey.size(); i++) {
                    int fieldIndex = ((RexInputRef) projects.get(i)).getIndex();
                    String fieldName = scan.getTable().getRowType().getFieldList().get(fieldIndex).getName();
                    assert fieldName.equals(primaryKey.get(i)) : "the projection isn't just for the primary key";
                }

                DeleteLogicalRel rel = new DeleteLogicalRel(
                        delete.getCluster(),
                        OptUtils.toLogicalConvention(delete.getTraitSet()),
                        delete.getTable(),
                        delete.getCatalogReader(),
                        OptUtils.toLogicalInput(delete.getInput()),
                        delete.isFlattened(),
                        hzTable.getFilter()
                );
                call.transformTo(rel);
                return;
            }

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


    static class DeleteNoScanRule extends RelRule<RelRule.Config> {
        @Value.Immutable
        interface NoScanConfig extends RelRule.Config {
            DeleteNoScanRule.Config DEFAULT = ImmutableDeleteLogicalRules.NoScanConfig.builder()
                    .description(DeleteNoScanRule.class.getSimpleName())
                    .operandSupplier(b0 -> b0.operand(TableModify.class)
                            .predicate(TableModify::isDelete)
                            .inputs(b1 -> b1.operand(RelNode.class)
                                    // DELETE with TableScan input is matched by the other rule
                                    .predicate(input -> !(input instanceof TableScan))
                                    .anyInputs())
                    ).build();

            @Override
            default RelOptRule toRule() {
                return new DeleteNoScanRule(this);
            }
        }

        DeleteNoScanRule(RelRule.Config config) {
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

    private DeleteLogicalRules() { }
}
