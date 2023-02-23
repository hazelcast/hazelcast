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

import com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.DeleteLogicalRules.DeleteWithScanRule;
import com.hazelcast.jet.sql.impl.opt.logical.UpdateLogicalRules.ScanRule.ScanInstanceConfig;
import com.hazelcast.jet.sql.impl.opt.logical.UpdateLogicalRules.ValuesRule.ValuesInstanceConfig;
import com.hazelcast.jet.sql.impl.schema.HazelcastRelOptTable;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

// no support for UPDATE FROM SELECT case (which is not yet implemented)
// once joins are there we need to create complementary rule
@Value.Enclosing
final class UpdateLogicalRules {

    static final RelOptRule SCAN_INSTANCE = ScanInstanceConfig.DEFAULT.toRule();
    static final RelOptRule VALUES_INSTANCE = ValuesInstanceConfig.DEFAULT.toRule();

    static class ScanRule extends RelRule<RelRule.Config> {
        @Value.Immutable
        interface ScanInstanceConfig extends RelRule.Config {
            DeleteWithScanRule.Config DEFAULT = ImmutableUpdateLogicalRules.ScanInstanceConfig.builder()
                    .description(ScanRule.class.getSimpleName())
                    .operandSupplier(b0 -> b0.operand(TableModifyLogicalRel.class)
                            .predicate(TableModify::isUpdate)
                            .inputs(b1 -> b1.operand(FullScanLogicalRel.class)
                                    .noInputs())
                    ).build();

            @Override
            default RelOptRule toRule() {
                return new ScanRule(this);
            }
        }

        ScanRule(RelRule.Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            TableModifyLogicalRel update = call.rel(0);
            FullScanLogicalRel scan = call.rel(1);

            UpdateLogicalRel rel = new UpdateLogicalRel(
                    update.getCluster(),
                    OptUtils.toLogicalConvention(update.getTraitSet()),
                    update.getTable(),
                    update.getCatalogReader(),
                    rewriteScan(scan),
                    update.getUpdateColumnList(),
                    update.getSourceExpressionList(),
                    update.isFlattened()
            );
            call.transformTo(rel);
        }

        // rewrites existing project to just primary keys project
        private RelNode rewriteScan(FullScanLogicalRel scan) {
            HazelcastRelOptTable relTable = (HazelcastRelOptTable) scan.getTable();
            HazelcastTable hzTable = relTable.unwrap(HazelcastTable.class);

            List<RexNode> keyProjects = keyProjects(scan.getCluster().getRexBuilder(), hzTable.getTarget());
            HazelcastRelOptTable convertedTable = OptUtils.createRelTable(relTable,
                    hzTable.withProject(keyProjects, null), scan.getCluster().getTypeFactory());

            return new FullScanLogicalRel(
                    scan.getCluster(),
                    OptUtils.toLogicalConvention(scan.getTraitSet()),
                    convertedTable,
                    null,
                    -1
            );
        }

        public List<RexNode> keyProjects(RexBuilder rexBuilder, Table table) {
            List<String> primaryKey = SqlConnectorUtil.getJetSqlConnector(table).getPrimaryKey(table);
            List<RexNode> res = new ArrayList<>(primaryKey.size());
            for (int i = 0; i < table.getFieldCount(); i++) {
                TableField field = table.getField(i);
                if (primaryKey.contains(field.getName())) {
                    RelDataType type = OptUtils.convert(field, HazelcastTypeFactory.INSTANCE);
                    res.add(rexBuilder.makeInputRef(type, i));
                }
            }
            return res;
        }
    }


    // Calcite replaces the table scan with empty VALUES when the WHERE clause
    // is always false
    // i.e. '... WHERE __key = 1 AND __key = 2'
    // could/should be optimized to no-op

    static class ValuesRule extends RelRule<RelRule.Config> {

        @Value.Immutable
        interface ValuesInstanceConfig extends RelRule.Config {
            DeleteWithScanRule.Config DEFAULT = ImmutableUpdateLogicalRules.ValuesInstanceConfig.builder()
                    .description(ValuesRule.class.getSimpleName())
                    .operandSupplier(b0 -> b0.operand(TableModifyLogicalRel.class)
                            .predicate(TableModify::isUpdate)
                            .inputs(b1 -> b1.operand(ValuesLogicalRel.class)
                                    .noInputs())
                    ).build();

            @Override
            default RelOptRule toRule() {
                return new ValuesRule(this);
            }
        }

        ValuesRule(RelRule.Config config) {
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
                    update.isFlattened()
            );
            call.transformTo(rel);
        }
    }

    private UpdateLogicalRules() {
    }
}
