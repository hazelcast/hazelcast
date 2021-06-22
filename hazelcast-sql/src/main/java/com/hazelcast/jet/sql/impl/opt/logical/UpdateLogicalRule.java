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

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.schema.HazelcastRelOptTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.Table;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

// does not cover UPDATE FROM SELECT case (which is not yet supported)
// once joins are implemented we need to revisit this rule or create complementary one
final class UpdateLogicalRule extends RelOptRule {

    static final RelOptRule INSTANCE = new UpdateLogicalRule();

    private UpdateLogicalRule() {
        super(
                operandJ(LogicalTableModify.class, null, TableModify::isUpdate,
                        operand(LogicalProject.class, operand(LogicalTableScan.class, none()))),
                RelFactories.LOGICAL_BUILDER,
                UpdateLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalTableModify update = call.rel(0);
        LogicalTableScan scan = call.rel(2);

        UpdateLogicalRel rel = new UpdateLogicalRel(
                update.getCluster(),
                OptUtils.toLogicalConvention(update.getTraitSet()),
                update.getTable(),
                update.getCatalogReader(),
                rewriteScan(scan),
                update.getOperation(),
                update.getUpdateColumnList(),
                update.getSourceExpressionList(),
                update.isFlattened()
        );
        call.transformTo(rel);
    }

    // rewrites existing project to just primary keys project
    private static RelNode rewriteScan(LogicalTableScan scan) {
        HazelcastRelOptTable relTable = (HazelcastRelOptTable) scan.getTable();
        HazelcastTable hazelcastTable = relTable.unwrap(HazelcastTable.class);

        return new FullScanLogicalRel(
                scan.getCluster(),
                OptUtils.toLogicalConvention(scan.getTraitSet()),
                OptUtils.createRelTable(
                        relTable.getDelegate().getQualifiedName(),
                        hazelcastTable.withProject(keyProjects(hazelcastTable.getTarget())),
                        scan.getCluster().getTypeFactory()
                )
        );
    }

    private static List<Integer> keyProjects(Table table) {
        List<String> primaryKey = SqlConnectorUtil.getJetSqlConnector(table).getPrimaryKey(table);
        return IntStream.range(0, table.getFieldCount())
                .filter(i -> primaryKey.contains(table.getField(i).getName()))
                .boxed()
                .collect(toList());
    }
}
