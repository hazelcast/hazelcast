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
import com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.schema.HazelcastRelOptTable;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.sql.impl.connector.HazelcastRexNode.wrap;
import static com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil.getJetSqlConnector;
import static java.util.Objects.requireNonNull;

/**
 * A rule to match a TableModify[operation=update], with a TableScan as input.
 * <p>
 * For an overall description, see {@link UpdateNoScanLogicalRule}.
 */
@Value.Enclosing
class UpdateWithScanLogicalRule extends RelRule<RelRule.Config> {

    static final RelOptRule INSTANCE = Config.DEFAULT.toRule();

    @Value.Immutable
    interface Config extends RelRule.Config {
        RelRule.Config DEFAULT = ImmutableUpdateWithScanLogicalRule.Config.builder()
                .description(UpdateWithScanLogicalRule.class.getSimpleName())
                .operandSupplier(b0 -> b0.operand(TableModifyLogicalRel.class)
                        .predicate(TableModify::isUpdate)
                        .inputs(b1 -> b1.operand(FullScanLogicalRel.class)
                                .noInputs())
                ).build();

        @Override
        default RelOptRule toRule() {
            return new UpdateWithScanLogicalRule(this);
        }
    }

    UpdateWithScanLogicalRule(RelRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        TableModifyLogicalRel update = call.rel(0);
        FullScanLogicalRel scan = call.rel(1);

        // IMap optimization to execute IMap operation directly
        boolean isImap = OptUtils.hasTableType(scan, PartitionedMapTable.class);
        if (!OptUtils.requiresJob(update) && isImap) {
            RelOptTable table = scan.getTable();
            RexNode keyCondition = OptUtils.extractKeyConstantExpression(table, update.getCluster().getRexBuilder());
            if (keyCondition != null) {
                UpdateByKeyMapLogicalRel rel = new UpdateByKeyMapLogicalRel(
                        update.getCluster(),
                        OptUtils.toLogicalConvention(update.getTraitSet()),
                        table,
                        keyCondition,
                        update.getUpdateColumnList(),
                        update.getSourceExpressionList()
                );
                call.transformTo(rel);
                return;
            }
        }

        HazelcastTable hzTable = OptUtils.extractHazelcastTable(scan);
        SqlConnector connector = getJetSqlConnector(hzTable.getTarget());
        if (connector.dmlSupportsPredicates()
                && (hzTable.getFilter() == null || connector.supportsExpression(wrap(hzTable.getFilter())))) {
            // push the predicate down to the UpdateLogicalRel and remove the scan
            UpdateLogicalRel rel = new UpdateLogicalRel(
                    update.getCluster(),
                    OptUtils.toLogicalConvention(update.getTraitSet()),
                    update.getTable(),
                    update.getCatalogReader(),
                    null,
                    requireNonNull(update.getUpdateColumnList()),
                    requireNonNull(update.getSourceExpressionList()),
                    update.isFlattened(),
                    hzTable.getFilter()
            );

            call.transformTo(rel);
            return;
        }

        // keep the scan as is, convert the TableModify[delete] to UpdateLogicalRel
        UpdateLogicalRel rel = new UpdateLogicalRel(
                update.getCluster(),
                OptUtils.toLogicalConvention(update.getTraitSet()),
                update.getTable(),
                update.getCatalogReader(),
                // this is IMap specific, in other cases we need to project more that just a key
                // TODO: we should project only columns needed in the update
                isImap ? rewriteScan(scan) : scan,
                requireNonNull(update.getUpdateColumnList()),
                requireNonNull(update.getSourceExpressionList()),
                update.isFlattened(),
                null
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
