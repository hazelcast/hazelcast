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

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

/**
 * Planner rule that matches single key, constant expression,
 * {@link PartitionedMapTable} DELETE.
 * <p>For example,</p>
 * <blockquote><code>DELETE FROM map WHERE __key = 1</code></blockquote>
 * <p>
 * Such DELETE is translated to optimized, direct key {@code IMap} operation
 * which does not involve starting any job.
 */
public final class DeleteByKeyMapLogicalRule extends RelOptRule {

    static final RelOptRule INSTANCE = new DeleteByKeyMapLogicalRule();

    private DeleteByKeyMapLogicalRule() {
        super(
                operandJ(
                        LogicalTableModify.class, null, TableModify::isDelete,
                        operandJ(LogicalTableScan.class,
                                null,
                                scan -> OptUtils.hasTableType(scan, PartitionedMapTable.class),
                                none()
                        )
                ),
                RelFactories.LOGICAL_BUILDER,
                DeleteByKeyMapLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalTableModify delete = call.rel(0);
        LogicalTableScan scan = call.rel(1);

        HazelcastTable table = scan.getTable().unwrap(HazelcastTable.class);
        RexNode keyCondition = extractConstantExpression(table, delete.getCluster().getRexBuilder());
        if (keyCondition != null) {
            DeleteByKeyMapLogicalRel rel = new DeleteByKeyMapLogicalRel(
                    delete.getCluster(),
                    OptUtils.toLogicalConvention(delete.getTraitSet()),
                    table.getTarget(),
                    keyCondition
            );
            call.transformTo(rel);
        }
    }

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    private RexNode extractConstantExpression(HazelcastTable table, RexBuilder rexBuilder) {
        RexNode filter = table.getFilter();
        if (filter == null) {
            return null;
        }

        int keyIndex = findKeyIndex(table.getTarget());
        switch (filter.getKind()) {
            // WHERE __key = true, calcite simplifies to just `WHERE __key`
            case INPUT_REF: {
                return ((RexInputRef) filter).getIndex() == keyIndex
                        ? rexBuilder.makeLiteral(true)
                        : null;
            }
            // WHERE __key = false, calcite simplifies to `WHERE NOT __key`
            case NOT: {
                RexNode operand = ((RexCall) filter).getOperands().get(0);
                return operand.getKind() == SqlKind.INPUT_REF && ((RexInputRef) operand).getIndex() == keyIndex
                        ? rexBuilder.makeLiteral(false)
                        : null;
            }
            // __key = ...
            case EQUALS: {
                Tuple2<Integer, RexNode> constantExpressionByIndex = extractConstantExpression((RexCall) filter);
                //noinspection ConstantConditions
                return constantExpressionByIndex != null && constantExpressionByIndex.getKey() == keyIndex
                        ? constantExpressionByIndex.getValue()
                        : null;
            }
            default:
                return null;
        }
    }

    private int findKeyIndex(Table table) {
        List<String> primaryKey = IMapSqlConnector.PRIMARY_KEY_LIST;
        assert primaryKey.size() == 1;

        int keyIndex = table.getFieldIndex(primaryKey.get(0));
        assert keyIndex > -1;

        return keyIndex;
    }

    private Tuple2<Integer, RexNode> extractConstantExpression(RexCall condition) {
        Tuple2<Integer, RexNode> constantExpression = extractConstantExpression(condition, 0);
        return constantExpression != null ? constantExpression : extractConstantExpression(condition, 1);
    }

    private Tuple2<Integer, RexNode> extractConstantExpression(RexCall condition, int i) {
        RexNode firstOperand = condition.getOperands().get(i);
        if (firstOperand.getKind() == SqlKind.INPUT_REF) {
            int index = ((RexInputRef) firstOperand).getIndex();
            RexNode secondOperand = condition.getOperands().get(1 - i);
            if (RexUtil.isConstant(secondOperand)) {
                return Tuple2.tuple2(index, secondOperand);
            }
        }
        return null;
    }
}
