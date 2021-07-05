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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.Objects;

public final class DeleteByKeyMapLogicalRule extends RelOptRule {

    static final RelOptRule INSTANCE = new DeleteByKeyMapLogicalRule();

    private DeleteByKeyMapLogicalRule() {
        super(
                operandJ(
                        LogicalTableModify.class, null, TableModify::isDelete,
                        operandJ(LogicalTableScan.class, null, OptUtils::overPartitionedMapTable, none())
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
            // __key = true
            case INPUT_REF: {
                return ((RexInputRef) filter).getIndex() == keyIndex
                        ? rexBuilder.makeLiteral(true)
                        : null;
            }
            // __key = false
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
            if (secondOperand.accept(ConstantExpressionVisitor.INSTANCE)) {
                return Tuple2.tuple2(index, secondOperand);
            }
        }
        return null;
    }

    private static final class ConstantExpressionVisitor implements RexVisitor<Boolean> {

        private static final ConstantExpressionVisitor INSTANCE = new ConstantExpressionVisitor();

        @Override
        public Boolean visitInputRef(RexInputRef inputRef) {
            return false;
        }

        @Override
        public Boolean visitLocalRef(RexLocalRef localRef) {
            return false;
        }

        @Override
        public Boolean visitLiteral(RexLiteral literal) {
            return true;
        }

        @Override
        public Boolean visitCall(RexCall call) {
            return call.getOperands().stream()
                    .filter(Objects::nonNull)
                    .allMatch(operand -> operand.accept(this));
        }

        @Override
        public Boolean visitOver(RexOver over) {
            return false;
        }

        @Override
        public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
            return false;
        }

        @Override
        public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
            return true;
        }

        @Override
        public Boolean visitRangeRef(RexRangeRef rangeRef) {
            return false;
        }

        @Override
        public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
            return false;
        }

        @Override
        public Boolean visitSubQuery(RexSubQuery subQuery) {
            return false;
        }

        @Override
        public Boolean visitTableInputRef(RexTableInputRef fieldRef) {
            return false;
        }

        @Override
        public Boolean visitPatternFieldRef(RexPatternFieldRef fieldRef) {
            return false;
        }
    }
}
