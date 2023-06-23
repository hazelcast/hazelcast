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

package com.hazelcast.jet.sql.impl.opt.prunability;

import com.hazelcast.jet.datamodel.Tuple4;
import com.hazelcast.sql.impl.schema.Table;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

public class PartitionStrategyConditionExtractor {

    /**
     * Returns tuple of table name, column name, left and right
     * operands of comparison extracted from analysed condition.
     */
    public List<Tuple4<String, String, RexInputRef, RexNode>> extractCondition(
            Table table,
            RexCall call,
            Set<Integer> partitioningColumns
    ) {
        final var conditions = extractSubCondition(table, call, partitioningColumns);
        final Set<Integer> affectedColumns = conditions.stream()
                .map(Tuple4::f2)
                .filter(Objects::nonNull)
                .map(RexInputRef::getIndex)
                .collect(Collectors.toSet());
//        TODO[Ivan] : enable strategy in RelPrunabilityTest
//        if (!affectedColumns.equals(partitioningColumns)) {
//            return emptyList();
//        }

        return conditions;
    }

    public List<Tuple4<String, String, RexInputRef, RexNode>> extractSubCondition(
            Table table,
            RexCall call,
            Set<Integer> partitioningColumns
    ) {
        List<Tuple4<String, String, RexInputRef, RexNode>> result = new ArrayList<>();
        // $1 = 1
        switch (call.getKind()) {
            // TODO: redesign into range-analysis based approach
            case AND:
                for (final RexNode operand : call.getOperands()) {
                    if (!(operand instanceof RexCall)) {
                        return emptyList();
                    }
                    result.addAll(extractSubCondition(table, (RexCall) operand, partitioningColumns));
                }

                break;
            case EQUALS:
                assert call.getOperands().size() == 2;
                final RexInputRef inputRef = extractInputRef(call);
                final RexNode constantExpr = extractConstantExpression(call);
                if (inputRef == null || constantExpr == null) {
                    break;
                }
//                if (!partitioningColumns.contains(inputRef.getIndex())) {
//                    break;
//                }
                String tableName = table.getSqlName();
                String columnName = table.getField(inputRef.getIndex()).getName();
                result.add(Tuple4.tuple4(tableName, columnName, inputRef, constantExpr));
                break;
            default:
                return result;

        }
        return result;
    }

    private RexInputRef extractInputRef(final RexCall call) {
        // only works for EQUALS
        assert call.isA(SqlKind.EQUALS);
        if (call.getOperands().get(0) instanceof RexInputRef) {
            return (RexInputRef) call.getOperands().get(0);
        }

        if (call.getOperands().get(1) instanceof RexInputRef) {
            return (RexInputRef) call.getOperands().get(1);
        }

        return null;
    }

    private RexNode extractConstantExpression(final RexCall call) {
        assert call.isA(SqlKind.EQUALS);
        if (call.getOperands().get(0) instanceof RexDynamicParam) {
            return call.getOperands().get(0);
        }

        if (call.getOperands().get(1) instanceof RexDynamicParam) {
            return call.getOperands().get(1);
        }

        if (call.getOperands().get(0) instanceof RexLiteral) {
            return call.getOperands().get(0);
        }

        if (call.getOperands().get(1) instanceof RexLiteral) {
            return call.getOperands().get(1);
        }

        return null;
    }
}
