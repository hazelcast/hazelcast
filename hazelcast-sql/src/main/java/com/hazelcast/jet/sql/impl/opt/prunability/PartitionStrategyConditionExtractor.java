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

import com.google.common.collect.Sets;
import com.hazelcast.sql.impl.schema.Table;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/**
 * Extracts RexDynamicParam/RexLiterals that correspond to key components in the filter.
 * Only supports simple top level AND operators, as well as single operator filters e.g.
 * - keyComp1 = ? AND keyComp2 = ?
 * - __key = ?
 * Since at this point of Opt all filters are normalized and coalesced into multi-operand AND/OR operators
 * we can safely assume that something like AND(b=1, AND(a=1,c=1)) will become AND(b=1,a=1,c=1) at this point.
 * Note that this class will likely change significantly with introduction of support for more complex filters.
 *
 */
public class PartitionStrategyConditionExtractor {

    /**
     * Returns a map of per-table candidates, structured as mapName -> [columnName -> RexLiteralOrDynamicParam]
     * where every innermost map represents a single candidate - a conjunction (AND) of EQUALS-based conditions.
     * Each candidate is guaranteed to "produce" a single Partition Key, meaning if it was used as a standalone filter
     * in a query, it would limit the query to only a single Partition Key.
     *
     * @return map of per-table partition pruning candidates or empty list if any of the candidates are incomplete.
     */
    public Map<String, List<Map<String, RexNode>>> extractCondition(
            Table table,
            RexCall call,
            Set<String> partitioningColumns
    ) {
        if (partitioningColumns.isEmpty()) {
            return emptyMap();
        }
        final var variants = extractSubCondition(table, call, partitioningColumns);
        if (variants.isEmpty()) {
            return emptyMap();
        }

        for (final Map<String, RexNode> variant : variants) {
            // if any of the variants miss any of the columns, then entire filter is unbounded
            if (!Sets.intersection(variant.keySet(), partitioningColumns).equals(partitioningColumns)) {
                return emptyMap();
            }
        }

        // TODO: possibly include catalog and schema name?
        return Map.of(table.getSqlName(), variants);
    }

    public List<Map<String, RexNode>> extractSubCondition(
            Table table,
            RexCall call,
            Set<String> partitioningColumns
    ) {
        final List<Map<String, RexNode>> result = new ArrayList<>();
        switch (call.getKind()) {
            case AND:
                final Map<String, RexNode> variant = new HashMap<>();
                for (final RexNode operand : call.getOperands()) {
                    if (!(operand instanceof RexCall)) {
                        return emptyList();
                    }
                    var condition = extractEqualityCondition(table, (RexCall) operand, partitioningColumns);
                    if (condition != null) {
                        variant.put(condition.getKey(), condition.getValue());
                    }
                }
                result.add(variant);
                break;
            case EQUALS:
                var entry = extractEqualityCondition(table, call, partitioningColumns);
                if (entry != null) {
                    result.add(Map.ofEntries(entry));
                }
                break;
            default:
                break;
        }
        return result;
    }

    private Map.Entry<String, RexNode> extractEqualityCondition(
            Table table,
            RexCall call,
            Set<String> partitioningColumns
    ) {
        if (!call.isA(SqlKind.EQUALS)) {
            return null;
        }
        assert call.getOperands().size() == 2;
        final RexInputRef inputRef = extractInputRef(call);
        final RexNode constantExpr = extractConstantExpression(call);
        if (inputRef == null || constantExpr == null) {
            return null;
        }

        final String columnName = table.getField(inputRef.getIndex()).getName();
        if (!partitioningColumns.contains(columnName)) {
            return null;
        }
        return new AbstractMap.SimpleEntry<>(columnName, constantExpr);
    }

    private RexInputRef extractInputRef(final RexCall call) {
        // only works for EQUALS
        assert call.isA(SqlKind.EQUALS);
        return (RexInputRef) call.getOperands().stream()
                .filter(operand -> operand instanceof RexInputRef).findFirst().orElse(null);
    }

    private RexNode extractConstantExpression(final RexCall call) {
        assert call.isA(SqlKind.EQUALS);
        return call.getOperands().stream()
                .filter(operand -> operand instanceof RexDynamicParam || operand instanceof RexLiteral)
                .findFirst().orElse(null);
    }
}
