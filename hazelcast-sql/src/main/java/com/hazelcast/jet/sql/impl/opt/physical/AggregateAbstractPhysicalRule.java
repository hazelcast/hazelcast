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

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.impl.execution.init.Contexts;
import com.hazelcast.jet.sql.impl.aggregate.AvgSqlAggregations;
import com.hazelcast.jet.sql.impl.aggregate.CountSqlAggregations;
import com.hazelcast.jet.sql.impl.aggregate.MaxSqlAggregation;
import com.hazelcast.jet.sql.impl.aggregate.MinSqlAggregation;
import com.hazelcast.jet.sql.impl.aggregate.SqlAggregation;
import com.hazelcast.jet.sql.impl.aggregate.SumSqlAggregations;
import com.hazelcast.jet.sql.impl.aggregate.ValueSqlAggregation;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

abstract class AggregateAbstractPhysicalRule extends RelRule<Config> {

    protected AggregateAbstractPhysicalRule(Config config) {
        super(config);
    }

    protected static AggregateOperation<?, JetSqlRow> aggregateOperation(
            RelDataType inputType,
            ImmutableBitSet groupSet,
            List<AggregateCall> aggregateCalls
    ) {
        List<QueryDataType> operandTypes = OptUtils.schema(inputType).getTypes();

        List<SupplierEx<SqlAggregation>> aggregationProviders = new ArrayList<>();
        List<FunctionEx<JetSqlRow, Object>> valueProviders = new ArrayList<>();

        for (Integer groupIndex : groupSet.toList()) {
            aggregationProviders.add(ValueSqlAggregation::new);
            // getMaybeSerialized is safe for ValueAggr because it only passes the value on
            valueProviders.add(row -> row.getMaybeSerialized(groupIndex));
        }
        for (AggregateCall aggregateCall : aggregateCalls) {
            boolean distinct = aggregateCall.isDistinct();
            List<Integer> aggregateCallArguments = aggregateCall.getArgList();
            SqlKind kind = aggregateCall.getAggregation().getKind();
            switch (kind) {
                case COUNT:
                    if (distinct) {
                        int countIndex = aggregateCallArguments.get(0);
                        aggregationProviders.add(() -> CountSqlAggregations.from(true, true));
                        // getMaybeSerialized is safe for COUNT because the aggregation only looks whether it is null or not
                        valueProviders.add(row -> row.getMaybeSerialized(countIndex));
                    } else if (aggregateCallArguments.size() == 1) {
                        int countIndex = aggregateCallArguments.get(0);
                        aggregationProviders.add(() -> CountSqlAggregations.from(true, false));
                        valueProviders.add(row -> row.getMaybeSerialized(countIndex));
                    } else {
                        aggregationProviders.add(() -> CountSqlAggregations.from(false, false));
                        valueProviders.add(row -> null);
                    }
                    break;
                case MIN:
                    int minIndex = aggregateCallArguments.get(0);
                    aggregationProviders.add(MinSqlAggregation::new);
                    valueProviders.add(row -> row.get(minIndex));
                    break;
                case MAX:
                    int maxIndex = aggregateCallArguments.get(0);
                    aggregationProviders.add(MaxSqlAggregation::new);
                    valueProviders.add(row -> row.get(maxIndex));
                    break;
                case SUM:
                    int sumIndex = aggregateCallArguments.get(0);
                    QueryDataType sumOperandType = operandTypes.get(sumIndex);
                    aggregationProviders.add(() -> SumSqlAggregations.from(sumOperandType, distinct));
                    valueProviders.add(row -> row.get(sumIndex));
                    break;
                case AVG:
                    int avgIndex = aggregateCallArguments.get(0);
                    QueryDataType avgOperandType = operandTypes.get(avgIndex);
                    aggregationProviders.add(() -> AvgSqlAggregations.from(avgOperandType, distinct));
                    valueProviders.add(row -> row.get(avgIndex));
                    break;
                default:
                    throw QueryException.error("Unsupported aggregation function: " + kind);
            }
        }

        return AggregateOperation
                .withCreate(() -> {
                    List<SqlAggregation> aggregations = new ArrayList<>(aggregationProviders.size());
                    for (SupplierEx<SqlAggregation> aggregationProvider : aggregationProviders) {
                        aggregations.add(aggregationProvider.get());
                    }
                    return aggregations;
                })
                .andAccumulate((List<SqlAggregation> aggregations, JetSqlRow row) -> {
                    for (int i = 0; i < aggregations.size(); i++) {
                        aggregations.get(i).accumulate(valueProviders.get(i).apply(row));
                    }
                })
                .andCombine((lefts, rights) -> {
                    assert lefts.size() == rights.size();

                    for (int i = 0; i < lefts.size(); i++) {
                        lefts.get(i).combine(rights.get(i));
                    }
                })
                .andExportFinish(aggregations -> {
                    Object[] row = new Object[aggregations.size()];
                    for (int i = 0; i < aggregations.size(); i++) {
                        row[i] = aggregations.get(i).collect();
                    }
                    return new JetSqlRow(Contexts.getCastedThreadContext().serializationService(), row);
                });
    }
}
