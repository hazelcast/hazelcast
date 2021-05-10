/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.opt.physical;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.sql.impl.aggregate.AvgSqlAggregations;
import com.hazelcast.jet.sql.impl.aggregate.CountSqlAggregations;
import com.hazelcast.jet.sql.impl.aggregate.MaxSqlAggregation;
import com.hazelcast.jet.sql.impl.aggregate.MinSqlAggregation;
import com.hazelcast.jet.sql.impl.aggregate.SqlAggregation;
import com.hazelcast.jet.sql.impl.aggregate.SumSqlAggregations;
import com.hazelcast.jet.sql.impl.aggregate.ValueSqlAggregation;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.AggregateLogicalRel;
import com.hazelcast.jet.sql.impl.processors.JetSqlRow;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.sql.impl.opt.JetConventions.LOGICAL;

final class AggregatePhysicalRule extends RelOptRule {

    static final RelOptRule INSTANCE = new AggregatePhysicalRule();

    private AggregatePhysicalRule() {
        super(
                operand(AggregateLogicalRel.class, LOGICAL, some(operand(RelNode.class, any()))),
                AggregatePhysicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        AggregateLogicalRel logicalAggregate = call.rel(0);
        RelNode input = logicalAggregate.getInput();

        assert logicalAggregate.getGroupType() == Group.SIMPLE;

        RelNode convertedInput = OptUtils.toPhysicalInput(input);
        Collection<RelNode> transformedInputs = OptUtils.extractPhysicalRelsFromSubset(convertedInput);
        for (RelNode transformedInput : transformedInputs) {
            call.transformTo(optimize(logicalAggregate, transformedInput));
        }
    }

    private static RelNode optimize(AggregateLogicalRel logicalAggregate, RelNode physicalInput) {
        return logicalAggregate.getGroupSet().cardinality() == 0
                ? toAggregate(logicalAggregate, physicalInput)
                : toAggregateByKey(logicalAggregate, physicalInput);
    }

    private static RelNode toAggregate(AggregateLogicalRel logicalAggregate, RelNode physicalInput) {
        AggregateOperation<?, JetSqlRow> aggrOp = aggregateOperation(
                physicalInput.getRowType(),
                logicalAggregate.getGroupSet(),
                logicalAggregate.getAggCallList()
        );

        if (logicalAggregate.containsDistinctCall()) {
            return new AggregatePhysicalRel(
                    physicalInput.getCluster(),
                    physicalInput.getTraitSet(),
                    physicalInput,
                    logicalAggregate.getGroupSet(),
                    logicalAggregate.getGroupSets(),
                    logicalAggregate.getAggCallList(),
                    aggrOp
            );
        } else {
            RelNode rel = new AggregateAccumulatePhysicalRel(
                    physicalInput.getCluster(),
                    physicalInput.getTraitSet(),
                    physicalInput,
                    aggrOp
            );

            return new AggregateCombinePhysicalRel(
                    rel.getCluster(),
                    rel.getTraitSet(),
                    rel,
                    logicalAggregate.getGroupSet(),
                    logicalAggregate.getGroupSets(),
                    logicalAggregate.getAggCallList(),
                    aggrOp
            );
        }
    }

    private static RelNode toAggregateByKey(AggregateLogicalRel logicalAggregate, RelNode physicalInput) {
        AggregateOperation<?, JetSqlRow> aggrOp = aggregateOperation(
                physicalInput.getRowType(),
                logicalAggregate.getGroupSet(),
                logicalAggregate.getAggCallList()
        );

        if (logicalAggregate.containsDistinctCall()) {
            return new AggregateByKeyPhysicalRel(
                    physicalInput.getCluster(),
                    physicalInput.getTraitSet(),
                    physicalInput,
                    logicalAggregate.getGroupSet(),
                    logicalAggregate.getGroupSets(),
                    logicalAggregate.getAggCallList(),
                    aggrOp
            );
        } else {
            RelNode rel = new AggregateAccumulateByKeyPhysicalRel(
                    physicalInput.getCluster(),
                    physicalInput.getTraitSet(),
                    physicalInput,
                    logicalAggregate.getGroupSet(),
                    aggrOp
            );

            return new AggregateCombineByKeyPhysicalRel(
                    rel.getCluster(),
                    rel.getTraitSet(),
                    rel,
                    logicalAggregate.getGroupSet(),
                    logicalAggregate.getGroupSets(),
                    logicalAggregate.getAggCallList(),
                    aggrOp
            );
        }
    }

    private static AggregateOperation<?, JetSqlRow> aggregateOperation(
            RelDataType inputType,
            ImmutableBitSet groupSet,
            List<AggregateCall> aggregateCalls
    ) {
        List<QueryDataType> operandTypes = OptUtils.schema(inputType).getTypes();

        List<SupplierEx<SqlAggregation>> aggregationProviders = new ArrayList<>();
        List<FunctionEx<JetSqlRow, Object>> valueProviders = new ArrayList<>();

        for (Integer groupIndex : groupSet.toList()) {
            aggregationProviders.add(ValueSqlAggregation::new);
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
                    valueProviders.add(row -> row.getMaybeSerialized(minIndex));
                    break;
                case MAX:
                    int maxIndex = aggregateCallArguments.get(0);
                    aggregationProviders.add(MaxSqlAggregation::new);
                    valueProviders.add(row -> row.getMaybeSerialized(maxIndex));
                    break;
                case SUM:
                    int sumIndex = aggregateCallArguments.get(0);
                    QueryDataType sumOperandType = operandTypes.get(sumIndex);
                    aggregationProviders.add(() -> SumSqlAggregations.from(sumOperandType, distinct));
                    valueProviders.add(row -> row.getMaybeSerialized(sumIndex));
                    break;
                case AVG:
                    int avgIndex = aggregateCallArguments.get(0);
                    QueryDataType avgOperandType = operandTypes.get(avgIndex);
                    aggregationProviders.add(() -> AvgSqlAggregations.from(avgOperandType, distinct));
                    valueProviders.add(row -> row.getMaybeSerialized(avgIndex));
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
                    JetSqlRow row = new JetSqlRow(aggregations.size());
                    for (int i = 0; i < aggregations.size(); i++) {
                        row.set(i, aggregations.get(i).collect());
                    }
                    return row;
                });
    }
}
