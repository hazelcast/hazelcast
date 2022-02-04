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

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.impl.execution.init.Contexts;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.jet.sql.impl.aggregate.AvgSqlAggregations;
import com.hazelcast.jet.sql.impl.aggregate.CountSqlAggregations;
import com.hazelcast.jet.sql.impl.aggregate.MaxSqlAggregation;
import com.hazelcast.jet.sql.impl.aggregate.MinSqlAggregation;
import com.hazelcast.jet.sql.impl.aggregate.SqlAggregation;
import com.hazelcast.jet.sql.impl.aggregate.SumSqlAggregations;
import com.hazelcast.jet.sql.impl.aggregate.ValueSqlAggregation;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AggregateAbstractPhysicalRule extends RelRule<Config> {

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
            valueProviders.add(new MaybeSerializedFunction(groupIndex));
        }
        for (AggregateCall aggregateCall : aggregateCalls) {
            boolean distinct = aggregateCall.isDistinct();
            List<Integer> aggregateCallArguments = aggregateCall.getArgList();
            SqlKind kind = aggregateCall.getAggregation().getKind();
            switch (kind) {
                case COUNT:
                    if (distinct) {
                        int countIndex = aggregateCallArguments.get(0);
                        aggregationProviders.add(AggregateCountTTSupplier.INSTANCE);
                        // getMaybeSerialized is safe for COUNT because the aggregation only looks whether it is null or not
                        valueProviders.add(new MaybeSerializedFunction(countIndex));
                    } else if (aggregateCallArguments.size() == 1) {
                        int countIndex = aggregateCallArguments.get(0);
                        aggregationProviders.add(AggregateCountTFSupplier.INSTANCE);
                        valueProviders.add(new MaybeSerializedFunction(countIndex));
                    } else {
                        aggregationProviders.add(AggregateCountFFSupplier.INSTANCE);
                        valueProviders.add(NullFunction.INSTANCE);
                    }
                    break;
                case MIN:
                    int minIndex = aggregateCallArguments.get(0);
                    aggregationProviders.add(MinSqlAggregation::new);
                    valueProviders.add(new GetFunction(minIndex));
                    break;
                case MAX:
                    int maxIndex = aggregateCallArguments.get(0);
                    aggregationProviders.add(MaxSqlAggregation::new);
                    valueProviders.add(new GetFunction(maxIndex));
                    break;
                case SUM:
                    int sumIndex = aggregateCallArguments.get(0);
                    QueryDataType sumOperandType = operandTypes.get(sumIndex);
                    aggregationProviders.add(new AggregateSumSupplier(distinct, sumOperandType));
                    valueProviders.add(new GetFunction(sumIndex));
                    break;
                case AVG:
                    int avgIndex = aggregateCallArguments.get(0);
                    QueryDataType avgOperandType = operandTypes.get(avgIndex);
                    aggregationProviders.add(new AggregateAvgSupplier(distinct, avgOperandType));
                    valueProviders.add(new GetFunction(avgIndex));
                    break;
                default:
                    throw QueryException.error("Unsupported aggregation function: " + kind);
            }
        }

        return AggregateOperation
                .withCreate(new AggregateCreateSupplier(aggregationProviders))
                .andAccumulate(new AggregateAccumulateFunction(valueProviders))
                .andCombine(AggregateCombineFunction.INSTANCE)
                .andExportFinish(AggregateExportFinishFunction.INSTANCE);
    }

    public static class AggregateAvgSupplier implements IdentifiedDataSerializable,
            SupplierEx<SqlAggregation> {
        private boolean distinct;
        private QueryDataType avgOperandType;

        public AggregateAvgSupplier() {
        }

        public AggregateAvgSupplier(boolean distinct, QueryDataType avgOperandType) {
            this.distinct = distinct;
            this.avgOperandType = avgOperandType;
        }

        @Override
        public SqlAggregation getEx() throws Exception {
            return AvgSqlAggregations.from(avgOperandType, distinct);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeBoolean(distinct);
            out.writeObject(avgOperandType);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            distinct = in.readBoolean();
            avgOperandType = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return JetSqlSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return JetSqlSerializerHook.AGGREGATE_AVG_SUPPLIER;
        }
    }

    public static class AggregateSumSupplier implements IdentifiedDataSerializable,
            SupplierEx<SqlAggregation> {
        private boolean distinct;
        private QueryDataType sumOperandType;

        public AggregateSumSupplier() {
        }

        public AggregateSumSupplier(boolean distinct, QueryDataType sumOperandType) {
            this.distinct = distinct;
            this.sumOperandType = sumOperandType;
        }

        @Override
        public SqlAggregation getEx() throws Exception {
            return SumSqlAggregations.from(sumOperandType, distinct);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeBoolean(distinct);
            out.writeObject(sumOperandType);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
           distinct = in.readBoolean();
           sumOperandType = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return JetSqlSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return JetSqlSerializerHook.AGGREGATE_SUM_SUPPLIER;
        }
    }

    public static final class AggregateCountTFSupplier implements IdentifiedDataSerializable,
            SupplierEx<SqlAggregation> {
        public static final AggregateCountTFSupplier INSTANCE = new AggregateCountTFSupplier();

        private AggregateCountTFSupplier() {
        }

        @Override
        public SqlAggregation getEx() throws Exception {
            return CountSqlAggregations.from(true, false);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }

        @Override
        public int getFactoryId() {
            return JetSqlSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return JetSqlSerializerHook.AGGREGATE_COUNT_TF_SUPPLIER;
        }
    }

    public static final class AggregateCountFFSupplier implements IdentifiedDataSerializable,
            SupplierEx<SqlAggregation> {
        public static final AggregateCountFFSupplier INSTANCE = new AggregateCountFFSupplier();

        private AggregateCountFFSupplier() {
        }

        @Override
        public SqlAggregation getEx() throws Exception {
            return CountSqlAggregations.from(false, false);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }

        @Override
        public int getFactoryId() {
            return JetSqlSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return JetSqlSerializerHook.AGGREGATE_COUNT_FF_SUPPLIER;
        }
    }

    public static final class AggregateCountTTSupplier implements IdentifiedDataSerializable,
            SupplierEx<SqlAggregation> {
        public static final AggregateCountTTSupplier INSTANCE = new AggregateCountTTSupplier();

        private AggregateCountTTSupplier() {
        }

        @Override
        public SqlAggregation getEx() throws Exception {
            return CountSqlAggregations.from(true, true);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }

        @Override
        public int getFactoryId() {
            return JetSqlSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return JetSqlSerializerHook.AGGREGATE_COUNT_TT_SUPPLIER;
        }
    }

    public static final class AggregateExportFinishFunction implements IdentifiedDataSerializable,
            FunctionEx<List<SqlAggregation>, JetSqlRow> {
        public static final AggregateExportFinishFunction INSTANCE = new AggregateExportFinishFunction();

        private AggregateExportFinishFunction() {
        }

        @Override
        public JetSqlRow applyEx(List<SqlAggregation> aggregations) throws Exception {
            Object[] row = new Object[aggregations.size()];
            for (int i = 0; i < aggregations.size(); i++) {
                row[i] = aggregations.get(i).collect();
            }
            return new JetSqlRow(Contexts.getCastedThreadContext().serializationService(), row);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }

        @Override
        public int getFactoryId() {
            return JetSqlSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return JetSqlSerializerHook.AGGREGATE_EXPORT_FINISH_FUNCTION;
        }
    }

    public static final class AggregateCombineFunction implements IdentifiedDataSerializable,
            BiConsumerEx<List<SqlAggregation>, List<SqlAggregation>> {
        public static final AggregateCombineFunction INSTANCE = new AggregateCombineFunction();

        private AggregateCombineFunction() {
        }

        @Override
        public void acceptEx(List<SqlAggregation> lefts, List<SqlAggregation> rights) throws Exception {
            assert lefts.size() == rights.size();

            for (int i = 0; i < lefts.size(); i++) {
                lefts.get(i).combine(rights.get(i));
            }
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }

        @Override
        public int getFactoryId() {
            return JetSqlSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return JetSqlSerializerHook.AGGREGATE_COMBINE_FUNCTION;
        }
    }

    public static class AggregateAccumulateFunction implements IdentifiedDataSerializable,
            BiConsumerEx<List<SqlAggregation>, JetSqlRow> {
        private List<FunctionEx<JetSqlRow, Object>> valueProviders;

        public AggregateAccumulateFunction() {
        }

        public AggregateAccumulateFunction(List<FunctionEx<JetSqlRow, Object>> valueProviders) {
            this.valueProviders = valueProviders;
        }

        @Override
        public void acceptEx(List<SqlAggregation> aggregations, JetSqlRow row) throws Exception {
            for (int i = 0; i < aggregations.size(); i++) {
                aggregations.get(i).accumulate(valueProviders.get(i).apply(row));
            }
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(valueProviders.size());
            for (FunctionEx<JetSqlRow, Object> aggregationProvider : valueProviders) {
                out.writeObject(aggregationProvider);
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            int aggregationProvidersSize = in.readInt();
            valueProviders = new ArrayList<>(aggregationProvidersSize);
            for (int i = 0; i < aggregationProvidersSize; i++) {
                valueProviders.add(in.readObject());
            }
        }

        @Override
        public int getFactoryId() {
            return JetSqlSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return JetSqlSerializerHook.AGGREGATE_ACCUMULATE_FUNCTION;
        }
    }

    public static class AggregateCreateSupplier implements IdentifiedDataSerializable, SupplierEx<List<SqlAggregation>> {
        private List<SupplierEx<SqlAggregation>> aggregationProviders;

        public AggregateCreateSupplier() {
        }

        public AggregateCreateSupplier(List<SupplierEx<SqlAggregation>> aggregationProviders) {
            this.aggregationProviders = aggregationProviders;
        }

        @Override
        public List<SqlAggregation> getEx() throws Exception {
            List<SqlAggregation> aggregations = new ArrayList<>(aggregationProviders.size());
            for (SupplierEx<SqlAggregation> aggregationProvider : aggregationProviders) {
                aggregations.add(aggregationProvider.get());
            }
            return aggregations;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(aggregationProviders.size());
            for (SupplierEx<SqlAggregation> aggregationProvider : aggregationProviders) {
                out.writeObject(aggregationProvider);
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            int aggregationProvidersSize = in.readInt();
            aggregationProviders = new ArrayList<>(aggregationProvidersSize);
            for (int i = 0; i < aggregationProvidersSize; i++) {
                aggregationProviders.add(in.readObject());
            }
        }

        @Override
        public int getFactoryId() {
            return JetSqlSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return JetSqlSerializerHook.AGGREGATE_CREATE_SUPPLIER;
        }
    }

    public static class MaybeSerializedFunction implements IdentifiedDataSerializable, FunctionEx<JetSqlRow, Object> {
        private Integer groupIndex;

        public MaybeSerializedFunction() {
        }

        public MaybeSerializedFunction(Integer groupIndex) {
            this.groupIndex = groupIndex;
        }

        @Override
        public Object applyEx(JetSqlRow row) throws Exception {
            return row.getMaybeSerialized(groupIndex);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(groupIndex);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            groupIndex = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return JetSqlSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return JetSqlSerializerHook.MAYBE_SERIALIZED_FUNCTION;
        }
    }

    public static class GetFunction implements IdentifiedDataSerializable, FunctionEx<JetSqlRow, Object> {
        private int index;

        public GetFunction() {
        }

        public GetFunction(Integer index) {
            this.index = index;
        }

        @Override
        public Object applyEx(JetSqlRow row) throws Exception {
            return row.get(index);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(index);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            index = in.readInt();
        }

        @Override
        public int getFactoryId() {
            return JetSqlSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return JetSqlSerializerHook.GET_FUNCTION;
        }
    }

    public static final class NullFunction implements IdentifiedDataSerializable, FunctionEx<JetSqlRow, Object> {
        public static final NullFunction INSTANCE = new NullFunction();

        private NullFunction() {
        }

        @Override
        public Object applyEx(JetSqlRow row) throws Exception {
            return null;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }

        @Override
        public int getFactoryId() {
            return JetSqlSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return JetSqlSerializerHook.NULL_FUNCTION;
        }
    }
}
