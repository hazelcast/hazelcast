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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.jet.sql.impl.connector.map.RowProjectorProcessorSupplier;
import com.hazelcast.jet.sql.impl.expression.json.JsonArrayFunction;
import com.hazelcast.jet.sql.impl.expression.json.JsonObjectFunction;
import com.hazelcast.jet.sql.impl.expression.json.JsonParseFunction;
import com.hazelcast.jet.sql.impl.expression.json.JsonQueryFunction;
import com.hazelcast.jet.sql.impl.expression.json.JsonValueFunction;
import com.hazelcast.jet.sql.impl.opt.FieldCollation;
import com.hazelcast.jet.sql.impl.opt.physical.AggregateAbstractPhysicalRule;
import com.hazelcast.jet.sql.impl.processors.RootResultConsumerSink;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.exec.scan.MapIndexScanMetadata;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.JET_SQL_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.JET_SQL_DS_FACTORY_ID;

/**
 * Serialization hook for Jet SQL engine classes.
 */
public class JetSqlSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(JET_SQL_DS_FACTORY, JET_SQL_DS_FACTORY_ID);

    public static final int JSON_QUERY = 0;
    public static final int JSON_PARSE = 1;
    public static final int JSON_VALUE = 2;
    public static final int JSON_OBJECT = 3;
    public static final int JSON_ARRAY = 4;
    public static final int MAP_INDEX_SCAN_METADATA = 5;
    public static final int ROW_PROJECTOR_PROCESSOR_SUPPLIER = 6;
    public static final int KV_ROW_PROJECTOR_SUPPLIER = 7;
    public static final int ROOT_RESULT_CONSUMER_SINK_SUPPLIER = 8;
    public static final int SQL_ROW_COMPARATOR = 9;
    public static final int FIELD_COLLATION = 10;
    public static final int ROW_GET_MAYBE_SERIALIZED_FN = 11;
    public static final int NULL_FUNCTION = 12;
    public static final int ROW_GET_FN = 13;
    public static final int AGGREGATE_CREATE_SUPPLIER = 14;
    public static final int AGGREGATE_ACCUMULATE_FUNCTION = 15;
    public static final int AGGREGATE_COMBINE_FUNCTION = 16;
    public static final int AGGREGATE_EXPORT_FINISH_FUNCTION = 17;
    public static final int AGGREGATE_SUM_SUPPLIER = 18;
    public static final int AGGREGATE_AVG_SUPPLIER = 19;
    public static final int AGGREGATE_COUNT_SUPPLIER = 20;

    public static final int LEN = AGGREGATE_COUNT_SUPPLIER + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[JSON_QUERY] = arg -> new JsonQueryFunction();
        constructors[JSON_PARSE] = arg -> new JsonParseFunction();
        constructors[JSON_VALUE] = arg -> new JsonValueFunction<>();
        constructors[JSON_OBJECT] = arg -> new JsonObjectFunction();
        constructors[JSON_ARRAY] = arg -> new JsonArrayFunction();
        constructors[MAP_INDEX_SCAN_METADATA] = arg -> new MapIndexScanMetadata();
        constructors[ROW_PROJECTOR_PROCESSOR_SUPPLIER] = arg -> new RowProjectorProcessorSupplier();
        constructors[KV_ROW_PROJECTOR_SUPPLIER] = arg -> new KvRowProjector.Supplier();
        constructors[ROOT_RESULT_CONSUMER_SINK_SUPPLIER] = arg -> new RootResultConsumerSink.Supplier();
        constructors[SQL_ROW_COMPARATOR] = arg -> new ExpressionUtil.SqlRowComparator();
        constructors[FIELD_COLLATION] = arg -> new FieldCollation();
        constructors[ROW_GET_MAYBE_SERIALIZED_FN] = arg -> new AggregateAbstractPhysicalRule.RowGetMaybeSerializedFn();
        constructors[NULL_FUNCTION] = arg -> AggregateAbstractPhysicalRule.NullFunction.INSTANCE;
        constructors[ROW_GET_FN] = arg -> new AggregateAbstractPhysicalRule.RowGetFn();
        constructors[AGGREGATE_CREATE_SUPPLIER] = arg -> new AggregateAbstractPhysicalRule.AggregateCreateSupplier();
        constructors[AGGREGATE_ACCUMULATE_FUNCTION] =
                arg -> new AggregateAbstractPhysicalRule.AggregateAccumulateFunction();
        constructors[AGGREGATE_COMBINE_FUNCTION] =
                arg -> AggregateAbstractPhysicalRule.AggregateCombineFunction.INSTANCE;
        constructors[AGGREGATE_EXPORT_FINISH_FUNCTION] =
                arg -> AggregateAbstractPhysicalRule.AggregateExportFinishFunction.INSTANCE;
        constructors[AGGREGATE_SUM_SUPPLIER] = arg -> new AggregateAbstractPhysicalRule.AggregateSumSupplier();
        constructors[AGGREGATE_AVG_SUPPLIER] = arg -> new AggregateAbstractPhysicalRule.AggregateAvgSupplier();
        constructors[AGGREGATE_COUNT_SUPPLIER] = arg -> new AggregateAbstractPhysicalRule.AggregateCountSupplier();

        return new ArrayDataSerializableFactory(constructors);
    }
}
