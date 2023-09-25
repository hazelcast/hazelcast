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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.jet.sql.impl.connector.map.LazyDefiningSpecificMemberPms;
import com.hazelcast.jet.sql.impl.connector.map.RowProjectorProcessorSupplier;
import com.hazelcast.jet.sql.impl.expression.UdtObjectToJsonFunction;
import com.hazelcast.jet.sql.impl.expression.json.JsonArrayFunction;
import com.hazelcast.jet.sql.impl.expression.json.JsonObjectFunction;
import com.hazelcast.jet.sql.impl.expression.json.JsonParseFunction;
import com.hazelcast.jet.sql.impl.expression.json.JsonQueryFunction;
import com.hazelcast.jet.sql.impl.expression.json.JsonValueFunction;
import com.hazelcast.jet.sql.impl.opt.FieldCollation;
import com.hazelcast.jet.sql.impl.opt.physical.AggregateAbstractPhysicalRule;
import com.hazelcast.jet.sql.impl.processors.RootResultConsumerSink;
import com.hazelcast.jet.sql.impl.validate.UpdateDataConnectionOperation;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.LazyTarget;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.exec.scan.MapIndexScanMetadata;
import com.hazelcast.sql.impl.exec.scan.index.IndexCompositeFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilterValue;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.expression.CaseExpression;
import com.hazelcast.sql.impl.expression.CastExpression;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.FieldAccessExpression;
import com.hazelcast.sql.impl.expression.ParameterExpression;
import com.hazelcast.sql.impl.expression.RowExpression;
import com.hazelcast.sql.impl.expression.SargExpression;
import com.hazelcast.sql.impl.expression.datetime.ExtractFunction;
import com.hazelcast.sql.impl.expression.datetime.ToCharFunction;
import com.hazelcast.sql.impl.expression.datetime.ToEpochMillisFunction;
import com.hazelcast.sql.impl.expression.datetime.ToTimestampTzFunction;
import com.hazelcast.sql.impl.expression.math.AbsFunction;
import com.hazelcast.sql.impl.expression.math.DivideFunction;
import com.hazelcast.sql.impl.expression.math.DoubleBiFunction;
import com.hazelcast.sql.impl.expression.math.DoubleFunction;
import com.hazelcast.sql.impl.expression.math.FloorCeilFunction;
import com.hazelcast.sql.impl.expression.math.MinusFunction;
import com.hazelcast.sql.impl.expression.math.MultiplyFunction;
import com.hazelcast.sql.impl.expression.math.PlusFunction;
import com.hazelcast.sql.impl.expression.math.RandFunction;
import com.hazelcast.sql.impl.expression.math.RemainderFunction;
import com.hazelcast.sql.impl.expression.math.RoundTruncateFunction;
import com.hazelcast.sql.impl.expression.math.SignFunction;
import com.hazelcast.sql.impl.expression.math.UnaryMinusFunction;
import com.hazelcast.sql.impl.expression.predicate.AndPredicate;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsFalsePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotFalsePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotNullPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotTruePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNullPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsTruePredicate;
import com.hazelcast.sql.impl.expression.predicate.NotPredicate;
import com.hazelcast.sql.impl.expression.predicate.OrPredicate;
import com.hazelcast.sql.impl.expression.predicate.SearchPredicate;
import com.hazelcast.sql.impl.expression.service.GetDdlFunction;
import com.hazelcast.sql.impl.expression.string.AsciiFunction;
import com.hazelcast.sql.impl.expression.string.CharLengthFunction;
import com.hazelcast.sql.impl.expression.string.ConcatFunction;
import com.hazelcast.sql.impl.expression.string.ConcatWSFunction;
import com.hazelcast.sql.impl.expression.string.InitcapFunction;
import com.hazelcast.sql.impl.expression.string.LikeFunction;
import com.hazelcast.sql.impl.expression.string.LowerFunction;
import com.hazelcast.sql.impl.expression.string.PositionFunction;
import com.hazelcast.sql.impl.expression.string.ReplaceFunction;
import com.hazelcast.sql.impl.expression.string.SubstringFunction;
import com.hazelcast.sql.impl.expression.string.TrimFunction;
import com.hazelcast.sql.impl.expression.string.UpperFunction;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.row.EmptyRow;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.dataconnection.DataConnectionCatalogEntry;
import com.hazelcast.sql.impl.schema.type.Type;
import com.hazelcast.sql.impl.schema.view.View;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import com.hazelcast.sql.impl.type.SqlYearMonthInterval;

import java.util.Map;

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
    public static final int AGGREGATE_FINISH_FUNCTION = 17;
    public static final int AGGREGATE_SUM_SUPPLIER = 18;
    public static final int AGGREGATE_AVG_SUPPLIER = 19;
    public static final int AGGREGATE_COUNT_SUPPLIER = 20;
    public static final int AGGREGATE_JSON_ARRAY_AGG_SUPPLIER = 21;
    public static final int ROW_IDENTITY_FN = 22;
    public static final int AGGREGATE_EXPORT_FUNCTION = 23;
    public static final int AGGREGATE_JSON_OBJECT_AGG_SUPPLIER = 24;
    public static final int UDT_OBJECT_TO_JSON = 26;
    public static final int INDEX_FILTER_VALUE = 27;
    public static final int INDEX_FILTER_EQUALS = 28;
    public static final int INDEX_FILTER_RANGE = 29;
    public static final int INDEX_FILTER_IN = 30;
    public static final int EXPRESSION_TO_CHAR = 31;
    public static final int EXPRESSION_COLUMN = 32;
    public static final int EXPRESSION_IS_NULL = 33;
    public static final int EXPRESSION_CONSTANT = 34;
    public static final int EXPRESSION_PARAMETER = 35;
    public static final int EXPRESSION_CAST = 36;
    public static final int EXPRESSION_DIVIDE = 37;
    public static final int EXPRESSION_MINUS = 38;
    public static final int EXPRESSION_MULTIPLY = 39;
    public static final int EXPRESSION_PLUS = 40;
    public static final int EXPRESSION_UNARY_MINUS = 41;
    public static final int EXPRESSION_AND = 42;
    public static final int EXPRESSION_OR = 43;
    public static final int EXPRESSION_NOT = 44;
    public static final int EXPRESSION_COMPARISON = 45;
    public static final int EXPRESSION_IS_TRUE = 46;
    public static final int EXPRESSION_IS_NOT_TRUE = 47;
    public static final int EXPRESSION_IS_FALSE = 48;
    public static final int EXPRESSION_IS_NOT_FALSE = 49;
    public static final int EXPRESSION_IS_NOT_NULL = 50;

    public static final int EXPRESSION_ABS = 51;
    public static final int EXPRESSION_SIGN = 52;
    public static final int EXPRESSION_RAND = 53;
    public static final int EXPRESSION_DOUBLE = 54;
    public static final int EXPRESSION_FLOOR_CEIL = 55;
    public static final int EXPRESSION_ROUND_TRUNCATE = 56;

    //region String expressions IDs
    public static final int EXPRESSION_ASCII = 57;
    public static final int EXPRESSION_CHAR_LENGTH = 58;
    public static final int EXPRESSION_INITCAP = 59;
    public static final int EXPRESSION_LOWER = 60;
    public static final int EXPRESSION_UPPER = 61;
    public static final int EXPRESSION_CONCAT = 62;
    public static final int EXPRESSION_LIKE = 63;
    public static final int EXPRESSION_SUBSTRING = 64;
    public static final int EXPRESSION_TRIM = 65;
    public static final int EXPRESSION_REMAINDER = 66;
    public static final int EXPRESSION_CONCAT_WS = 67;
    public static final int EXPRESSION_REPLACE = 68;
    public static final int EXPRESSION_POSITION = 69;
    public static final int EXPRESSION_CASE = 70;
    public static final int EXPRESSION_EXTRACT = 71;
    //endregion

    public static final int EXPRESSION_DOUBLE_DOUBLE = 72;
    public static final int EXPRESSION_TO_TIMESTAMP_TZ = 73;
    public static final int EXPRESSION_TO_EPOCH_MILLIS = 74;
    public static final int SARG_EXPRESSION = 75;
    public static final int SEARCH_PREDICATE = 76;
    public static final int EXPRESSION_FIELD_ACCESS = 77;
    public static final int EXPRESSION_ROW = 78;

    public static final int DATA_CONNECTION = 79;

    public static final int UPDATE_DATA_CONNECTION_OPERATION = 80;

    public static final int ROW_HEAP = 82;
    public static final int ROW_EMPTY = 83;

    public static final int LAZY_TARGET = 84;

    public static final int TARGET_DESCRIPTOR_GENERIC = 85;

    public static final int QUERY_PATH = 86;

    public static final int INTERVAL_YEAR_MONTH = 87;
    public static final int INTERVAL_DAY_SECOND = 88;

    public static final int EXPRESSION_GET_DDL = 90;

    public static final int LAZY_SPECIFIC_MEMBER_PROCESSOR_META_SUPPLIER = 91;

    public static final int LEN = LAZY_SPECIFIC_MEMBER_PROCESSOR_META_SUPPLIER + 1;

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
        constructors[AGGREGATE_FINISH_FUNCTION] =
                arg -> AggregateAbstractPhysicalRule.AggregateFinishFunction.INSTANCE;
        constructors[AGGREGATE_SUM_SUPPLIER] = arg -> new AggregateAbstractPhysicalRule.AggregateSumSupplier();
        constructors[AGGREGATE_AVG_SUPPLIER] = arg -> new AggregateAbstractPhysicalRule.AggregateAvgSupplier();
        constructors[AGGREGATE_COUNT_SUPPLIER] = arg -> new AggregateAbstractPhysicalRule.AggregateCountSupplier();
        constructors[AGGREGATE_JSON_ARRAY_AGG_SUPPLIER] = arg -> new AggregateAbstractPhysicalRule.AggregateArrayAggSupplier();
        constructors[ROW_IDENTITY_FN] = arg -> new AggregateAbstractPhysicalRule.RowIdentityFn();
        constructors[AGGREGATE_EXPORT_FUNCTION] = arg -> AggregateAbstractPhysicalRule.AggregateExportFunction.INSTANCE;
        constructors[AGGREGATE_JSON_OBJECT_AGG_SUPPLIER] = arg -> new AggregateAbstractPhysicalRule.AggregateObjectAggSupplier();
        constructors[UDT_OBJECT_TO_JSON] = arg -> new UdtObjectToJsonFunction();
        constructors[UPDATE_DATA_CONNECTION_OPERATION] = arg -> new UpdateDataConnectionOperation();

        constructors[INDEX_FILTER_VALUE] = arg -> new IndexFilterValue();
        constructors[INDEX_FILTER_EQUALS] = arg -> new IndexEqualsFilter();
        constructors[INDEX_FILTER_RANGE] = arg -> new IndexRangeFilter();
        constructors[INDEX_FILTER_IN] = arg -> new IndexCompositeFilter();

        constructors[EXPRESSION_TO_CHAR] = arg -> new ToCharFunction();
        constructors[EXPRESSION_COLUMN] = arg -> new ColumnExpression<>();
        constructors[EXPRESSION_IS_NULL] = arg -> new IsNullPredicate();

        constructors[EXPRESSION_CONSTANT] = arg -> new ConstantExpression<>();
        constructors[EXPRESSION_PARAMETER] = arg -> new ParameterExpression<>();
        constructors[EXPRESSION_CAST] = arg -> new CastExpression<>();
        constructors[EXPRESSION_DIVIDE] = arg -> new DivideFunction<>();
        constructors[EXPRESSION_MINUS] = arg -> new MinusFunction<>();
        constructors[EXPRESSION_MULTIPLY] = arg -> new MultiplyFunction<>();
        constructors[EXPRESSION_PLUS] = arg -> new PlusFunction<>();
        constructors[EXPRESSION_UNARY_MINUS] = arg -> new UnaryMinusFunction<>();
        constructors[EXPRESSION_AND] = arg -> new AndPredicate();
        constructors[EXPRESSION_OR] = arg -> new OrPredicate();
        constructors[EXPRESSION_NOT] = arg -> new NotPredicate();
        constructors[EXPRESSION_COMPARISON] = arg -> new ComparisonPredicate();
        constructors[EXPRESSION_IS_TRUE] = arg -> new IsTruePredicate();
        constructors[EXPRESSION_IS_NOT_TRUE] = arg -> new IsNotTruePredicate();
        constructors[EXPRESSION_IS_FALSE] = arg -> new IsFalsePredicate();
        constructors[EXPRESSION_IS_NOT_FALSE] = arg -> new IsNotFalsePredicate();
        constructors[EXPRESSION_IS_NOT_NULL] = arg -> new IsNotNullPredicate();

        constructors[EXPRESSION_ABS] = arg -> new AbsFunction<>();
        constructors[EXPRESSION_SIGN] = arg -> new SignFunction<>();
        constructors[EXPRESSION_RAND] = arg -> new RandFunction();
        constructors[EXPRESSION_DOUBLE] = arg -> new DoubleFunction();
        constructors[EXPRESSION_FLOOR_CEIL] = arg -> new FloorCeilFunction<>();
        constructors[EXPRESSION_ROUND_TRUNCATE] = arg -> new RoundTruncateFunction<>();

        constructors[EXPRESSION_ASCII] = arg -> new AsciiFunction();
        constructors[EXPRESSION_CHAR_LENGTH] = arg -> new CharLengthFunction();
        constructors[EXPRESSION_INITCAP] = arg -> new InitcapFunction();
        constructors[EXPRESSION_LOWER] = arg -> new LowerFunction();
        constructors[EXPRESSION_UPPER] = arg -> new UpperFunction();
        constructors[EXPRESSION_CONCAT] = arg -> new ConcatFunction();
        constructors[EXPRESSION_LIKE] = arg -> new LikeFunction();
        constructors[EXPRESSION_SUBSTRING] = arg -> new SubstringFunction();
        constructors[EXPRESSION_TRIM] = arg -> new TrimFunction();
        constructors[EXPRESSION_REPLACE] = arg -> new ReplaceFunction();
        constructors[EXPRESSION_POSITION] = arg -> new PositionFunction();
        constructors[EXPRESSION_REMAINDER] = arg -> new RemainderFunction<>();
        constructors[EXPRESSION_CONCAT_WS] = arg -> new ConcatWSFunction();
        constructors[EXPRESSION_CASE] = arg -> new CaseExpression<>();
        constructors[EXPRESSION_EXTRACT] = arg -> new ExtractFunction();

        constructors[EXPRESSION_DOUBLE_DOUBLE] = arg -> new DoubleBiFunction();
        constructors[EXPRESSION_TO_TIMESTAMP_TZ] = arg -> new ToTimestampTzFunction();
        constructors[EXPRESSION_TO_EPOCH_MILLIS] = arg -> new ToEpochMillisFunction();

        constructors[SARG_EXPRESSION] = arg -> new SargExpression<>();
        constructors[SEARCH_PREDICATE] = arg -> new SearchPredicate();
        constructors[EXPRESSION_FIELD_ACCESS] = arg -> new FieldAccessExpression<>();
        constructors[EXPRESSION_ROW] = arg -> new RowExpression();

        constructors[DATA_CONNECTION] = arg -> new DataConnectionCatalogEntry();

        constructors[ROW_HEAP] = arg -> new HeapRow();
        constructors[ROW_EMPTY] = arg -> EmptyRow.INSTANCE;

        constructors[LAZY_TARGET] = arg -> new LazyTarget();

        constructors[TARGET_DESCRIPTOR_GENERIC] = arg -> GenericQueryTargetDescriptor.DEFAULT;

        constructors[QUERY_PATH] = arg -> new QueryPath();

        constructors[INTERVAL_YEAR_MONTH] = arg -> new SqlYearMonthInterval();
        constructors[INTERVAL_DAY_SECOND] = arg -> new SqlDaySecondInterval();

        constructors[EXPRESSION_GET_DDL] = arg -> new GetDdlFunction();

        constructors[LAZY_SPECIFIC_MEMBER_PROCESSOR_META_SUPPLIER] = arg -> new LazyDefiningSpecificMemberPms();

        return new ArrayDataSerializableFactory(constructors);
    }

    @Override
    public void afterFactoriesCreated(Map<Integer, DataSerializableFactory> factories) {
        ArrayDataSerializableFactory mapDataFactory = (ArrayDataSerializableFactory) factories.get(SqlDataSerializerHook.F_ID);

        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors =
                new ConstructorFunction[SqlDataSerializerHook.LEN];
        constructors[SqlDataSerializerHook.QUERY_DATA_TYPE] = arg -> new QueryDataType();
        // SqlDataSerializerHook.QUERY_ID
        constructors[SqlDataSerializerHook.MAPPING] = arg -> new Mapping();
        constructors[SqlDataSerializerHook.MAPPING_FIELD] = arg -> new MappingField();
        constructors[SqlDataSerializerHook.VIEW] = arg -> new View();
        constructors[SqlDataSerializerHook.TYPE] = arg -> new Type();
        constructors[SqlDataSerializerHook.TYPE_FIELD] = arg -> new Type.TypeField();
        // SqlDataSerializerHook.ROW_VALUE
        constructors[SqlDataSerializerHook.QUERY_DATA_TYPE_FIELD] = arg -> new QueryDataType.QueryDataTypeField();

        mapDataFactory.mergeConstructors(constructors);
    }
}
