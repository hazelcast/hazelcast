/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.exec.scan.index.IndexCompositeFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexEqualsFilter;
import com.hazelcast.sql.impl.exec.scan.index.IndexFilterValue;
import com.hazelcast.sql.impl.exec.scan.index.IndexRangeFilter;
import com.hazelcast.sql.impl.expression.CaseExpression;
import com.hazelcast.sql.impl.expression.CastExpression;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.ParameterExpression;
import com.hazelcast.sql.impl.expression.SearchableExpression;
import com.hazelcast.sql.impl.expression.datetime.ExtractFunction;
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
import com.hazelcast.sql.impl.schema.view.View;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.SqlDaySecondInterval;
import com.hazelcast.sql.impl.type.SqlYearMonthInterval;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SQL_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SQL_DS_FACTORY_ID;

/**
 * Serialization hook for SQL classes.
 */
public class SqlDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(SQL_DS_FACTORY, SQL_DS_FACTORY_ID);

    public static final int QUERY_DATA_TYPE = 0;

    public static final int QUERY_ID = 1;

    public static final int ROW_HEAP = 2;
    public static final int ROW_EMPTY = 3;

    public static final int LAZY_TARGET = 4;

    public static final int INDEX_FILTER_VALUE = 5;
    public static final int INDEX_FILTER_EQUALS = 6;
    public static final int INDEX_FILTER_RANGE = 7;
    public static final int INDEX_FILTER_IN = 8;

    public static final int EXPRESSION_COLUMN = 10;
    public static final int EXPRESSION_IS_NULL = 11;

    public static final int TARGET_DESCRIPTOR_GENERIC = 12;

    public static final int QUERY_PATH = 13;

    public static final int EXPRESSION_CONSTANT = 14;
    public static final int EXPRESSION_PARAMETER = 15;
    public static final int EXPRESSION_CAST = 16;
    public static final int EXPRESSION_DIVIDE = 17;
    public static final int EXPRESSION_MINUS = 18;
    public static final int EXPRESSION_MULTIPLY = 19;
    public static final int EXPRESSION_PLUS = 20;
    public static final int EXPRESSION_UNARY_MINUS = 21;
    public static final int EXPRESSION_AND = 22;
    public static final int EXPRESSION_OR = 23;
    public static final int EXPRESSION_NOT = 24;
    public static final int EXPRESSION_COMPARISON = 25;
    public static final int EXPRESSION_IS_TRUE = 26;
    public static final int EXPRESSION_IS_NOT_TRUE = 27;
    public static final int EXPRESSION_IS_FALSE = 28;
    public static final int EXPRESSION_IS_NOT_FALSE = 29;
    public static final int EXPRESSION_IS_NOT_NULL = 30;

    public static final int EXPRESSION_ABS = 31;
    public static final int EXPRESSION_SIGN = 32;
    public static final int EXPRESSION_RAND = 33;
    public static final int EXPRESSION_DOUBLE = 34;
    public static final int EXPRESSION_FLOOR_CEIL = 35;
    public static final int EXPRESSION_ROUND_TRUNCATE = 36;

    public static final int INTERVAL_YEAR_MONTH = 37;
    public static final int INTERVAL_DAY_SECOND = 38;

    //region String expressions IDs
    public static final int EXPRESSION_ASCII = 39;
    public static final int EXPRESSION_CHAR_LENGTH = 40;
    public static final int EXPRESSION_INITCAP = 41;
    public static final int EXPRESSION_LOWER = 42;
    public static final int EXPRESSION_UPPER = 43;
    public static final int EXPRESSION_CONCAT = 44;
    public static final int EXPRESSION_LIKE = 45;
    public static final int EXPRESSION_SUBSTRING = 46;
    public static final int EXPRESSION_TRIM = 47;
    public static final int EXPRESSION_REMAINDER = 48;
    public static final int EXPRESSION_CONCAT_WS = 49;
    public static final int EXPRESSION_REPLACE = 50;
    public static final int EXPRESSION_POSITION = 51;
    public static final int EXPRESSION_CASE = 52;
    public static final int EXPRESSION_EXTRACT = 53;
    //endregion

    public static final int EXPRESSION_DOUBLE_DOUBLE = 54;
    public static final int EXPRESSION_TO_TIMESTAMP_TZ = 55;
    public static final int EXPRESSION_TO_EPOCH_MILLIS = 56;

    public static final int MAPPING = 57;
    public static final int MAPPING_FIELD = 58;

    public static final int EXPRESSION_SEARCHABLE = 59;
    public static final int EXPRESSION_SEARCH = 60;

    public static final int VIEW = 61;

    public static final int LEN = VIEW + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[QUERY_DATA_TYPE] = arg -> new QueryDataType();

        constructors[QUERY_ID] = arg -> new QueryId();

        constructors[ROW_HEAP] = arg -> new HeapRow();
        constructors[ROW_EMPTY] = arg -> EmptyRow.INSTANCE;

        constructors[LAZY_TARGET] = arg -> new LazyTarget();

        constructors[INDEX_FILTER_VALUE] = arg -> new IndexFilterValue();
        constructors[INDEX_FILTER_EQUALS] = arg -> new IndexEqualsFilter();
        constructors[INDEX_FILTER_RANGE] = arg -> new IndexRangeFilter();
        constructors[INDEX_FILTER_IN] = arg -> new IndexCompositeFilter();

        constructors[EXPRESSION_COLUMN] = arg -> new ColumnExpression<>();
        constructors[EXPRESSION_IS_NULL] = arg -> new IsNullPredicate();

        constructors[TARGET_DESCRIPTOR_GENERIC] = arg -> GenericQueryTargetDescriptor.DEFAULT;

        constructors[QUERY_PATH] = arg -> new QueryPath();

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

        constructors[INTERVAL_YEAR_MONTH] = arg -> new SqlYearMonthInterval();
        constructors[INTERVAL_DAY_SECOND] = arg -> new SqlDaySecondInterval();

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

        constructors[MAPPING] = arg -> new Mapping();
        constructors[MAPPING_FIELD] = arg -> new MappingField();

        constructors[EXPRESSION_SEARCHABLE] = arg -> new SearchableExpression<>();
        constructors[EXPRESSION_SEARCH] = arg -> new SearchPredicate();

        constructors[VIEW] = arg -> new View();

        return new ArrayDataSerializableFactory(constructors);
    }
}
