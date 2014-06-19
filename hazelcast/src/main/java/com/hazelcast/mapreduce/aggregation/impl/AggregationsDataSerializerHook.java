/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.aggregation.impl;

import com.hazelcast.nio.serialization.ArrayDataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;

/**
 * This class contains all the ID hooks for IdentifiedDataSerializable classes used for aggregations.
 */
//Deactivated all checkstyle rules because those classes will never comply
//CHECKSTYLE:OFF
public class AggregationsDataSerializerHook
        implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.AGGREGATIONS_DS_FACTORY, -24);

    public static final int SUPPLIER_CONSUMING_MAPPER = 0;
    public static final int ACCEPT_ALL_SUPPLIER = 1;
    public static final int DISTINCT_VALUES_MAPPER = 2;
    public static final int SET_ADAPTER = 3;
    public static final int BIG_DECIMAL_AVG_COMBINER_FACTORY = 4;
    public static final int BIG_DECIMAL_AVG_REDUCER_FACTORY = 5;
    public static final int BIG_DECIMAL_MAX_COMBINER_FACTORY = 6;
    public static final int BIG_DECIMAL_MAX_REDUCER_FACTORY = 7;
    public static final int BIG_DECIMAL_MIN_COMBINER_FACTORY = 8;
    public static final int BIG_DECIMAL_MIN_REDUCER_FACTORY = 9;
    public static final int BIG_DECIMAL_SUM_COMBINER_FACTORY = 10;
    public static final int BIG_DECIMAL_SUM_REDUCER_FACTORY = 11;
    public static final int BIG_INTEGER_AVG_COMBINER_FACTORY = 12;
    public static final int BIG_INTEGER_AVG_REDUCER_FACTORY = 13;
    public static final int BIG_INTEGER_MAX_COMBINER_FACTORY = 14;
    public static final int BIG_INTEGER_MAX_REDUCER_FACTORY = 15;
    public static final int BIG_INTEGER_MIN_COMBINER_FACTORY = 16;
    public static final int BIG_INTEGER_MIN_REDUCER_FACTORY = 17;
    public static final int BIG_INTEGER_SUM_COMBINER_FACTORY = 18;
    public static final int BIG_INTEGER_SUM_REDUCER_FACTORY = 19;
    public static final int COMPARABLE_MAX_COMBINER_FACTORY = 20;
    public static final int COMPARABLE_MAX_REDUCER_FACTORY = 21;
    public static final int COMPARABLE_MIN_COMBINER_FACTORY = 22;
    public static final int COMPARABLE_MIN_REDUCER_FACTORY = 23;
    public static final int COUNT_COMBINER_FACTORY = 24;
    public static final int COUNT_REDUCER_FACTORY = 25;
    public static final int DISTINCT_VALUES_COMBINER_FACTORY = 26;
    public static final int DISTINCT_VALUES_REDUCER_FACTORY = 27;
    public static final int DOUBLE_AVG_COMBINER_FACTORY = 28;
    public static final int DOUBLE_AVG_REDUCER_FACTORY = 29;
    public static final int DOUBLE_MAX_COMBINER_FACTORY = 30;
    public static final int DOUBLE_MAX_REDUCER_FACTORY = 31;
    public static final int DOUBLE_MIN_COMBINER_FACTORY = 32;
    public static final int DOUBLE_MIN_REDUCER_FACTORY = 33;
    public static final int DOUBLE_SUM_COMBINER_FACTORY = 34;
    public static final int DOUBLE_SUM_REDUCER_FACTORY = 35;
    public static final int INTEGER_AVG_COMBINER_FACTORY = 36;
    public static final int INTEGER_AVG_REDUCER_FACTORY = 37;
    public static final int INTEGER_MAX_COMBINER_FACTORY = 38;
    public static final int INTEGER_MAX_REDUCER_FACTORY = 39;
    public static final int INTEGER_MIN_COMBINER_FACTORY = 40;
    public static final int INTEGER_MIN_REDUCER_FACTORY = 41;
    public static final int INTEGER_SUM_COMBINER_FACTORY = 42;
    public static final int INTEGER_SUM_REDUCER_FACTORY = 43;
    public static final int LONG_AVG_COMBINER_FACTORY = 44;
    public static final int LONG_AVG_REDUCER_FACTORY = 45;
    public static final int LONG_MAX_COMBINER_FACTORY = 46;
    public static final int LONG_MAX_REDUCER_FACTORY = 47;
    public static final int LONG_MIN_COMBINER_FACTORY = 48;
    public static final int LONG_MIN_REDUCER_FACTORY = 49;
    public static final int LONG_SUM_COMBINER_FACTORY = 50;
    public static final int LONG_SUM_REDUCER_FACTORY = 51;
    public static final int KEY_PREDICATE_SUPPLIER = 52;
    public static final int PREDICATE_SUPPLIER = 53;
    public static final int AVG_TUPLE = 54;

    private static final int LEN = AVG_TUPLE + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];
        constructors[SUPPLIER_CONSUMING_MAPPER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SupplierConsumingMapper();
            }
        };
        constructors[ACCEPT_ALL_SUPPLIER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AcceptAllSupplier();
            }
        };
        constructors[DISTINCT_VALUES_MAPPER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DistinctValuesAggregation.DistinctValueMapper();
            }
        };
        constructors[SET_ADAPTER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new SetAdapter();
            }
        };
        constructors[AVG_TUPLE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AvgTuple();
            }
        };
        constructors[BIG_DECIMAL_AVG_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BigDecimalAvgAggregation.BigDecimalAvgCombinerFactory();
            }
        };
        constructors[BIG_DECIMAL_AVG_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BigDecimalAvgAggregation.BigDecimalAvgReducerFactory();
            }
        };
        constructors[BIG_DECIMAL_MAX_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BigDecimalMaxAggregation.BigDecimalMaxCombinerFactory();
            }
        };
        constructors[BIG_DECIMAL_MAX_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BigDecimalMaxAggregation.BigDecimalMaxReducerFactory();
            }
        };
        constructors[BIG_DECIMAL_MIN_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BigDecimalMinAggregation.BigDecimalMinCombinerFactory();
            }
        };
        constructors[BIG_DECIMAL_MIN_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BigDecimalMinAggregation.BigDecimalMinReducerFactory();
            }
        };
        constructors[BIG_DECIMAL_SUM_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BigDecimalSumAggregation.BigDecimalSumCombinerFactory();
            }
        };
        constructors[BIG_DECIMAL_SUM_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BigDecimalSumAggregation.BigDecimalSumReducerFactory();
            }
        };
        constructors[BIG_INTEGER_AVG_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BigIntegerAvgAggregation.BigIntegerAvgCombinerFactory();
            }
        };
        constructors[BIG_INTEGER_AVG_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BigIntegerAvgAggregation.BigIntegerAvgReducerFactory();
            }
        };
        constructors[BIG_INTEGER_MAX_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BigIntegerMaxAggregation.BigIntegerMaxCombinerFactory();
            }
        };
        constructors[BIG_INTEGER_MAX_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BigIntegerMaxAggregation.BigIntegerMaxReducerFactory();
            }
        };
        constructors[BIG_INTEGER_MIN_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BigIntegerMinAggregation.BigIntegerMinCombinerFactory();
            }
        };
        constructors[BIG_INTEGER_MIN_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BigIntegerMinAggregation.BigIntegerMinReducerFactory();
            }
        };
        constructors[BIG_INTEGER_SUM_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BigIntegerSumAggregation.BigIntegerSumCombinerFactory();
            }
        };
        constructors[BIG_INTEGER_SUM_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new BigIntegerSumAggregation.BigIntegerSumReducerFactory();
            }
        };
        constructors[COMPARABLE_MAX_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ComparableMaxAggregation.ComparableMaxCombinerFactory();
            }
        };
        constructors[COMPARABLE_MAX_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ComparableMaxAggregation.ComparableMaxReducerFactory();
            }
        };
        constructors[COMPARABLE_MIN_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ComparableMinAggregation.ComparableMinCombinerFactory();
            }
        };
        constructors[COMPARABLE_MIN_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ComparableMinAggregation.ComparableMinReducerFactory();
            }
        };
        constructors[COUNT_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CountAggregation.CountCombinerFactory();
            }
        };
        constructors[COUNT_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CountAggregation.CountReducerFactory();
            }
        };
        constructors[DISTINCT_VALUES_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DistinctValuesAggregation.DistinctValuesCombinerFactory();
            }
        };
        constructors[DISTINCT_VALUES_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DistinctValuesAggregation.DistinctValuesReducerFactory();
            }
        };
        constructors[DOUBLE_AVG_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DoubleAvgAggregation.DoubleAvgCombinerFactory();
            }
        };
        constructors[DOUBLE_AVG_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DoubleAvgAggregation.DoubleAvgReducerFactory();
            }
        };
        constructors[DOUBLE_MAX_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DoubleMaxAggregation.DoubleMaxCombinerFactory();
            }
        };
        constructors[DOUBLE_MAX_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DoubleMaxAggregation.DoubleMaxReducerFactory();
            }
        };
        constructors[DOUBLE_MIN_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DoubleMinAggregation.DoubleMinCombinerFactory();
            }
        };
        constructors[DOUBLE_MIN_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DoubleMinAggregation.DoubleMinReducerFactory();
            }
        };
        constructors[DOUBLE_SUM_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DoubleSumAggregation.DoubleSumCombinerFactory();
            }
        };
        constructors[DOUBLE_SUM_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new DoubleSumAggregation.DoubleSumReducerFactory();
            }
        };
        constructors[INTEGER_AVG_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new IntegerAvgAggregation.IntegerAvgCombinerFactory();
            }
        };
        constructors[INTEGER_AVG_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new IntegerAvgAggregation.IntegerAvgReducerFactory();
            }
        };
        constructors[INTEGER_MAX_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new IntegerMaxAggregation.IntegerMaxCombinerFactory();
            }
        };
        constructors[INTEGER_MAX_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new IntegerMaxAggregation.IntegerMaxReducerFactory();
            }
        };
        constructors[INTEGER_MIN_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new IntegerMinAggregation.IntegerMinCombinerFactory();
            }
        };
        constructors[INTEGER_MIN_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new IntegerMinAggregation.IntegerMinReducerFactory();
            }
        };
        constructors[INTEGER_SUM_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new IntegerSumAggregation.IntegerSumCombinerFactory();
            }
        };
        constructors[INTEGER_SUM_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new IntegerSumAggregation.IntegerSumReducerFactory();
            }
        };
        constructors[LONG_AVG_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LongAvgAggregation.LongAvgCombinerFactory();
            }
        };
        constructors[LONG_AVG_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LongAvgAggregation.LongAvgReducerFactory();
            }
        };
        constructors[LONG_MAX_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LongMaxAggregation.LongMaxCombinerFactory();
            }
        };
        constructors[LONG_MAX_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LongMaxAggregation.LongMaxReducerFactory();
            }
        };
        constructors[LONG_MIN_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LongMinAggregation.LongMinCombinerFactory();
            }
        };
        constructors[LONG_MIN_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LongMinAggregation.LongMinReducerFactory();
            }
        };
        constructors[LONG_SUM_COMBINER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LongSumAggregation.LongSumCombinerFactory();
            }
        };
        constructors[LONG_SUM_REDUCER_FACTORY] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new LongSumAggregation.LongSumReducerFactory();
            }
        };
        constructors[KEY_PREDICATE_SUPPLIER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new KeyPredicateSupplier();
            }
        };
        constructors[PREDICATE_SUPPLIER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            @Override
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PredicateSupplier();
            }
        };
        return new ArrayDataSerializableFactory(constructors);
    }
}
