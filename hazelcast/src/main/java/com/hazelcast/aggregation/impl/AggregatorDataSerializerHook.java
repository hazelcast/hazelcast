/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.aggregation.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.util.function.Supplier;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.AGGREGATOR_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.AGGREGATOR_DS_FACTORY_ID;

public final class AggregatorDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(AGGREGATOR_DS_FACTORY, AGGREGATOR_DS_FACTORY_ID);

    public static final int BIG_DECIMAL_AVG = 0;
    public static final int BIG_DECIMAL_SUM = 1;
    public static final int BIG_INT_AVG = 2;
    public static final int BIG_INT_SUM = 3;
    public static final int COUNT = 4;
    public static final int DISTINCT_VALUES = 5;
    public static final int DOUBLE_AVG = 6;
    public static final int DOUBLE_SUM = 7;
    public static final int FIXED_SUM = 8;
    public static final int FLOATING_POINT_SUM = 9;
    public static final int INT_AVG = 10;
    public static final int INT_SUM = 11;
    public static final int LONG_AVG = 12;
    public static final int LONG_SUM = 13;
    public static final int MAX = 14;
    public static final int MIN = 15;
    public static final int NUMBER_AVG = 16;
    public static final int MAX_BY = 17;
    public static final int MIN_BY = 18;
    public static final int CANONICALIZING_SET = 19;

    private static final int LEN = CANONICALIZING_SET + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSerializableFactory createFactory() {
        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[LEN];

        constructors[BIG_DECIMAL_AVG] = BigDecimalAverageAggregator::new;
        constructors[BIG_DECIMAL_SUM] = BigDecimalSumAggregator::new;
        constructors[BIG_INT_AVG] = BigIntegerAverageAggregator::new;
        constructors[BIG_INT_SUM] = BigIntegerSumAggregator::new;
        constructors[COUNT] = CountAggregator::new;
        constructors[DISTINCT_VALUES] = DistinctValuesAggregator::new;
        constructors[DOUBLE_AVG] = DoubleAverageAggregator::new;
        constructors[DOUBLE_SUM] = DoubleSumAggregator::new;
        constructors[FIXED_SUM] = FixedSumAggregator::new;
        constructors[FLOATING_POINT_SUM] = FloatingPointSumAggregator::new;
        constructors[INT_AVG] = IntegerAverageAggregator::new;
        constructors[INT_SUM] = IntegerSumAggregator::new;
        constructors[LONG_AVG] = LongAverageAggregator::new;
        constructors[LONG_SUM] = LongSumAggregator::new;
        constructors[MAX] = MaxAggregator::new;
        constructors[MIN] = MinAggregator::new;
        constructors[NUMBER_AVG] = NumberAverageAggregator::new;
        constructors[MAX_BY] = MaxByAggregator::new;
        constructors[MIN_BY] = MinByAggregator::new;
        constructors[CANONICALIZING_SET] = CanonicalizingHashSet::new;

        return new ArrayDataSerializableFactory(constructors);
    }
}
