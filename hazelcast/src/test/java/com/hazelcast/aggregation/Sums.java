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

package com.hazelcast.aggregation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

final class Sums {

    private Sums() {
    }

    static Long sumIntegers(List<Integer> values) {
        long sum = 0L;
        for (Integer value : values) {
            sum += value;
        }
        return sum;
    }

    static Long sumLongs(List<Long> values) {
        long sum = 0L;
        for (long value : values) {
            sum += value;
        }
        return sum;
    }

    static double sumFloatingPointNumbers(List<? extends Number> values) {
        double sum = 0L;
        for (Number value : values) {
            sum += value.doubleValue();
        }
        return sum;
    }

    static double sumDoubles(List<Double> values) {
        double sum = 0L;
        for (double value : values) {
            sum += value;
        }
        return sum;
    }

    static BigDecimal sumBigDecimals(List<BigDecimal> values) {
        BigDecimal sum = BigDecimal.ZERO;
        for (BigDecimal value : values) {
            sum = sum.add(value);
        }
        return sum;
    }

    static BigInteger sumBigIntegers(List<BigInteger> values) {
        BigInteger sum = BigInteger.ZERO;
        for (BigInteger value : values) {
            sum = sum.add(value);
        }
        return sum;
    }

    static long sumFixedPointNumbers(List<? extends Number> values) {
        long sum = 0L;
        for (Number value : values) {
            sum += value.longValue();
        }
        return sum;
    }

    @SuppressWarnings("unchecked")
    static <T extends Number> T sumValueContainer(List<ValueContainer> values, ValueContainer.ValueType valueType) {
        switch (valueType) {
            case INTEGER:
                Long intSum = 0L;
                for (ValueContainer container : values) {
                    intSum += container.intValue;
                }
                return (T) intSum;
            case LONG:
                Long longSum = 0L;
                for (ValueContainer container : values) {
                    longSum += container.longValue;
                }
                return (T) longSum;
            case FLOAT:
                Double floatSum = 0d;
                for (ValueContainer container : values) {
                    floatSum += container.floatValue;
                }
                return (T) floatSum;
            case DOUBLE:
                Double doubleSum = 0d;
                for (ValueContainer container : values) {
                    doubleSum += container.doubleValue;
                }
                return (T) doubleSum;
            case BIG_DECIMAL:
                BigDecimal bigDecimalSum = BigDecimal.ZERO;
                for (ValueContainer container : values) {
                    bigDecimalSum = bigDecimalSum.add(container.bigDecimal);
                }
                return (T) bigDecimalSum;
            case BIG_INTEGER:
                BigInteger bigIntegerSum = BigInteger.ZERO;
                for (ValueContainer container : values) {
                    bigIntegerSum = bigIntegerSum.add(container.bigInteger);
                }
                return (T) bigIntegerSum;
            case NUMBER:
                Double numberSum = 0d;
                for (ValueContainer container : values) {
                    numberSum += container.numberValue.doubleValue();
                }
                return (T) numberSum;
            default:
                return null;
        }
    }
}
