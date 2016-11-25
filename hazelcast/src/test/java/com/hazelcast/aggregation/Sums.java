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
    static <T extends Number> T sumNumberContainer(List<NumberContainer> values, NumberContainer.ValueType valueType) {
        switch (valueType) {
            case INTEGER:
                Long intSum = 0L;
                for (NumberContainer container : values) {
                    intSum += container.intValue;
                }
                return (T) intSum;
            case LONG:
                Long longSum = 0L;
                for (NumberContainer container : values) {
                    longSum += container.longValue;
                }
                return (T) longSum;
            case FLOAT:
                Double floatSum = 0d;
                for (NumberContainer container : values) {
                    floatSum += container.floatValue;
                }
                return (T) floatSum;
            case DOUBLE:
                Double doubleSum = 0d;
                for (NumberContainer container : values) {
                    doubleSum += container.doubleValue;
                }
                return (T) doubleSum;
            case BIG_DECIMAL:
                BigDecimal bigDecimalSum = BigDecimal.ZERO;
                for (NumberContainer container : values) {
                    bigDecimalSum = bigDecimalSum.add(container.bigDecimal);
                }
                return (T) bigDecimalSum;
            case BIG_INTEGER:
                BigInteger bigIntegerSum = BigInteger.ZERO;
                for (NumberContainer container : values) {
                    bigIntegerSum = bigIntegerSum.add(container.bigInteger);
                }
                return (T) bigIntegerSum;
            case NUMBER:
                Double numberSum = 0d;
                for (NumberContainer container : values) {
                    numberSum += container.numberValue.doubleValue();
                }
                return (T) numberSum;
        }
        return null;
    }
}
