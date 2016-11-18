package com.hazelcast.aggregation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

final class Sums {
    private Sums() {
        // Utility class
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

    static double sumDoubles(List<Double> values) {
        double sum = 0L;
        for (double value : values) {
            sum += value;
        }
        return sum;
    }

    public static double sumFloatingPointNumbers(List<? extends Number> values) {
        double sum = 0L;
        for (Number value : values) {
            sum += value.doubleValue();
        }
        return sum;
    }

    public static long sumFixedPointNumbers(List<? extends Number> values) {
        long sum = 0L;
        for (Number value : values) {
            sum += value.longValue();
        }
        return sum;
    }
}
