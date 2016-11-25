package com.hazelcast.aggregation;

import com.hazelcast.map.impl.MapEntrySimple;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

final class TestDoubles {

    private static final int NUMBER_OF_SAMPLE_VALUES = 10000;

    private TestDoubles() {
    }

    static <T> Map.Entry<T, T> createEntryWithValue(T value) {
        return new MapEntrySimple<T, T>(value, value);
    }

    static <T extends Number> List<T> sampleValues(RandomNumberSupplier<T> randomNumberSupplier) {
        List<T> numbers = new ArrayList<T>();
        for (int i = 0; i < NUMBER_OF_SAMPLE_VALUES; i++) {
            numbers.add(randomNumberSupplier.get());
        }
        return numbers;
    }

    static List<BigDecimal> sampleBigDecimals() {
        return sampleValues(new RandomNumberSupplier<BigDecimal>() {
            @Override
            protected BigDecimal mapFrom(Number value) {
                return BigDecimal.valueOf(value.doubleValue());
            }
        });
    }

    static List<BigInteger> sampleBigIntegers() {
        return sampleValues(new RandomNumberSupplier<BigInteger>() {
            @Override
            protected BigInteger mapFrom(Number value) {
                return BigInteger.valueOf(value.longValue());
            }
        });
    }

    static List<Double> sampleDoubles() {
        return sampleValues(new RandomNumberSupplier<Double>() {
            @Override
            protected Double mapFrom(Number value) {
                return value.doubleValue();
            }
        });
    }

    static List<Integer> sampleIntegers() {
        return sampleValues(new RandomNumberSupplier<Integer>() {
            @Override
            protected Integer mapFrom(Number value) {
                return value.intValue();
            }
        });
    }

    static List<Long> sampleLongs() {
        return sampleValues(new RandomNumberSupplier<Long>() {
            @Override
            protected Long mapFrom(Number value) {
                return value.longValue();
            }
        });
    }

    static List<String> sampleStrings() {
        String loremIpsum = "Lorem ipsum dolor sit amet consectetur adipiscing elit";
        return asList(loremIpsum.split(" "));
    }

    static Collection<Float> sampleFloats() {
        return sampleValues(new RandomNumberSupplier<Float>() {
            @Override
            protected Float mapFrom(Number value) {
                return value.floatValue();
            }
        });
    }
}
