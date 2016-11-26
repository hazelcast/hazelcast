package com.hazelcast.aggregation;

import java.util.Random;

abstract class RandomNumberSupplier<T extends Number> {

    private static final Random RANDOM = new Random();

    T get() {
        Double randomValue = randomInt(1, 1000) * randomDouble();
        return mapFrom(randomValue);
    }

    private double randomDouble() {
        return RANDOM.nextDouble() + 0.01;
    }

    private int randomInt(int from, int to) {
        return from + RANDOM.nextInt(to);
    }

    protected abstract T mapFrom(Number value);
}
