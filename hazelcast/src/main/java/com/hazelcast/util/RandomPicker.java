package com.hazelcast.util;

import java.util.Random;

/**
 * User: ahmetmircik
 * Date: 10/7/13
 * Time: 12:27 PM
 *
 */
public final class RandomPicker {

    private RandomPicker() {}

    private static Random randomNumberGenerator;

    private static synchronized void initRNG() {
        if (randomNumberGenerator == null)
            randomNumberGenerator = new Random();
    }

    public static int getInt(int n) {
        if (randomNumberGenerator == null) initRNG();
        return randomNumberGenerator.nextInt(n);
    }

}
