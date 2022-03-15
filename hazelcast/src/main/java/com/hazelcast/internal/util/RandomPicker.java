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

package com.hazelcast.internal.util;

import java.util.Random;

/**
 * User: ahmetmircik
 * Date: 10/7/13
 * Time: 12:27 PM
 */
public final class RandomPicker {

    private static Random randomNumberGenerator;

    private RandomPicker() {
    }

    private static synchronized void initRNG() {
        if (randomNumberGenerator == null) {
            randomNumberGenerator = new Random();
        }
    }

    /**
     * Returns a pseudorandom, uniformly distributed int value between 0 (inclusive)
     * and the specified value (exclusive), drawn from this random number generator's sequence.
     * Starts the random number generator sequence if it has not been initialized.
     *
     * @param n the specified value
     * @return a value between 0 (inclusive) and the specified value (exclusive).
     */
    public static int getInt(int n) {
        if (randomNumberGenerator == null) {
            initRNG();
        }
        return randomNumberGenerator.nextInt(n);
    }


    /**
     * Return a pseudorandom, uniformly distributed in value between the low value (inclusive) and
     * the high value (exclusive), drawn from this random number generator's sequence.
     * Starts the random number generator sequence if it has not been initialized.
     *
     * @param low lowest value of the range (inclusive)
     * @param high highest value of the range (exclusive)
     * @return a value between the specified low (inclusive) and high value (exclusive).
     */
    public static int getInt(int low, int high) {
        return getInt(high - low) + low;
    }

}
