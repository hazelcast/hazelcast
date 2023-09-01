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

package com.hazelcast.internal.tpcengine.util;

import static com.hazelcast.internal.tpcengine.util.BitUtil.isPowerOfTwo;

public final class Preconditions {

    private Preconditions() {
    }

    /**
     * Tests if a {@code value} is positive, that is strictly larger than 0
     * (value &gt; 0).
     *
     * @param value     the value tested to see if it is positive.
     * @param paramName the the name of the checked parameter that will be in
     *                 exception message
     * @param value     the value tested to see if it is positive.
     * @return the value
     * @throws IllegalArgumentException if the value is not positive.
     */
    public static int checkPositive(int value, String paramName) {
        if (value <= 0) {
            throw new IllegalArgumentException(paramName + " is " + value + " but must be > 0");
        }
        return value;
    }


    public static void checkIsLessThanOrEqual(int firstValue, String firstName,
                                              int secondValue, String secondName) {
        if (firstValue > secondValue) {
            throw new IllegalArgumentException(firstName + " should be smaller than " + secondName);
        }
    }

    /**
     * Tests if a {@code value} is positive, that is strictly larger than 0
     * (value &gt; 0).
     *
     * @param value     the value tested to see if it is positive.
     * @param paramName the the name of the checked parameter that will be in
     *                 exception message
     * @param value     the value tested to see if it is positive.
     * @return the value
     * @throws IllegalArgumentException if the value is not positive.
     */
    public static long checkPositive(long value, String paramName) {
        if (value <= 0) {
            throw new IllegalArgumentException(paramName + " is " + value + " but must be > 0");
        }
        return value;
    }

    /**
     * Tests if the {@code value} is &gt;= 0.
     *
     * @param value     the  value tested to see if it is not negative.
     * @param paramName the the name of the checked parameter that will be in
     *                  exception message
     * @return the value
     * @throws java.lang.IllegalArgumentException if the value is negative.
     */
    public static int checkNotNegative(int value, String paramName) {
        if (value < 0) {
            throw new IllegalArgumentException(paramName + " is " + value + " but must be >= 0");
        }
        return value;
    }

    /**
     * Tests if value is a power of 2.
     *
     * @param value
     * @param paramName
     * @return
     */
    public static int checkPowerOf2(int value, String paramName) {
        if (!isPowerOfTwo(value)) {
            throw new IllegalArgumentException(paramName + " is " + value
                    + " but must be a power of 2.");
        }
        return value;
    }

    /**
     * Tests if the {@code value} is &gt;= 0.
     *
     * @param value     the  value tested to see if it is not negative.
     * @param paramName the the name of the checked parameter that will be in
     *                  exception message
     * @return the value
     * @throws java.lang.IllegalArgumentException if the value is negative.
     */
    public static long checkNotNegative(long value, String paramName) {
        if (value < 0) {
            throw new IllegalArgumentException(paramName + " is " + value
                    + " but must be >= 0");
        }
        return value;
    }

    /**
     * Tests if an argument is not <code>null</code>.
     *
     * @param argument the argument tested to see if it is not null.
     * @param name     the name of the variable/field that can't be null
     * @return the argument that was tested.
     * @throws NullPointerException if argument is null
     */
    public static <T> T checkNotNull(T argument, String name) {
        if (argument == null) {
            throw new NullPointerException(name + " can't be null");
        }
        return argument;
    }

    /**
     * Tests if an argument is <code>null</code>.
     *
     * @param argument the argument tested to see if it is <code>null</code>.
     * @param name     the name of the variable/field that must be <code>null</code>
     * @return the argument that was tested.
     * @throws IllegalArgumentException if argument is not null
     */
    public static <T> void checkNull(T argument, String name) {
        if (argument != null) {
            throw new IllegalArgumentException(name + " must be null");
        }
    }

    /**
     * Tests if an argument is not null.
     *
     * @param argument the argument tested to see if it is not null.
     * @return the argument that was tested.
     * @throws NullPointerException if argument is null
     */
    public static <T> T checkNotNull(T argument) {
        if (argument == null) {
            throw new NullPointerException();
        }
        return argument;
    }

}
