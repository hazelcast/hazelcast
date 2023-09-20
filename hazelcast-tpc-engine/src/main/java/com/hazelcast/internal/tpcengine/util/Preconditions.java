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

public final class Preconditions {

    private Preconditions() {
    }

    public static <E> E checkInstanceOf(Class<E> type, Object object, String paramName) {
        checkNotNull(type, "type");
        checkNotNull(object, paramName);

        if (!type.isInstance(object)) {
            throw new IllegalArgumentException("object " + object + " of type " + object.getClass()
                    + " is not an instanceof " + type);
        }
        return (E) object;
    }

    /**
     * Tests if a {@code value} is positive, that is strictly larger than 0 (value &gt; 0).
     *
     * @param value     the value tested to see if it is positive.
     * @param paramName the the name of the checked parameter that will be in exception message
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

    /**
     * Tests if the {@code value} is &gt;= 0.
     *
     * @param value     the  value tested to see if it is not negative.
     * @param paramName the the name of the checked parameter that will be in exception message
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
     * Tests if the {@code value} is &gt;= 0.
     *
     * @param value     the  value tested to see if it is not negative.
     * @param paramName the the name of the checked parameter that will be in exception message
     * @return the value
     * @throws java.lang.IllegalArgumentException if the value is negative.
     */
    public static long checkNotNegative(long value, String paramName) {
        if (value < 0) {
            throw new IllegalArgumentException(paramName + " is " + value + " but must be >= 0");
        }
        return value;
    }

    /**
     * Tests if an argument is not null.
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
