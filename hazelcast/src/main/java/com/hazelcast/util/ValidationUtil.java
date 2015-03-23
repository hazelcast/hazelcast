/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import static java.lang.String.format;

/**
 * A utility class for validating arguments and state.
 */
public final class ValidationUtil {

    private ValidationUtil() {
    }

    /**
     * Tests if an argument is not null.
     *
     * @param argument the argument
     * @param message  message thrown if argument is null
     * @return the argument
     * @throws java.lang.NullPointerException if argument is null
     */
    public static <T> T checkNotNull(T argument, String message) {
        if (argument == null) {
            throw new NullPointerException(message);
        }
        return argument;
    }

    /**
     * Tests if a string contains text.
     *
     * @param argument the string to test
     * @param argName  the string name (used in message if an error is thrown)
     * @return the string argument
     * @throws java.lang.IllegalArgumentException if the string is empty
     */
    public static String hasText(String argument, String argName) {
        isNotNull(argument, argName);

        if (argument.isEmpty()) {
            throw new IllegalArgumentException(format("argument '%s' can't be an empty string", argName));
        }

        return argument;
    }

    /**
     * Tests if a string is not null.
     *
     * @param argument the string to test
     * @param argName  the string name (used in message if an error is thrown)
     * @return the string argument
     * @throws java.lang.IllegalArgumentException if the string is null
     */
    public static <E> E isNotNull(E argument, String argName) {
        if (argument == null) {
            throw new IllegalArgumentException(format("argument '%s' can't be null", argName));
        }

        return argument;
    }

    /**
     * Tests if a long value is not negative.
     *
     * @param value        the long value to test
     * @param argumentName the value name (used in message if an error is thrown)
     * @throws java.lang.IllegalArgumentException if the value is negative
     */
    public static void isNotNegative(long value, String argumentName) {
        if (value < 0) {
            throw new IllegalArgumentException(argumentName + " cannot be negative!");
        }
    }

    /**
     * Tests if a long value is positive.
     *
     * @param value        the long value to test
     * @param argumentName the value name (used in message if an error is thrown)
     * @throws java.lang.IllegalArgumentException if the value is not positive
     */
    public static void shouldBePositive(long value, String argumentName) {
        if (value <= 0) {
            throw new IllegalArgumentException(argumentName + " should be positive!");
        }
    }


    /**
     * Tests whether the supplied object is an instance of the supplied class type.
     *
     * @param type         the expected type
     * @param object       the object to be tested against the type
     * @param argumentName the argument name of the object
     * @return the object argument.
     * @throws java.lang.IllegalArgumentException if the object is not an instance of the type
     */
    public static <E> E checkInstanceOf(Class type, E object, String argumentName) {
        isNotNull(type, "type");
        if (!type.isInstance(object)) {
            throw new IllegalArgumentException(argumentName + " should be an instance of " + type
                    + ", but found " + object);
        }
        return object;
    }

    /**
     * Tests the supplied object is not a type of the supplied class.
     *
     * @param type         the type which is not expected
     * @param object       the object to be tested against the type
     * @param argumentName the argument name of the object
     * @return the object argument.
     * @throws java.lang.IllegalArgumentException if the object is an instance of the type
     */
    public static <E> E checkNotInstanceOf(Class type, E object, String argumentName) {
        isNotNull(type, "type");
        if (type.isInstance(object)) {
            throw new IllegalArgumentException(argumentName + " can not be an instance of " + type);
        }
        return object;
    }

    /**
     * Tests whether the supplied expression is {@code false}.
     *
     * @param expression expression to be tested.
     * @param message    exception message in case of a failure.
     * @throws java.lang.IllegalArgumentException if the supplied expression is {@code true}
     */
    public static void checkFalse(boolean expression, String message) {
        if (expression) {
            throw new IllegalArgumentException(message);
        }
    }
}

