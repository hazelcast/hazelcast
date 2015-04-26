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
public final class Preconditions {

    private Preconditions() {
    }

    /**
     * Tests if an argument is not null.
     *
     * @param argument the argument tested to see if it is not null.
     * @param errorMessage the errorMessage
     * @return the argument that was tested.
     * @throws java.lang.NullPointerException if argument is null
     */
    public static <T> T checkNotNull(T argument, String errorMessage) {
        if (argument == null) {
            throw new NullPointerException(errorMessage);
        }
        return argument;
    }

    /**
     * Tests if a string contains text.
     *
     * @param argument the string tested to see if it contains text.
     * @param errorMessage the errorMessage
     * @return the string argument that was tested.
     * @throws java.lang.IllegalArgumentException if the string is empty
     */
    public static String checkHasText(String argument, String errorMessage) {
        if (argument == null || argument.isEmpty()) {
            throw new IllegalArgumentException(errorMessage);
        }

        return argument;
    }

    /**
     * Tests if a string is not null.
     *
     * @param argument the string tested to see if it is not null.
     * @param argName  the string name (used in message if an error is thrown).
     * @return the string argument that was tested.
     * @throws java.lang.IllegalArgumentException if the string is null.
     */
    public static <E> E isNotNull(E argument, String argName) {
        if (argument == null) {
            throw new IllegalArgumentException(format("argument '%s' can't be null", argName));
        }

        return argument;
    }

    /**
     * Tests if a value is not negative.
     *
     * @param value        the value tested to see if it is not negative.
     * @param errorMessage the errorMessage
     * @return the value
     * @throws java.lang.IllegalArgumentException if the value is negative.
     */
    public static long checkNotNegative(long value, String errorMessage) {
        if (value < 0) {
            throw new IllegalArgumentException(errorMessage);
        }
        return value;
    }

    /**
     * Tests if a value is not negative.
     *
     * @param value        the  value tested to see if it is not negative.
     * @param errorMessage the errorMessage
     * @return the value
     * @throws java.lang.IllegalArgumentException if the value is negative.
     */
    public static int checkNotNegative(int value, String errorMessage) {
        if (value < 0) {
            throw new IllegalArgumentException(errorMessage);
        }
        return value;
    }

    /**
     * Tests if a long value is not negative.
     *
     * @param value        the long value tested to see if it is not negative.
     * @param errorMessage the message
     * @return the value
     * @throws java.lang.IllegalArgumentException if the value is negative.
     */
    public static long checkNegative(long value, String errorMessage) {
        if (value >= 0) {
            throw new IllegalArgumentException(errorMessage);
        }
        return value;
    }

    /**
     * Tests if a value is positive; so larger than 0.
     *
     * @param value        the value tested to see if it is positive.
     * @param errorMessage the message
     * @return the value
     * @throws java.lang.IllegalArgumentException if the value is not positive.
     */
    public static long checkPositive(long value, String errorMessage) {
        if (value <= 0) {
            throw new IllegalArgumentException(errorMessage);
        }
        return value;
    }

    /**
     * Tests if a value is positive; larger than 0.
     *
     * @param value        the value tested to see if it is positive.
     * @param errorMessage the message
     * @return the value
     * @throws java.lang.IllegalArgumentException if the value is not positive.
     */
    public static int checkPositive(int value, String errorMessage) {
        if (value <= 0) {
            throw new IllegalArgumentException(errorMessage);
        }
        return value;
    }


    /**
     * Tests whether the supplied object is an instance of the supplied class type.
     *
     * @param type    the expected type.
     * @param object  the object tested against the expected type.
     * @param errorMessage the errorMessage
     * @return the object argument.
     * @throws java.lang.IllegalArgumentException if the object is not an instance of the expected type.
     */
    public static <E> E checkInstanceOf(Class type, E object, String errorMessage) {
        isNotNull(type, "type");
        if (!type.isInstance(object)) {
            throw new IllegalArgumentException(errorMessage);
        }
        return object;
    }

    /**
     * Tests the supplied object to see if it is not a type of the supplied class.
     *
     * @param type    the type that is not of the supplied class.
     * @param object  the object tested against the type.
     * @param errorMessage the errorMessage
     * @return the object argument.
     * @throws java.lang.IllegalArgumentException if the object is an instance of the type that is not of the expected class.
     */
    public static <E> E checkNotInstanceOf(Class type, E object, String errorMessage) {
        isNotNull(type, "type");
        if (type.isInstance(object)) {
            throw new IllegalArgumentException(errorMessage);
        }
        return object;
    }

    /**
     * Tests whether the supplied expression is {@code false}.
     *
     * @param expression the expression tested to see if it is {@code false}.
     * @param errorMessage the errorMessage
     * @throws java.lang.IllegalArgumentException if the supplied expression is {@code true}.
     */
    public static void checkFalse(boolean expression, String errorMessage) {
        if (expression) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    /**
     * Tests whether the supplied expression is {@code true}.
     *
     * @param expression the expression tested to see if it is {@code true}.
     * @param errorMessage the errorMessage
     * @throws java.lang.IllegalArgumentException if the supplied expression is {@code false}.
     */
    public static void checkTrue(boolean expression, String errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(errorMessage);
        }
    }
}

