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

package com.hazelcast.jet.core.test;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Assert methods that throw exceptions similar to those thrown by JUnit.
 *
 * @since Jet 3.0
 */
public final class JetAssert {

    private JetAssert() {
    }

    /**
     * Asserts that the given condition is {@code true}. If not, an
     * {@link AssertionError} is thrown with the given message.
     */
    public static void assertTrue(@Nullable String message, boolean condition) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    /**
     * Asserts that the given condition is {@code false}. If not, an
     * {@link AssertionError} is thrown with the given message.
     */
    public static void assertFalse(@Nullable String message, boolean condition) {
        assertTrue(message, !condition);
    }

    /**
     * Asserts that the two given objects are the same, when compared using
     * the {@code ==} operator and if not, an {@link AssertionError} is thrown
     * with the given message.
     */
    public static void assertSame(@Nullable String message, @Nullable Object expected, @Nullable Object actual) {
        if (expected == actual) {
            return;
        }
        throwNotEqual(message, expected, actual);
    }

    /**
     * Asserts that the two given objects are equal, when compared using
     * {@link Object#equals(Object)}. If they are not equal, an
     * {@link AssertionError} is thrown with the given message.
     */
    public static void assertEquals(@Nullable String message, @Nullable Object expected, @Nullable Object actual) {
        if (Objects.equals(expected, actual)) {
            return;
        }
        throwNotEqual(message, expected, actual);
    }

    /**
     * Throws an {@link AssertionError} with the given message.
     */
    public static void fail(@Nullable String message) {
        throw new AssertionError(message);
    }

    private static void throwNotEqual(String message, Object expected, Object actual) {
        if (message != null && !message.equals("")) {
            message = message + " ";
        }
        String expectedString = String.valueOf(expected);
        String actualString = String.valueOf(actual);
        if (expectedString.equals(actualString)) {
            message = message + ", expected: "
                    + formatClassAndValue(expected, expectedString)
                    + " but was: " + formatClassAndValue(actual, actualString);
        } else {
            message = message + ", expected:<" + expectedString + "> but was:<"
                    + actualString + ">";
        }
        throw new AssertionError(message);
    }

    private static String formatClassAndValue(Object value, String valueString) {
        String className = value == null ? "null" : value.getClass().getName();
        return className + "<" + valueString + ">";
    }
}
