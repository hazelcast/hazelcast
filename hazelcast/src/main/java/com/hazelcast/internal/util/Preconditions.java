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

import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.hazelcast.internal.partition.InternalPartition.MAX_BACKUP_COUNT;
import static java.lang.String.format;

/**
 * A utility class for validating arguments and state.
 */
public final class Preconditions {

    private Preconditions() {
    }

    /**
     * Tests if a string contains text.
     *
     * @param argument     the string tested to see if it contains text.
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
     * Tests if an argument is not null.
     *
     * @param argument     the argument tested to see if it is not null.
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
     * Tests if the elements inside the argument collection are not null.
     * If collection is null or empty the test is ignored.
     *
     * @param argument     the iterable tested to see if it does not contain null elements; may be null or empty
     * @param errorMessage the errorMessage
     * @return the argument that was tested.
     * @throws java.lang.NullPointerException if argument contains a null element inside
     */
    public static <T> Iterable<T> checkNoNullInside(Iterable<T> argument, String errorMessage) {
        if (argument == null) {
            return argument;
        }
        for (T element : argument) {
            checkNotNull(element, errorMessage);
        }
        return argument;
    }

    /**
     * Tests if an argument is not null.
     *
     * @param argument the argument tested to see if it is not null.
     * @return the argument that was tested.
     * @throws java.lang.NullPointerException if argument is null
     */
    public static <T> T checkNotNull(T argument) {
        if (argument == null) {
            throw new NullPointerException();
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
     * Tests if the {@code value} is &gt;= 0.
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
     * Tests if the {@code value} is &gt;= 0.
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
     * Tests if the {@code value} is &lt; 0.
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
     * Tests if a {@code value} is positive, that is strictly larger than 0 (value &gt; 0).
     *
     * @param paramName the name of the checked parameter that will be in exception message
     * @param value     the value tested to see if it is positive.
     * @return the value
     * @throws java.lang.IllegalArgumentException if the value is not positive.
     */
    public static long checkPositive(String paramName, long value) {
        if (value <= 0) {
            throw new IllegalArgumentException(paramName + " is " + value + " but must be > 0");
        }
        return value;
    }

    /**
     * Tests if a {@code value} is positive, that is strictly larger than 0 (value &gt; 0).
     *
     * @param value        the value tested to see if it is positive.
     * @param errorMessage the message
     * @return the value
     * @throws java.lang.IllegalArgumentException if the value is not positive.
     */
    public static double checkPositive(double value, String errorMessage) {
        if (value <= 0) {
            throw new IllegalArgumentException(errorMessage);
        }
        return value;
    }

    /**
     * Tests if a {@code value} is positive, that is strictly larger than 0 (value &gt; 0).
     *
     * @param paramName the the name of the checked parameter that will be in exception message
     * @param value     the value tested to see if it is positive.
     * @return the value
     * @throws java.lang.IllegalArgumentException if the value is not positive.
     */
    public static int checkPositive(String paramName, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException(paramName + " is " + value + " but must be > 0");
        }
        return value;
    }

    /**
     * Tests if the newBackupCount count is valid.
     *
     * @param newBackupCount          the number of sync backups
     * @param currentAsyncBackupCount the current number of async backups
     * @return the newBackupCount
     * @throws java.lang.IllegalArgumentException if newBackupCount is smaller than 0, or larger than the maximum
     *                                            number of backups.
     */
    public static int checkBackupCount(int newBackupCount, int currentAsyncBackupCount) {
        if (newBackupCount < 0) {
            throw new IllegalArgumentException("backup-count can't be smaller than 0");
        }

        if (currentAsyncBackupCount < 0) {
            throw new IllegalArgumentException("async-backup-count can't be smaller than 0");
        }

        if (newBackupCount > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("backup-count can't be larger than than " + MAX_BACKUP_COUNT);
        }

        if (newBackupCount + currentAsyncBackupCount > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("the sum of backup-count and async-backup-count can't be larger than than "
                    + MAX_BACKUP_COUNT);
        }

        return newBackupCount;
    }

    /**
     * Tests if the newAsyncBackupCount count is valid.
     *
     * @param currentBackupCount  the current number of backups
     * @param newAsyncBackupCount the new number of async backups
     * @return the newAsyncBackupCount
     * @throws java.lang.IllegalArgumentException if asyncBackupCount is smaller than 0, or larger than the maximum
     *                                            number of backups.
     */
    public static int checkAsyncBackupCount(int currentBackupCount, int newAsyncBackupCount) {
        if (currentBackupCount < 0) {
            throw new IllegalArgumentException("backup-count can't be smaller than 0");
        }

        if (newAsyncBackupCount < 0) {
            throw new IllegalArgumentException("async-backup-count can't be smaller than 0");
        }

        if (newAsyncBackupCount > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("async-backup-count can't be larger than than " + MAX_BACKUP_COUNT);
        }

        if (currentBackupCount + newAsyncBackupCount > MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("the sum of backup-count and async-backup-count can't be larger than than "
                    + MAX_BACKUP_COUNT);
        }

        return newAsyncBackupCount;
    }

    /**
     * Tests whether the supplied object is an instance of the supplied class type.
     *
     * @param type         the expected type.
     * @param object       the object tested against the expected type.
     * @param errorMessage the errorMessage
     * @return the object argument.
     * @throws java.lang.IllegalArgumentException if the object is not an instance of the expected type.
     */
    public static <E> E checkInstanceOf(Class<E> type, Object object, String errorMessage) {
        isNotNull(type, "type");
        if (!type.isInstance(object)) {
            throw new IllegalArgumentException(errorMessage);
        }
        return (E) object;
    }

    public static <E> E checkInstanceOf(Class<E> type, Object object) {
        isNotNull(type, "type");
        if (!type.isInstance(object)) {
            throw new IllegalArgumentException(object + " must be instanceof " + type.getName());
        }
        return (E) object;
    }

    /**
     * Tests the supplied object to see if it is not a type of the supplied class.
     *
     * @param type         the type that is not of the supplied class.
     * @param object       the object tested against the type.
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
     * @param expression   the expression tested to see if it is {@code false}.
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
     * @param expression   the expression tested to see if it is {@code true}.
     * @param errorMessage the errorMessage
     * @throws java.lang.IllegalArgumentException if the supplied expression is {@code false}.
     */
    public static void checkTrue(boolean expression, String errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    /**
     * Tests whether the supplied expression is {@code true}.
     *
     * @param expression   the expression tested to see if it is {@code true}.
     * @param errorMessage the exception message.
     * @throws java.lang.UnsupportedOperationException if the supplied expression is {@code false}.
     */
    public static void checkTrueUnsupportedOperation(boolean expression, String errorMessage) {
        if (!expression) {
            throw new UnsupportedOperationException(errorMessage);
        }
    }

    /**
     * Check if iterator has next element. If not throw NoSuchElementException
     *
     * @param iterator
     * @param message
     * @return the iterator itself
     * @throws java.util.NoSuchElementException if iterator.hasNext returns false
     */
    public static <T> Iterator<T> checkHasNext(Iterator<T> iterator, String message) throws NoSuchElementException {
        if (!iterator.hasNext()) {
            throw new NoSuchElementException(message);
        }
        return iterator;
    }

    /**
     * Check the state of a condition
     *
     * @param condition
     * @param message
     * @throws IllegalStateException if condition if false
     */
    public static void checkState(boolean condition, String message) throws IllegalStateException {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
