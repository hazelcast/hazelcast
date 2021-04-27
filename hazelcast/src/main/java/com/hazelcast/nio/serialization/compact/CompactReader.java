/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization.compact;


import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.function.Function;

/**
 * All read(String fieldName) methods throw HazelcastSerializationException when the related field is not found or
 * there is a type mismatch.
 * To avoid exception when the field is not found, the user can make use of `read(String fieldName, T defaultValue)`.
 * Especially useful, when class is evolved  (a new field is added to/removed from the class).
 */
public interface CompactReader {

    byte getByte(String fieldName);

    byte getByte(String fieldName, byte defaultValue);

    short getShort(String fieldName);

    short getShort(String fieldName, short defaultValue);

    int getInt(String fieldName);

    int getInt(String fieldName, int defaultValue);

    long getLong(String fieldName);

    long getLong(String fieldName, long defaultValue);

    float getFloat(String fieldName);

    float getFloat(String fieldName, float defaultValue);

    double getDouble(String fieldName);

    double getDouble(String fieldName, double defaultValue);

    boolean getBoolean(String fieldName);

    boolean getBoolean(String fieldName, boolean defaultValue);

    char getChar(String fieldName);

    char getChar(String fieldName, char defaultValue);

    String getString(String fieldName);

    String getString(String fieldName, String defaultValue);

    BigDecimal getDecimal(String fieldName);

    BigDecimal getDecimal(String fieldName, BigDecimal defaultValue);

    LocalTime getTime(String fieldName);

    LocalTime getTime(String fieldName, LocalTime defaultValue);

    LocalDate getDate(String fieldName);

    LocalDate getDate(String fieldName, LocalDate defaultValue);

    LocalDateTime getTimestamp(String fieldName);

    LocalDateTime getTimestamp(String fieldName, LocalDateTime defaultValue);

    OffsetDateTime getTimestampWithTimezone(String fieldName);

    OffsetDateTime getTimestampWithTimezone(String fieldName, OffsetDateTime defaultValue);

    /**
     * @throws com.hazelcast.core.HazelcastException If the object is not able to be created because the related class not be
     *                                               found in the classpath
     */
    <T> T getObject(String fieldName);

    <T> T getObject(String fieldName, T defaultValue);

    byte[] getByteArray(String fieldName);

    byte[] getByteArray(String fieldName, byte[] defaultValue);

    boolean[] getBooleanArray(String fieldName);

    boolean[] getBooleanArray(String fieldName, boolean[] defaultValue);

    char[] getCharArray(String fieldName);

    char[] getCharArray(String fieldName, char[] defaultValue);

    int[] getIntArray(String fieldName);

    int[] getIntArray(String fieldName, int[] defaultValue);

    long[] getLongArray(String fieldName);

    long[] getLongArray(String fieldName, long[] defaultValue);

    double[] getDoubleArray(String fieldName);

    double[] getDoubleArray(String fieldName, double[] defaultValue);

    float[] getFloatArray(String fieldName);

    float[] getFloatArray(String fieldName, float[] defaultValue);

    short[] getShortArray(String fieldName);

    short[] getShortArray(String fieldName, short[] defaultValue);

    String[] getStringArray(String fieldName);

    String[] getStringArray(String fieldName, String[] defaultValue);

    BigDecimal[] getDecimalArray(String fieldName);

    BigDecimal[] getDecimalArray(String fieldName, BigDecimal[] defaultValue);

    LocalTime[] getTimeArray(String fieldName);

    LocalTime[] getTimeArray(String fieldName, LocalTime[] defaultValue);

    LocalDate[] getDateArray(String fieldName);

    LocalDate[] getDateArray(String fieldName, LocalDate[] defaultValue);

    LocalDateTime[] getTimestampArray(String fieldName);

    LocalDateTime[] getTimestampArray(String fieldName, LocalDateTime[] defaultValue);

    OffsetDateTime[] getTimestampWithTimezoneArray(String fieldName);

    OffsetDateTime[] getTimestampWithTimezoneArray(String fieldName, OffsetDateTime[] defaultValue);

    /**
     * @throws com.hazelcast.core.HazelcastException If the object is not able to be created because the related class not be
     *                                               found in the classpath
     */
    <T> T[] getObjectArray(String fieldName, Class<T> componentType);

    <T> T[] getObjectArray(String fieldName, Class<T> componentType, T[] defaultValue);

    /**
     * @throws com.hazelcast.core.HazelcastException If the object is not able to be created because the related class not be
     *                                               found in the classpath
     */
    <T> Collection<T> getObjectCollection(String fieldName, Function<Integer, Collection<T>> constructor);

    <T> Collection<T> getObjectCollection(String fieldName, Function<Integer, Collection<T>> constructor,
                                          Collection<T> defaultValue);

}
