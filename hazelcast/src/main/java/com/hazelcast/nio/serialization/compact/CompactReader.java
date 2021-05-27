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


import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * All read(String fieldName) methods throw HazelcastSerializationException when the related field is not found or
 * there is a type mismatch.
 * To avoid exception when the field is not found, the user can make use of `read(String fieldName, T defaultValue)`.
 * Especially useful, when class is evolved  (a new field is added to/removed from the class).
 */
public interface CompactReader {

    byte readByte(@Nonnull String fieldName);

    byte readByte(@Nonnull String fieldName, byte defaultValue);

    short readShort(@Nonnull String fieldName);

    short readShort(@Nonnull String fieldName, short defaultValue);

    int readInt(@Nonnull String fieldName);

    int readInt(@Nonnull String fieldName, int defaultValue);

    long readLong(@Nonnull String fieldName);

    long readLong(@Nonnull String fieldName, long defaultValue);

    float readFloat(@Nonnull String fieldName);

    float readFloat(@Nonnull String fieldName, float defaultValue);

    double readDouble(@Nonnull String fieldName);

    double readDouble(@Nonnull String fieldName, double defaultValue);

    boolean readBoolean(@Nonnull String fieldName);

    boolean readBoolean(@Nonnull String fieldName, boolean defaultValue);

    char readChar(@Nonnull String fieldName);

    char readChar(@Nonnull String fieldName, char defaultValue);

    String readString(@Nonnull String fieldName);

    String readString(@Nonnull String fieldName, String defaultValue);

    BigDecimal readDecimal(@Nonnull String fieldName);

    BigDecimal readDecimal(@Nonnull String fieldName, BigDecimal defaultValue);

    LocalTime readTime(@Nonnull String fieldName);

    LocalTime readTime(@Nonnull String fieldName, LocalTime defaultValue);

    LocalDate readDate(@Nonnull String fieldName);

    LocalDate readDate(@Nonnull String fieldName, LocalDate defaultValue);

    LocalDateTime readTimestamp(@Nonnull String fieldName);

    LocalDateTime readTimestamp(@Nonnull String fieldName, LocalDateTime defaultValue);

    OffsetDateTime readTimestampWithTimezone(@Nonnull String fieldName);

    OffsetDateTime readTimestampWithTimezone(@Nonnull String fieldName, OffsetDateTime defaultValue);

    /**
     * @throws com.hazelcast.core.HazelcastException If the object is not able to be created because the related class not be
     *                                               found in the classpath
     */
    <T> T readObject(@Nonnull String fieldName);

    <T> T readObject(@Nonnull String fieldName, T defaultValue);

    byte[] readByteArray(@Nonnull String fieldName);

    byte[] readByteArray(@Nonnull String fieldName, byte[] defaultValue);

    boolean[] readBooleanArray(@Nonnull String fieldName);

    boolean[] readBooleanArray(@Nonnull String fieldName, boolean[] defaultValue);

    char[] readCharArray(@Nonnull String fieldName);

    char[] readCharArray(@Nonnull String fieldName, char[] defaultValue);

    int[] readIntArray(@Nonnull String fieldName);

    int[] readIntArray(@Nonnull String fieldName, int[] defaultValue);

    long[] readLongArray(@Nonnull String fieldName);

    long[] readLongArray(@Nonnull String fieldName, long[] defaultValue);

    double[] readDoubleArray(@Nonnull String fieldName);

    double[] readDoubleArray(@Nonnull String fieldName, double[] defaultValue);

    float[] readFloatArray(@Nonnull String fieldName);

    float[] readFloatArray(@Nonnull String fieldName, float[] defaultValue);

    short[] readShortArray(@Nonnull String fieldName);

    short[] readShortArray(@Nonnull String fieldName, short[] defaultValue);

    String[] readStringArray(@Nonnull String fieldName);

    String[] readStringArray(@Nonnull String fieldName, String[] defaultValue);

    BigDecimal[] readDecimalArray(@Nonnull String fieldName);

    BigDecimal[] readDecimalArray(@Nonnull String fieldName, BigDecimal[] defaultValue);

    LocalTime[] readTimeArray(@Nonnull String fieldName);

    LocalTime[] readTimeArray(@Nonnull String fieldName, LocalTime[] defaultValue);

    LocalDate[] readDateArray(@Nonnull String fieldName);

    LocalDate[] readDateArray(@Nonnull String fieldName, LocalDate[] defaultValue);

    LocalDateTime[] readTimestampArray(@Nonnull String fieldName);

    LocalDateTime[] readTimestampArray(@Nonnull String fieldName, LocalDateTime[] defaultValue);

    OffsetDateTime[] readTimestampWithTimezoneArray(@Nonnull String fieldName);

    OffsetDateTime[] readTimestampWithTimezoneArray(@Nonnull String fieldName, OffsetDateTime[] defaultValue);

    <T> T[] readObjectArray(@Nonnull String fieldName, Class<T> componentType);

    <T> T[] readObjectArray(@Nonnull String fieldName, Class<T> componentType, T[] defaultValue);

}
