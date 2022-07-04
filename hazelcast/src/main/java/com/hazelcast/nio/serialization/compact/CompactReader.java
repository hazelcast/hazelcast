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

package com.hazelcast.nio.serialization.compact;

import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Provides means of reading compact serialized fields from the binary data.
 * <p>
 * Read operations might throw {@link HazelcastSerializationException}
 * when a field with the given name is not found or there is a type mismatch. On such
 * occasions, one might provide default values to the read methods to return it in case
 * of the failure scenarios described above. Providing default values might be especially
 * useful, if the class might evolve in future, either by adding or removing fields.
 *
 * @since Hazelcast 5.0 as BETA
 */
@Beta
public interface CompactReader {

    /**
     * Reads a boolean.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    boolean readBoolean(@Nonnull String fieldName);

    /**
     * Reads a boolean or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    boolean readBoolean(@Nonnull String fieldName, boolean defaultValue);

    /**
     * Reads an 8-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    byte readInt8(@Nonnull String fieldName);

    /**
     * Reads an 8-bit two's complement signed integer or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    byte readInt8(@Nonnull String fieldName, byte defaultValue);

    /**
     * Reads a 16-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    short readInt16(@Nonnull String fieldName);

    /**
     * Reads a 16-bit two's complement signed integer or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    short readInt16(@Nonnull String fieldName, short defaultValue);

    /**
     * Reads a 32-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    int readInt32(@Nonnull String fieldName);

    /**
     * Reads a 32-bit two's complement signed integer or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    int readInt32(@Nonnull String fieldName, int defaultValue);

    /**
     * Reads a 64-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    long readInt64(@Nonnull String fieldName);

    /**
     * Reads a 64-bit two's complement signed integer or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    long readInt64(@Nonnull String fieldName, long defaultValue);

    /**
     * Reads a 32-bit IEEE 754 floating point number.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    float readFloat32(@Nonnull String fieldName);

    /**
     * Reads a 32-bit IEEE 754 floating point number or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    float readFloat32(@Nonnull String fieldName, float defaultValue);

    /**
     * Reads a 64-bit IEEE 754 floating point number.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    double readFloat64(@Nonnull String fieldName);

    /**
     * Reads a 64-bit IEEE 754 floating point number or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    double readFloat64(@Nonnull String fieldName, double defaultValue);

    /**
     * Reads an UTF-8 encoded string.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    String readString(@Nonnull String fieldName);

    /**
     * Reads an UTF-8 encoded string or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    String readString(@Nonnull String fieldName, @Nullable String defaultValue);

    /**
     * Reads an arbitrary precision and scale floating point number.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    BigDecimal readDecimal(@Nonnull String fieldName);

    /**
     * Reads an arbitrary precision and scale floating point number
     * or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    BigDecimal readDecimal(@Nonnull String fieldName, @Nullable BigDecimal defaultValue);

    /**
     * Reads a time consisting of hour, minute, second, and nano seconds.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    LocalTime readTime(@Nonnull String fieldName);

    /**
     * Reads a time consisting of hour, minute, second, and nano seconds
     * or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    LocalTime readTime(@Nonnull String fieldName, @Nullable LocalTime defaultValue);

    /**
     * Reads a date consisting of year, month, and day.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    LocalDate readDate(@Nonnull String fieldName);

    /**
     * Reads a date consisting of year, month, and day or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    LocalDate readDate(@Nonnull String fieldName, @Nullable LocalDate defaultValue);

    /**
     * Reads a timestamp consisting of date and time.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    LocalDateTime readTimestamp(@Nonnull String fieldName);

    /**
     * Reads a timestamp consisting of date and time or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    LocalDateTime readTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime defaultValue);

    /**
     * Reads a timestamp with timezone consisting of date, time and timezone offset.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    OffsetDateTime readTimestampWithTimezone(@Nonnull String fieldName);

    /**
     * Reads a timestamp with timezone consisting of date, time and timezone offset
     * or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    OffsetDateTime readTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime defaultValue);

    /**
     * Reads a compact object
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException       if the field does not exist in the schema
     *                                               or the type of the field does not match
     *                                               with the one defined in the schema.
     * @throws com.hazelcast.core.HazelcastException if the object cannot be created.
     */
    @Nullable
    <T> T readCompact(@Nonnull String fieldName);

    /**
     * Reads a compact object
     * or returns the default value
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    <T> T readCompact(@Nonnull String fieldName, @Nullable T defaultValue);

    /**
     * Reads an array of booleans.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    boolean[] readArrayOfBoolean(@Nonnull String fieldName);

    /**
     * Reads an array of booleans or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    boolean[] readArrayOfBoolean(@Nonnull String fieldName, @Nullable boolean[] defaultValue);

    /**
     * Reads an array of 8-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    byte[] readArrayOfInt8(@Nonnull String fieldName);

    /**
     * Reads an array of 8-bit two's complement signed integers or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    byte[] readArrayOfInt8(@Nonnull String fieldName, @Nullable byte[] defaultValue);

    /**
     * Reads an array of 16-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    short[] readArrayOfInt16(@Nonnull String fieldName);

    /**
     * Reads an array of 16-bit two's complement signed integers or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    short[] readArrayOfInt16(@Nonnull String fieldName, @Nullable short[] defaultValue);

    /**
     * Reads an array of 32-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    int[] readArrayOfInt32(@Nonnull String fieldName);

    /**
     * Reads an array of 32-bit two's complement signed integers or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    int[] readArrayOfInt32(@Nonnull String fieldName, @Nullable int[] defaultValue);

    /**
     * Reads an array of 64-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    long[] readArrayOfInt64(@Nonnull String fieldName);

    /**
     * Reads an array of 64-bit two's complement signed integers or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    long[] readArrayOfInt64(@Nonnull String fieldName, @Nullable long[] defaultValue);

    /**
     * Reads an array of 32-bit IEEE 754 floating point numbers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    float[] readArrayOfFloat32(@Nonnull String fieldName);

    /**
     * Reads an array of 32-bit IEEE 754 floating point numbers or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    float[] readArrayOfFloat32(@Nonnull String fieldName, @Nullable float[] defaultValue);

    /**
     * Reads an array of 64-bit IEEE 754 floating point numbers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    double[] readArrayOfFloat64(@Nonnull String fieldName);

    /**
     * Reads an array of 64-bit IEEE 754 floating point numbers or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    double[] readArrayOfFloat64(@Nonnull String fieldName, @Nullable double[] defaultValue);

    /**
     * Reads an array of UTF-8 encoded strings.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    String[] readArrayOfString(@Nonnull String fieldName);

    /**
     * Reads an array of UTF-8 encoded strings or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    String[] readArrayOfString(@Nonnull String fieldName, @Nullable String[] defaultValue);

    /**
     * Reads an array of arbitrary precision and scale floating point numbers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    BigDecimal[] readArrayOfDecimal(@Nonnull String fieldName);

    /**
     * Reads an array of arbitrary precision and scale floating point numbers or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    BigDecimal[] readArrayOfDecimal(@Nonnull String fieldName, @Nullable BigDecimal[] defaultValue);

    /**
     * Reads an array of times consisting of hour, minute, second, and nanoseconds
     *
     * @param fieldName name of the field.
     * @return the value of the field. The items in the array cannot be null.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    LocalTime[] readArrayOfTime(@Nonnull String fieldName);

    /**
     * Reads an array of times consisting of hour, minute, second, and nanoseconds or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    LocalTime[] readArrayOfTime(@Nonnull String fieldName, @Nullable LocalTime[] defaultValue);

    /**
     * Reads an array of dates consisting of year, month, and day.
     *
     * @param fieldName name of the field.
     * @return the value of the field. The items in the array cannot be null.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    LocalDate[] readArrayOfDate(@Nonnull String fieldName);

    /**
     * Reads an array of dates consisting of year, month, and day or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    LocalDate[] readArrayOfDate(@Nonnull String fieldName, @Nullable LocalDate[] defaultValue);

    /**
     * Reads an array of timestamps consisting of date and time.
     *
     * @param fieldName name of the field.
     * @return the value of the field. The items in the array cannot be null.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    LocalDateTime[] readArrayOfTimestamp(@Nonnull String fieldName);

    /**
     * Reads an array of timestamps consisting of date and time or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    LocalDateTime[] readArrayOfTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime[] defaultValue);

    /**
     * Reads an array of timestamps with timezone consisting of date, time and timezone offset.
     *
     * @param fieldName name of the field.
     * @return the value of the field. The items in the array cannot be null.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    OffsetDateTime[] readArrayOfTimestampWithTimezone(@Nonnull String fieldName);

    /**
     * Reads an array of timestamps with timezone consisting of date, time and timezone offset or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    OffsetDateTime[] readArrayOfTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime[] defaultValue);

    /**
     * Reads an array of compact objects.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    <T> T[] readArrayOfCompact(@Nonnull String fieldName, @Nullable Class<T> componentType);

    /**
     * Reads an array of compact objects or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    <T> T[] readArrayOfCompact(@Nonnull String fieldName, @Nullable Class<T> componentType, @Nullable T[] defaultValue);

    /**
     * Reads a nullable boolean.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    Boolean readNullableBoolean(@Nonnull String fieldName);

    /**
     * Reads a nullable boolean or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    Boolean readNullableBoolean(@Nonnull String fieldName, @Nullable Boolean defaultValue);

    /**
     * Reads a nullable 8-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    Byte readNullableInt8(@Nonnull String fieldName);

    /**
     * Reads a nullable 8-bit two's complement signed integer or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    Byte readNullableInt8(@Nonnull String fieldName, @Nullable Byte defaultValue);

    /**
     * Reads a nullable 16-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    Short readNullableInt16(@Nonnull String fieldName);

    /**
     * Reads a nullable 16-bit two's complement signed integer or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    Short readNullableInt16(@Nonnull String fieldName, @Nullable Short defaultValue);

    /**
     * Reads a nullable 32-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    Integer readNullableInt32(@Nonnull String fieldName);

    /**
     * Reads a nullable 32-bit two's complement signed integer or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    Integer readNullableInt32(@Nonnull String fieldName, @Nullable Integer defaultValue);

    /**
     * Reads a nullable 64-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    Long readNullableInt64(@Nonnull String fieldName);

    /**
     * Reads a nullable 64-bit two's complement signed integer or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    Long readNullableInt64(@Nonnull String fieldName, @Nullable Long defaultValue);

    /**
     * Reads a nullable 32-bit IEEE 754 floating point number.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    Float readNullableFloat32(@Nonnull String fieldName);

    /**
     * Reads a nullable 32-bit IEEE 754 floating point number or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    Float readNullableFloat32(@Nonnull String fieldName, @Nullable Float defaultValue);

    /**
     * Reads a nullable 64-bit IEEE 754 floating point number.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    Double readNullableFloat64(@Nonnull String fieldName);

    /**
     * Reads a nullable 64-bit IEEE 754 floating point number or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    Double readNullableFloat64(@Nonnull String fieldName, @Nullable Double defaultValue);

    /**
     * Reads a nullable array of nullable booleans.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    Boolean[] readArrayOfNullableBoolean(@Nonnull String fieldName);

    /**
     * Reads a nullable array of nullable booleans or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    Boolean[] readArrayOfNullableBoolean(@Nonnull String fieldName, @Nullable Boolean[] defaultValue);

    /**
     * Reads a nullable array of nullable 8-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    Byte[] readArrayOfNullableInt8(@Nonnull String fieldName);

    /**
     * Reads a nullable array of nullable 8-bit two's complement signed integers or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    Byte[] readArrayOfNullableInt8(@Nonnull String fieldName, @Nullable Byte[] defaultValue);

    /**
     * Reads a nullable array of nullable 16-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    Short[] readArrayOfNullableInt16(@Nonnull String fieldName);

    /**
     * Reads a nullable array of nullable 16-bit two's complement signed integers or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    Short[] readArrayOfNullableInt16(@Nonnull String fieldName, @Nullable Short[] defaultValue);

    /**
     * Reads a nullable array of nullable 32-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    Integer[] readArrayOfNullableInt32(@Nonnull String fieldName);

    /**
     * Reads a nullable array of nullable 32-bit two's complement signed integers or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    Integer[] readArrayOfNullableInt32(@Nonnull String fieldName, @Nullable Integer[] defaultValue);

    /**
     * Reads a nullable array of nullable 64-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    Long[] readArrayOfNullableInt64(@Nonnull String fieldName);

    /**
     * Reads a nullable array of nullable 64-bit two's complement signed integers or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    Long[] readArrayOfNullableInt64(@Nonnull String fieldName, @Nullable Long[] defaultValue);

    /**
     * Reads a nullable array of nullable 32-bit IEEE 754 floating point numbers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    Float[] readArrayOfNullableFloat32(@Nonnull String fieldName);

    /**
     * Reads a nullable array of nullable 32-bit IEEE 754 floating point numbers or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    Float[] readArrayOfNullableFloat32(@Nonnull String fieldName, @Nullable Float[] defaultValue);

    /**
     * Reads a nullable array of nullable 64-bit IEEE 754 floating point numbers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    Double[] readArrayOfNullableFloat64(@Nonnull String fieldName);

    /**
     * Reads a nullable array of nullable 64-bit IEEE 754 floating point numbers or returns the default value.
     *
     * @param fieldName    name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    Double[] readArrayOfNullableFloat64(@Nonnull String fieldName, @Nullable Double[] defaultValue);
}
