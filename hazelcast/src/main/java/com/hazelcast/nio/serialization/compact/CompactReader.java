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

import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
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
     * Reads an 8-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    byte readByte(@Nonnull String fieldName);

    /**
     * Reads an unsigned byte.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    int readUnsignedByte(@Nonnull String fieldName);

    /**
     * Reads an 8-bit two's complement signed integer or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    byte readByte(@Nonnull String fieldName, byte defaultValue);

    /**
     * Reads an unsigned integer or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    int readUnsignedByte(@Nonnull String fieldName, int defaultValue);

    /**
     * Reads a 16-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    short readShort(@Nonnull String fieldName);

    /**
     * Reads a 16-bit unsigned integer.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    int readUnsignedShort(@Nonnull String fieldName);

    /**
     * Reads a 16-bit two's complement signed integer or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    short readShort(@Nonnull String fieldName, short defaultValue);

    /**
     * Reads a 16-bit unsigned integer or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    int readUnsignedShort(@Nonnull String fieldName, int defaultValue);

    /**
     * Reads a 32-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    int readInt(@Nonnull String fieldName);

    /**
     * Reads a 32-bit unsigned integer.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    long readUnsignedInt(@Nonnull String fieldName);

    /**
     * Reads a 32-bit two's complement signed integer or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    int readInt(@Nonnull String fieldName, int defaultValue);

    /**
     * Reads a 32-bit unsigned integer or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    long readUnsignedInt(@Nonnull String fieldName, long defaultValue);

    /**
     * Reads a 64-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    long readLong(@Nonnull String fieldName);

    /**
     * Reads a 64-bit unsigned integer.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    BigInteger readUnsignedLong(@Nonnull String fieldName);

    /**
     * Reads a 64-bit two's complement signed integer or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    long readLong(@Nonnull String fieldName, long defaultValue);

    /**
     * Reads a 64-bit unsigned integer or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    BigInteger readUnsignedLong(@Nonnull String fieldName, BigInteger defaultValue);

    /**
     * Reads a 32-bit IEEE 754 floating point number.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    float readFloat(@Nonnull String fieldName);

    /**
     * Reads a 32-bit IEEE 754 floating point number or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    float readFloat(@Nonnull String fieldName, float defaultValue);

    /**
     * Reads a 64-bit IEEE 754 floating point number.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    double readDouble(@Nonnull String fieldName);

    /**
     * Reads a 64-bit IEEE 754 floating point number or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    double readDouble(@Nonnull String fieldName, double defaultValue);

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
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    boolean readBoolean(@Nonnull String fieldName, boolean defaultValue);

    /**
     * Reads a 16-bit unsigned integer.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    char readChar(@Nonnull String fieldName);

    /**
     * Reads a 16-bit unsigned integer or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    char readChar(@Nonnull String fieldName, char defaultValue);

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
     * @param fieldName name of the field.
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
     * @param fieldName name of the field.
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
    @Nonnull
    LocalTime readTime(@Nonnull String fieldName);

    /**
     * Reads a time consisting of hour, minute, second, and nano seconds
     * or returns the default value.
     * <p>
     * This method may only return {@code null}, if the {@code defaultValue}
     * is {@code null}.
     *
     * @param fieldName name of the field.
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
    @Nonnull
    LocalDate readDate(@Nonnull String fieldName);

    /**
     * Reads a date consisting of year, month, and day or returns the default value.
     * <p>
     * This method may only return {@code null}, if the {@code defaultValue}
     * is {@code null}.
     *
     * @param fieldName name of the field.
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
    @Nonnull
    LocalDateTime readTimestamp(@Nonnull String fieldName);

    /**
     * Reads a timestamp consisting of date and time or returns the default value.
     * <p>
     * This method may only return {@code null}, if the {@code defaultValue}
     * is {@code null}.
     *
     * @param fieldName name of the field.
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
    @Nonnull
    OffsetDateTime readTimestampWithTimezone(@Nonnull String fieldName);

    /**
     * Reads a timestamp with timezone consisting of date, time and timezone offset
     * or returns the default value.
     * <p>
     * This method may only return {@code null}, if the {@code defaultValue}
     * is {@code null}.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    OffsetDateTime readTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime defaultValue);

    /**
     * Reads an arbitrary object.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     * @throws com.hazelcast.core.HazelcastException if the object cannot be created.
     */
    @Nullable
    <T> T readObject(@Nonnull String fieldName);

    /**
     * Reads an arbitrary object or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     * @throws com.hazelcast.core.HazelcastException if the object cannot be created.
     */
    @Nullable
    <T> T readObject(@Nonnull String fieldName, @Nullable T defaultValue);

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
    byte[] readByteArray(@Nonnull String fieldName);

    /**
     * Reads an array of 8-bit unsigned integers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    int[] readUnsignedByteArray(@Nonnull String fieldName);

    /**
     * Reads an array of 8-bit two's complement signed integers
     * or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    byte[] readByteArray(@Nonnull String fieldName, @Nullable byte[] defaultValue);

    /**
     * Reads an array of 8-bit unsigned integers
     * or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    int[] readUnsignedByteArray(@Nonnull String fieldName, @Nullable int[] defaultValue);

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
    boolean[] readBooleanArray(@Nonnull String fieldName);

    /**
     * Reads an array of booleans or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    boolean[] readBooleanArray(@Nonnull String fieldName, @Nullable boolean[] defaultValue);

    /**
     * Reads an array of 16-bit unsigned integers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    char[] readCharArray(@Nonnull String fieldName);

    /**
     * Reads an array of 16-bit unsigned integers or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    char[] readCharArray(@Nonnull String fieldName, @Nullable char[] defaultValue);

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
    int[] readIntArray(@Nonnull String fieldName);

    /**
     * Reads an array of 32-bit unsigned integers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    long[] readUnsignedIntArray(@Nonnull String fieldName);

    /**
     * Reads an array of 32-bit two's complement signed integers
     * or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    int[] readIntArray(@Nonnull String fieldName, @Nullable int[] defaultValue);

    /**
     * Reads an array of 32-bit unsigned integers
     * or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    long[] readUnsignedIntArray(@Nonnull String fieldName, @Nullable long[] defaultValue);

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
    long[] readLongArray(@Nonnull String fieldName);

    /**
     * Reads an array of 64-bit unsigned integers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    BigInteger[] readUnsignedLongArray(@Nonnull String fieldName);

    /**
     * Reads an array of 64-bit two's complement signed integers
     * or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    long[] readLongArray(@Nonnull String fieldName, @Nullable long[] defaultValue);

    /**
     * Reads an array of 64-bit unsigned integers
     * or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    BigInteger[] readLongArray(@Nonnull String fieldName, @Nullable BigInteger[] defaultValue);

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
    double[] readDoubleArray(@Nonnull String fieldName);

    /**
     * Reads an array of 64-bit IEEE 754 floating point numbers
     * or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    double[] readDoubleArray(@Nonnull String fieldName, @Nullable double[] defaultValue);

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
    float[] readFloatArray(@Nonnull String fieldName);

    /**
     * Reads an array of 32-bit IEEE 754 floating point numbers
     * or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    float[] readFloatArray(@Nonnull String fieldName, @Nullable float[] defaultValue);

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
    short[] readShortArray(@Nonnull String fieldName);

    /**
     * Reads an array of 16-bit unsigned integers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    int[] readUnsignedShortArray(@Nonnull String fieldName);

    /**
     * Reads an array of 16-bit two's complement signed integers
     * or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    short[] readShortArray(@Nonnull String fieldName, @Nullable short[] defaultValue);

    /**
     * Reads an array of 16-bit unsigned integers
     * or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    int[] readUnsignedShortArray(@Nonnull String fieldName, @Nullable int[] defaultValue);

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
    String[] readStringArray(@Nonnull String fieldName);

    /**
     * Reads an array of UTF-8 encoded strings or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    String[] readStringArray(@Nonnull String fieldName, @Nullable String[] defaultValue);

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
    BigDecimal[] readDecimalArray(@Nonnull String fieldName);

    /**
     * Reads an array of arbitrary precision and scale floating point numbers
     * or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    BigDecimal[] readDecimalArray(@Nonnull String fieldName, @Nullable BigDecimal[] defaultValue);

    /**
     * Reads an array of times consisting of hour, minute, second, and nano seconds.
     *
     * @param fieldName name of the field.
     * @return the value of the field. The items in the array cannot be null.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    LocalTime[] readTimeArray(@Nonnull String fieldName);

    /**
     * Reads an array of times consisting of hour, minute, second, and nano seconds
     * or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field. The items in the array
     * cannot be null except for the items in the provided {@code defaultValue}.
     */
    @Nullable
    LocalTime[] readTimeArray(@Nonnull String fieldName, @Nullable LocalTime[] defaultValue);

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
    LocalDate[] readDateArray(@Nonnull String fieldName);

    /**
     * Reads an array of dates consisting of year, month, and day
     * or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field. The items in the array
     * cannot be null except for the items in the provided {@code defaultValue}.
     */
    @Nullable
    LocalDate[] readDateArray(@Nonnull String fieldName, @Nullable LocalDate[] defaultValue);

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
    LocalDateTime[] readTimestampArray(@Nonnull String fieldName);

    /**
     * Reads an array of timestamps consisting of date and time
     * or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field. The items in the array
     * cannot be null except for the items in the provided {@code defaultValue}.
     */
    @Nullable
    LocalDateTime[] readTimestampArray(@Nonnull String fieldName, @Nullable LocalDateTime[] defaultValue);

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
    OffsetDateTime[] readTimestampWithTimezoneArray(@Nonnull String fieldName);

    /**
     * Reads an array of timestamps with timezone consisting of date, time and timezone offset
     * or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field. The items in the array
     * cannot be null except for the items in the provided {@code defaultValue}.
     */
    @Nullable
    OffsetDateTime[] readTimestampWithTimezoneArray(@Nonnull String fieldName, @Nullable OffsetDateTime[] defaultValue);

    /**
     * Reads an array of arbitrary objects.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in the schema
     *                                         or the type of the field does not match
     *                                         with the one defined in the schema.
     */
    @Nullable
    <T> T[] readObjectArray(@Nonnull String fieldName, @Nullable Class<T> componentType);

    /**
     * Reads an array of arbitrary objects or returns the default value.
     *
     * @param fieldName name of the field.
     * @param defaultValue default value to return if the field with the given name
     *                     does not exist in the schema or the type of the field does
     *                     not match with the one defined in the schema.
     * @return the value or the default value of the field.
     */
    @Nullable
    <T> T[] readObjectArray(@Nonnull String fieldName, @Nullable Class<T> componentType, T[] defaultValue);
}
