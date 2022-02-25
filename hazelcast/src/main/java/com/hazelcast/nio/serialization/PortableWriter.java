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

package com.hazelcast.nio.serialization;

import com.hazelcast.nio.ObjectDataOutput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Provides means for writing portable fields to binary data in the form of java primitives,
 * arrays of java primitives, nested portable fields and arrays of portable fields.
 */
public interface PortableWriter {

    /**
     * Writes a primitive int.
     *
     * @param fieldName name of the field
     * @param value     int value to be written
     * @throws IOException in case of any exceptional case
     */
    void writeInt(@Nonnull String fieldName, int value) throws IOException;

    /**
     * Writes a primitive long.
     *
     * @param fieldName name of the field
     * @param value     long value to be written
     * @throws IOException in case of any exceptional case
     */
    void writeLong(@Nonnull String fieldName, long value) throws IOException;

    /**
     * Writes an UTF string.
     *
     * @param fieldName name of the field
     * @param value     utf string value to be written
     * @throws IOException in case of any exceptional case
     * @deprecated for the sake of better naming. Use {@link #writeString(String, String)} instead.
     */
    @Deprecated
    void writeUTF(@Nonnull String fieldName, @Nullable String value) throws IOException;

    /**
     * Writes an UTF string.
     *
     * @param fieldName name of the field
     * @param value     utf string value to be written
     * @throws IOException in case of any exceptional case
     */
    void writeString(@Nonnull String fieldName, @Nullable String value) throws IOException;

    /**
     * Writes a primitive boolean.
     *
     * @param fieldName name of the field
     * @param value     int value to be written
     * @throws IOException in case of any exceptional case
     */
    void writeBoolean(@Nonnull String fieldName, boolean value) throws IOException;

    /**
     * Writes a primitive byte.
     *
     * @param fieldName name of the field
     * @param value     int value to be written
     * @throws IOException in case of any exceptional case
     */
    void writeByte(@Nonnull String fieldName, byte value) throws IOException;

    /**
     * Writes a primitive char.
     *
     * @param fieldName name of the field
     * @param value     int value to be written
     * @throws IOException in case of any exceptional case
     */
    void writeChar(@Nonnull String fieldName, int value) throws IOException;

    /**
     * Writes a primitive double.
     *
     * @param fieldName name of the field
     * @param value     int value to be written
     * @throws IOException in case of any exceptional case
     */
    void writeDouble(@Nonnull String fieldName, double value) throws IOException;

    /**
     * Writes a primitive float.
     *
     * @param fieldName name of the field
     * @param value     int value to be written
     * @throws IOException in case of any exceptional case
     */
    void writeFloat(@Nonnull String fieldName, float value) throws IOException;

    /**
     * Writes a primitive short.
     *
     * @param fieldName name of the field
     * @param value     int value to be written
     * @throws IOException in case of any exceptional case
     */
    void writeShort(@Nonnull String fieldName, short value) throws IOException;

    /**
     * Writes a Portable.
     * Use {@link #writeNullPortable(String, int, int)} to write a {@code null} Portable
     *
     * @param fieldName name of the field
     * @param portable  Portable to be written
     * @throws IOException in case of any exceptional case
     */
    void writePortable(@Nonnull String fieldName, @Nullable Portable portable) throws IOException;

    /**
     * To write a null portable value, user needs to provide class and factoryIds of related class.
     *
     * @param fieldName name of the field
     * @param factoryId factory ID of related portable class
     * @param classId   class ID of related portable class
     * @throws IOException in case of any exceptional case
     */
    void writeNullPortable(@Nonnull String fieldName, int factoryId, int classId) throws IOException;

    /**
     * Writes a decimal which is arbitrary precision and scale floating-point number
     *
     * @param fieldName name of the field
     * @param value     BigDecimal value to be written
     * @throws IOException in case of any exceptional case
     */
    void writeDecimal(@Nonnull String fieldName, @Nullable BigDecimal value) throws IOException;

    /**
     * Write a time field consisting of hour, minute, seconds and nanos parts
     *
     * @param fieldName name of the field
     * @param value     LocalTime value to be written
     * @throws IOException in case of any exceptional case
     */
    void writeTime(@Nonnull String fieldName, @Nullable LocalTime value) throws IOException;

    /**
     * Writes a date field consisting of year, month of the year and day of the month
     *
     * @param fieldName name of the field
     * @param value     LocalDate value to be written
     * @throws IOException in case of any exceptional case
     */
    void writeDate(@Nonnull String fieldName, @Nullable LocalDate value) throws IOException;

    /**
     * Writes a timestamp field consisting of
     * year, month of the year, day of the month, hour, minute, seconds, nanos parts
     *
     * @param fieldName name of the field
     * @param value     LocalDateTime value to be written
     * @throws IOException in case of any exceptional case
     */
    void writeTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime value) throws IOException;

    /**
     * Writes a timestamp with timezone field consisting of
     * year, month of the year, day of the month, offset seconds, hour, minute, seconds, nanos parts
     *
     * @param fieldName name of the field
     * @param value     OffsetDateTime value to be written
     * @throws IOException in case of any exceptional case
     */
    void writeTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime value) throws IOException;

    /**
     * Writes a primitive byte-array.
     *
     * @param fieldName name of the field
     * @param bytes     byte array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeByteArray(@Nonnull String fieldName, @Nullable byte[] bytes) throws IOException;

    /**
     * Writes a primitive boolean-array.
     *
     * @param fieldName name of the field
     * @param booleans  boolean array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeBooleanArray(@Nonnull String fieldName, @Nullable boolean[] booleans) throws IOException;

    /**
     * Writes a primitive char-array.
     *
     * @param fieldName name of the field
     * @param chars     char array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeCharArray(@Nonnull String fieldName, @Nullable char[] chars) throws IOException;

    /**
     * Writes a primitive int-array.
     *
     * @param fieldName name of the field
     * @param ints      int array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeIntArray(@Nonnull String fieldName, @Nullable int[] ints) throws IOException;

    /**
     * Writes a primitive long-array.
     *
     * @param fieldName name of the field
     * @param longs     long array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeLongArray(@Nonnull String fieldName, @Nullable long[] longs) throws IOException;

    /**
     * Writes a primitive double array.
     *
     * @param fieldName name of the field
     * @param values    double array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeDoubleArray(@Nonnull String fieldName, @Nullable double[] values) throws IOException;

    /**
     * Writes a primitive float array.
     *
     * @param fieldName name of the field
     * @param values    float array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeFloatArray(@Nonnull String fieldName, @Nullable float[] values) throws IOException;

    /**
     * Writes a primitive short-array.
     *
     * @param fieldName name of the field
     * @param values    short array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeShortArray(@Nonnull String fieldName, @Nullable short[] values) throws IOException;

    /**
     * Writes a String-array.
     *
     * @param fieldName name of the field
     * @param values    String array to be written
     * @throws IOException in case of any exceptional case
     * @deprecated for the sake of better naming. Use {@link #writeStringArray(String, String[])} instead.
     */
    @Deprecated
    void writeUTFArray(@Nonnull String fieldName, @Nullable String[] values) throws IOException;

    /**
     * Writes a String-array.
     *
     * @param fieldName name of the field
     * @param values    String array to be written
     * @throws IOException in case of any exceptional case
     */
    void writeStringArray(@Nonnull String fieldName, @Nullable String[] values) throws IOException;

    /**
     * Writes a an array of Portables.
     *
     * @param fieldName name of the field
     * @param values    portable array to be written
     * @throws IOException in case of any exceptional case
     */
    void writePortableArray(@Nonnull String fieldName, @Nullable Portable[] values) throws IOException;

    /**
     * Writes an array of Decimals
     *
     * @param fieldName name of the field
     * @param values    BigDecimal array to be written
     * @throws IOException in case of any exceptional case
     * @see #writeDecimal(String, BigDecimal)
     */
    void writeDecimalArray(@Nonnull String fieldName, @Nullable BigDecimal[] values) throws IOException;

    /**
     * Writes an array of Time's
     *
     * @param fieldName name of the field
     * @param values    LocalTime array to be written
     * @throws IOException in case of any exceptional case
     * @see #writeTime(String, LocalTime)
     */
    void writeTimeArray(@Nonnull String fieldName, @Nullable LocalTime[] values) throws IOException;

    /**
     * Writes an array of Date's
     *
     * @param fieldName name of the field
     * @param values    LocalDate array to be written
     * @throws IOException in case of any exceptional case
     * @see #writeDate(String, LocalDate)
     */
    void writeDateArray(@Nonnull String fieldName, @Nullable LocalDate[] values) throws IOException;

    /**
     * Writes an array of Timestamp's
     *
     * @param fieldName name of the field
     * @param values    LocalDateTime array to be written
     * @throws IOException in case of any exceptional case
     * @see #writeTimestamp(String, LocalDateTime)
     */
    void writeTimestampArray(@Nonnull String fieldName, @Nullable LocalDateTime[] values) throws IOException;

    /**
     * Writes an array of TimestampWithTimezone's
     *
     * @param fieldName name of the field
     * @param values    OffsetDateTime array to be written
     * @throws IOException in case of any exceptional case
     * @see #writeTimestampWithTimezone(String, OffsetDateTime)
     */
    void writeTimestampWithTimezoneArray(@Nonnull String fieldName, @Nullable OffsetDateTime[] values) throws IOException;

    /**
     * After writing portable fields one can subsequently write remaining fields in the old-fashioned way.
     * Users should note that after calling this method, trying to write portable fields will result
     * in an IOException.
     *
     * @return ObjectDataOutput
     * @throws IOException in case of any exceptional case
     */
    @Nonnull
    ObjectDataOutput getRawDataOutput() throws IOException;

}
