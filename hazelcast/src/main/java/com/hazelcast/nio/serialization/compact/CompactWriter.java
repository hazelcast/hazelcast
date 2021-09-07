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

import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Provides means of writing compact serialized fields to the binary data.
 *
 * @since Hazelcast 5.0 as BETA
 */
@Beta
public interface CompactWriter {

    /**
     * Writes a 32-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @param value value to be written.
     */
    void writeInt(@Nonnull String fieldName, int value);

    /**
     * Writes a 64-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @param value value to be written.
     */
    void writeLong(@Nonnull String fieldName, long value);

    /**
     * Writes an UTF-8 encoded string.
     *
     * @param fieldName name of the field.
     * @param value value to be written.
     */
    void writeString(@Nonnull String fieldName, String value);

    /**
     * Writes a boolean.
     *
     * @param fieldName name of the field.
     * @param value value to be written.
     */
    void writeBoolean(@Nonnull String fieldName, boolean value);

    /**
     * Writes an 8-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @param value value to be written.
     */
    void writeByte(@Nonnull String fieldName, byte value);

    /**
     * Writes a 16-bit unsigned integer.
     *
     * @param fieldName name of the field.
     * @param value value to be written.
     */
    void writeChar(@Nonnull String fieldName, char value);

    /**
     * Writes a 64-bit IEEE 754 floating point number.
     *
     * @param fieldName name of the field.
     * @param value value to be written.
     */
    void writeDouble(@Nonnull String fieldName, double value);

    /**
     * Writes a 32-bit IEEE 754 floating point number.
     *
     * @param fieldName name of the field.
     * @param value value to be written.
     */
    void writeFloat(@Nonnull String fieldName, float value);

    /**
     * Writes a 16-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @param value value to be written.
     */
    void writeShort(@Nonnull String fieldName, short value);

    /**
     * Writes an arbitrary precision and scale floating point number.
     *
     * @param fieldName name of the field.
     * @param value value to be written.
     */
    void writeDecimal(@Nonnull String fieldName, @Nullable BigDecimal value);

    /**
     * Writes a time consisting of hour, minute, second, and nano seconds.
     *
     * @param fieldName name of the field.
     * @param value value to be written.
     */
    void writeTime(@Nonnull String fieldName, @Nonnull LocalTime value);

    /**
     * Writes a date consisting of year, month, and day.
     *
     * @param fieldName name of the field.
     * @param value value to be written.
     */
    void writeDate(@Nonnull String fieldName, @Nonnull LocalDate value);

    /**
     * Writes a timestamp consisting of date and time.
     *
     * @param fieldName name of the field.
     * @param value value to be written.
     */
    void writeTimestamp(@Nonnull String fieldName, @Nonnull LocalDateTime value);

    /**
     * Reads a timestamp with timezone consisting of date, time and timezone offset.
     *
     * @param fieldName name of the field.
     * @param value value to be written.
     */
    void writeTimestampWithTimezone(@Nonnull String fieldName, @Nonnull OffsetDateTime value);

    /**
     * Writes an enum object.
     *
     * @param fieldName name of the field.
     * @param value value to be written.
     */
    <T extends Enum> void writeEnum(@Nonnull String fieldName, @Nullable T value);

    /**
     * Writes an arbitrary object.
     *
     * @param fieldName name of the field.
     * @param value value to be written.
     */
    <T> void writeObject(@Nonnull String fieldName, @Nullable T value);

    /**
     * Writes an array of 8-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @param values values to be written.
     */
    void writeByteArray(@Nonnull String fieldName, @Nullable byte[] values);

    /**
     * Writes an array of booleans.
     *
     * @param fieldName name of the field.
     * @param values value to be written.
     */
    void writeBooleanArray(@Nonnull String fieldName, @Nullable boolean[] values);

    /**
     * Writes an array of 16-bit unsigned integers.
     *
     * @param fieldName name of the field.
     * @param values values to be written.
     */
    void writeCharArray(@Nonnull String fieldName, @Nullable char[] values);

    /**
     * Writes an array of 32-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @param values values to be written.
     */
    void writeIntArray(@Nonnull String fieldName, @Nullable int[] values);

    /**
     * Writes an array of 64-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @param values values to be written.
     */
    void writeLongArray(@Nonnull String fieldName, @Nullable long[] values);

    /**
     * Writes an array of 64-bit IEEE 754 floating point numbers.
     *
     * @param fieldName name of the field.
     * @param values values to be written.
     */
    void writeDoubleArray(@Nonnull String fieldName, @Nullable double[] values);

    /**
     * Writes an array of 32-bit IEEE 754 floating point numbers.
     *
     * @param fieldName name of the field.
     * @param values values to be written.
     */
    void writeFloatArray(@Nonnull String fieldName, @Nullable float[] values);

    /**
     * Writes an array of 16-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @param values values to be written.
     */
    void writeShortArray(@Nonnull String fieldName, @Nullable short[] values);

    /**
     * Writes an array of UTF-8 encoded strings.
     *
     * @param fieldName name of the field.
     * @param values values to be written.
     */
    void writeStringArray(@Nonnull String fieldName, @Nullable String[] values);

    /**
     * Writes an array of arbitrary precision and scale floating point numbers.
     *
     * @param fieldName name of the field.
     * @param values values to be written.
     */
    void writeDecimalArray(@Nonnull String fieldName, @Nullable BigDecimal[] values);

    /**
     * Writes an array of times consisting of hour, minute, second, and nano seconds.
     *
     * @param fieldName name of the field.
     * @param values values to be written.
     */
    void writeTimeArray(@Nonnull String fieldName, @Nullable LocalTime[] values);

    /**
     * Writes an array of dates consisting of year, month, and day.
     *
     * @param fieldName name of the field.
     * @param values values to be written.
     */
    void writeDateArray(@Nonnull String fieldName, @Nullable LocalDate[] values);

    /**
     * Writes an array of timestamps consisting of date and time.
     *
     * @param fieldName name of the field.
     * @param values values to be written.
     */
    void writeTimestampArray(@Nonnull String fieldName, @Nullable LocalDateTime[] values);

    /**
     * Writes an array of timestamps with timezone consisting of date, time and timezone offset.
     *
     * @param fieldName name of the field.
     * @param values values to be written.
     */
    void writeTimestampWithTimezoneArray(@Nonnull String fieldName, @Nullable OffsetDateTime[] values);

    /**
     * Writes an array of enums.
     *
     * @param fieldName name of the field.
     * @param values values to be written.
     */
    <T extends Enum> void writeEnumArray(@Nonnull String fieldName, @Nullable T[] values);

    /**
     * Writes an array of arbitrary objects.
     *
     * @param fieldName name of the field.
     * @param values values to be written.
     */
    <T> void writeObjectArray(@Nonnull String fieldName, @Nullable T[] values);
}
