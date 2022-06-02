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
     * Writes a boolean.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeBoolean(@Nonnull String fieldName, boolean value);

    /**
     * Writes an 8-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeInt8(@Nonnull String fieldName, byte value);

    /**
     * Writes a 16-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeInt16(@Nonnull String fieldName, short value);

    /**
     * Writes a 32-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeInt32(@Nonnull String fieldName, int value);

    /**
     * Writes a 64-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeInt64(@Nonnull String fieldName, long value);

    /**
     * Writes a 32-bit IEEE 754 floating point number.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeFloat32(@Nonnull String fieldName, float value);

    /**
     * Writes a 64-bit IEEE 754 floating point number.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeFloat64(@Nonnull String fieldName, double value);

    /**
     * Writes a UTF-8 encoded string.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeString(@Nonnull String fieldName, @Nullable String value);

    /**
     * Writes an arbitrary precision and scale floating point number.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeDecimal(@Nonnull String fieldName, @Nullable BigDecimal value);

    /**
     * Writes a time consisting of hour, minute, second, and nanoseconds.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeTime(@Nonnull String fieldName, @Nonnull LocalTime value);

    /**
     * Writes a date consisting of year, month, and day.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeDate(@Nonnull String fieldName, @Nonnull LocalDate value);

    /**
     * Writes a timestamp consisting of date and time.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeTimestamp(@Nonnull String fieldName, @Nonnull LocalDateTime value);

    /**
     * Writes a timestamp with timezone consisting of date, time and timezone offset.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeTimestampWithTimezone(@Nonnull String fieldName, @Nonnull OffsetDateTime value);

    /**
     * Writes a nested compact object.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    <T> void writeCompact(@Nonnull String fieldName, @Nullable T value);

    /**
     * Writes an array of booleans.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfBoolean(@Nonnull String fieldName, @Nullable boolean[] value);

    /**
     * Writes an array of 8-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfInt8(@Nonnull String fieldName, @Nullable byte[] value);

    /**
     * Writes an array of 16-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfInt16(@Nonnull String fieldName, @Nullable short[] value);

    /**
     * Writes an array of 32-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfInt32(@Nonnull String fieldName, @Nullable int[] value);

    /**
     * Writes an array of 64-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfInt64(@Nonnull String fieldName, @Nullable long[] value);

    /**
     * Writes an array of 32-bit IEEE 754 floating point numbers.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfFloat32(@Nonnull String fieldName, @Nullable float[] value);

    /**
     * Writes an array of 64-bit IEEE 754 floating point numbers.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfFloat64(@Nonnull String fieldName, @Nullable double[] value);

    /**
     * Writes an array of UTF-8 encoded strings.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfString(@Nonnull String fieldName, @Nullable String[] value);

    /**
     * Writes an array of arbitrary precision and scale floating point numbers.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfDecimal(@Nonnull String fieldName, @Nullable BigDecimal[] value);

    /**
     * Writes an array of times consisting of hour, minute, second, and nano seconds.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfTime(@Nonnull String fieldName, @Nullable LocalTime[] value);

    /**
     * Writes an array of dates consisting of year, month, and day.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfDate(@Nonnull String fieldName, @Nullable LocalDate[] value);

    /**
     * Writes an array of timestamps consisting of date and time.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime[] value);

    /**
     * Writes an array of timestamps with timezone consisting of date, time and timezone offset.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime[] value);

    /**
     * Writes an array of nested compact objects.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    <T> void writeArrayOfCompact(@Nonnull String fieldName, @Nullable T[] value);

    /**
     * Writes a nullable boolean.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeNullableBoolean(@Nonnull String fieldName, @Nullable Boolean value);

    /**
     * Writes a nullable 8-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeNullableInt8(@Nonnull String fieldName, @Nullable Byte value);

    /**
     * Writes a nullable 16-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeNullableInt16(@Nonnull String fieldName, @Nullable Short value);

    /**
     * Writes a nullable 32-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeNullableInt32(@Nonnull String fieldName, @Nullable Integer value);

    /**
     * Writes a nullable 64-bit two's complement signed integer.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeNullableInt64(@Nonnull String fieldName, @Nullable Long value);

    /**
     * Writes a nullable 32-bit IEEE 754 floating point number.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeNullableFloat32(@Nonnull String fieldName, @Nullable Float value);

    /**
     * Writes a nullable 64-bit IEEE 754 floating point number.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeNullableFloat64(@Nonnull String fieldName, @Nullable Double value);

    /**
     * Writes a nullable array of nullable booleans.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfNullableBoolean(@Nonnull String fieldName, @Nullable Boolean[] value);

    /**
     * Writes a nullable array of nullable 8-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfNullableInt8(@Nonnull String fieldName, @Nullable Byte[] value);

    /**
     * Writes a nullable array of nullable 16-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfNullableInt16(@Nonnull String fieldName, @Nullable Short[] value);

    /**
     * Writes a nullable array of nullable 32-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfNullableInt32(@Nonnull String fieldName, @Nullable Integer[] value);

    /**
     * Writes a nullable array of nullable 64-bit two's complement signed integers.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfNullableInt64(@Nonnull String fieldName, @Nullable Long[] value);

    /**
     * Writes a nullable array of nullable 32-bit IEEE 754 floating point numbers.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfNullableFloat32(@Nonnull String fieldName, @Nullable Float[] value);

    /**
     * Writes a nullable array of nullable 64-bit IEEE 754 floating point numbers.
     *
     * @param fieldName name of the field.
     * @param value     to be written.
     */
    void writeArrayOfNullableFloat64(@Nonnull String fieldName, @Nullable Double[] value);
}
