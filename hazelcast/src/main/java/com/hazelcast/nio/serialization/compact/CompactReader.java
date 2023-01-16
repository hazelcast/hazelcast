/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Provides means of reading Compact serialized fields from the binary data.
 * <p>
 * Read operations might throw {@link HazelcastSerializationException} when a
 * field with the given name is not found or there is a type mismatch.
 * <p>
 * The way to use {@link CompactReader} for class evolution is to check for the
 * existence of a field with its name and kind, with the
 * {@link #getFieldKind(String)} method. One should read the field if it exists
 * with the given name and kind, and use some other logic, like using a default
 * value, if it does not exist.
 * <pre>{@code
 * public Foo read(CompactReader reader) {
 *     int bar = reader.readInt32("bar");  // A field that is always present
 *     String baz;
 *     if (reader.getFieldKind("baz") == FieldKind.STRING) {
 *         baz = reader.readString("baz");
 *     } else {
 *         baz = ""; // Use a default value, if the field is not present
 *     }
 *     return new Foo(bar, baz);
 * }
 * }</pre>
 *
 * @since 5.2
 */
public interface CompactReader {

    /**
     * Returns the kind of the field for the given field name.
     * <p>
     * If the field with the given name does not exist,
     * {@link FieldKind#NOT_AVAILABLE} is returned.
     * <p>
     * This method can be used to check the existence of a field, which can be
     * useful when the class is evolved.
     *
     * @param fieldName name of the field.
     * @return kind of the field
     */
    @Nonnull
    FieldKind getFieldKind(@Nonnull String fieldName);

    /**
     * Reads a boolean.
     * <p>
     * This method can also read a nullable boolean, as long as it is not
     * {@code null}. If a {@code null} value is read with this method,
     * {@link HazelcastSerializationException} is thrown.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    boolean readBoolean(@Nonnull String fieldName);

    /**
     * Reads an 8-bit two's complement signed integer.
     * <p>
     * This method can also read a nullable int8, as long as it is not
     * {@code null}. If a {@code null} value is read with this method,
     * {@link HazelcastSerializationException} is thrown.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    byte readInt8(@Nonnull String fieldName);


    /**
     * Reads a 16-bit two's complement signed integer.
     * <p>
     * This method can also read a nullable int16, as long as it is not
     * {@code null}. If a {@code null} value is read with this method,
     * {@link HazelcastSerializationException} is thrown.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    short readInt16(@Nonnull String fieldName);


    /**
     * Reads a 32-bit two's complement signed integer.
     * <p>
     * This method can also read a nullable int32, as long as it is not
     * {@code null}. If a {@code null} value is read with this method,
     * {@link HazelcastSerializationException} is thrown.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    int readInt32(@Nonnull String fieldName);


    /**
     * Reads a 64-bit two's complement signed integer.
     * <p>
     * This method can also read a nullable int64, as long as it is not
     * {@code null}. If a {@code null} value is read with this method,
     * {@link HazelcastSerializationException} is thrown.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    long readInt64(@Nonnull String fieldName);


    /**
     * Reads a 32-bit IEEE 754 floating point number.
     * <p>
     * This method can also read a nullable float32, as long as it is not
     * {@code null}. If a {@code null} value is read with this method,
     * {@link HazelcastSerializationException} is thrown.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    float readFloat32(@Nonnull String fieldName);


    /**
     * Reads a 64-bit IEEE 754 floating point number.
     * <p>
     * This method can also read a nullable float64, as long as it is not
     * {@code null}. If a {@code null} value is read with this method,
     * {@link HazelcastSerializationException} is thrown.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    double readFloat64(@Nonnull String fieldName);


    /**
     * Reads an UTF-8 encoded string.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    String readString(@Nonnull String fieldName);


    /**
     * Reads an arbitrary precision and scale floating point number.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    BigDecimal readDecimal(@Nonnull String fieldName);


    /**
     * Reads a time consisting of hour, minute, second, and nano seconds.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    LocalTime readTime(@Nonnull String fieldName);


    /**
     * Reads a date consisting of year, month, and day.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    LocalDate readDate(@Nonnull String fieldName);


    /**
     * Reads a timestamp consisting of date and time.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    LocalDateTime readTimestamp(@Nonnull String fieldName);


    /**
     * Reads a timestamp with timezone consisting of date, time and timezone
     * offset.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    OffsetDateTime readTimestampWithTimezone(@Nonnull String fieldName);


    /**
     * Reads a compact object
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException       if the field does not exist
     *                                               in the schema or the type
     *                                               of the field does not match
     *                                               with the one defined in the
     *                                               schema.
     * @throws com.hazelcast.core.HazelcastException if the object cannot be
     *                                               created.
     */
    @Nullable
    <T> T readCompact(@Nonnull String fieldName);


    /**
     * Reads an array of booleans.
     * <p>
     * This method can also read an array of nullable booleans, as long as it
     * does not contain {@code null} values. If a {@code null} array item is
     * read with this method, {@link HazelcastSerializationException} is thrown.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    boolean[] readArrayOfBoolean(@Nonnull String fieldName);


    /**
     * Reads an array of 8-bit two's complement signed integers.
     * <p>
     * This method can also read an array of nullable int8s, as long as it
     * does not contain {@code null} values. If a {@code null} array item is
     * read with this method, {@link HazelcastSerializationException} is thrown.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    byte[] readArrayOfInt8(@Nonnull String fieldName);


    /**
     * Reads an array of 16-bit two's complement signed integers.
     * <p>
     * This method can also read an array of nullable int16s, as long as it
     * does not contain {@code null} values. If a {@code null} array item is
     * read with this method, {@link HazelcastSerializationException} is thrown.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    short[] readArrayOfInt16(@Nonnull String fieldName);


    /**
     * Reads an array of 32-bit two's complement signed integers.
     * <p>
     * This method can also read an array of nullable int32s, as long as it
     * does not contain {@code null} values. If a {@code null} array item is
     * read with this method, {@link HazelcastSerializationException} is thrown.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    int[] readArrayOfInt32(@Nonnull String fieldName);


    /**
     * Reads an array of 64-bit two's complement signed integers.
     * <p>
     * This method can also read an array of nullable int64s, as long as it
     * does not contain {@code null} values. If a {@code null} array item is
     * read with this method, {@link HazelcastSerializationException} is thrown.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    long[] readArrayOfInt64(@Nonnull String fieldName);


    /**
     * Reads an array of 32-bit IEEE 754 floating point numbers.
     * <p>
     * This method can also read an array of nullable float32s, as long as it
     * does not contain {@code null} values. If a {@code null} array item is
     * read with this method, {@link HazelcastSerializationException} is thrown.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    float[] readArrayOfFloat32(@Nonnull String fieldName);


    /**
     * Reads an array of 64-bit IEEE 754 floating point numbers.
     * <p>
     * This method can also read an array of nullable float64s, as long as it
     * does not contain {@code null} values. If a {@code null} array item is
     * read with this method, {@link HazelcastSerializationException} is thrown.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    double[] readArrayOfFloat64(@Nonnull String fieldName);


    /**
     * Reads an array of UTF-8 encoded strings.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    String[] readArrayOfString(@Nonnull String fieldName);


    /**
     * Reads an array of arbitrary precision and scale floating point numbers.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    BigDecimal[] readArrayOfDecimal(@Nonnull String fieldName);


    /**
     * Reads an array of times consisting of hour, minute, second, and
     * nanoseconds
     *
     * @param fieldName name of the field.
     * @return the value of the field. The items in the array cannot be null.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    LocalTime[] readArrayOfTime(@Nonnull String fieldName);


    /**
     * Reads an array of dates consisting of year, month, and day.
     *
     * @param fieldName name of the field.
     * @return the value of the field. The items in the array cannot be null.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    LocalDate[] readArrayOfDate(@Nonnull String fieldName);


    /**
     * Reads an array of timestamps consisting of date and time.
     *
     * @param fieldName name of the field.
     * @return the value of the field. The items in the array cannot be null.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    LocalDateTime[] readArrayOfTimestamp(@Nonnull String fieldName);


    /**
     * Reads an array of timestamps with timezone consisting of date, time and
     * timezone offset.
     *
     * @param fieldName name of the field.
     * @return the value of the field. The items in the array cannot be null.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    OffsetDateTime[] readArrayOfTimestampWithTimezone(@Nonnull String fieldName);


    /**
     * Reads an array of compact objects.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    <T> T[] readArrayOfCompact(@Nonnull String fieldName, @Nullable Class<T> componentType);


    /**
     * Reads a nullable boolean.
     * <p>
     * This method can also read a non-nullable boolean.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    Boolean readNullableBoolean(@Nonnull String fieldName);


    /**
     * Reads a nullable 8-bit two's complement signed integer.
     * <p>
     * This method can also read a non-nullable int8.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    Byte readNullableInt8(@Nonnull String fieldName);


    /**
     * Reads a nullable 16-bit two's complement signed integer.
     * <p>
     * This method can also read a non-nullable int16.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    Short readNullableInt16(@Nonnull String fieldName);


    /**
     * Reads a nullable 32-bit two's complement signed integer.
     * <p>
     * This method can also read a non-nullable int32.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    Integer readNullableInt32(@Nonnull String fieldName);


    /**
     * Reads a nullable 64-bit two's complement signed integer.
     * <p>
     * This method can also read a non-nullable int64.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    Long readNullableInt64(@Nonnull String fieldName);


    /**
     * Reads a nullable 32-bit IEEE 754 floating point number.
     * <p>
     * This method can also read a non-nullable float32.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    Float readNullableFloat32(@Nonnull String fieldName);


    /**
     * Reads a nullable 64-bit IEEE 754 floating point number.
     * <p>
     * This method can also read a non-nullable float64.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    Double readNullableFloat64(@Nonnull String fieldName);


    /**
     * Reads a nullable array of nullable booleans.
     * <p>
     * This method can also read array of non-nullable booleans.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    Boolean[] readArrayOfNullableBoolean(@Nonnull String fieldName);


    /**
     * Reads a nullable array of nullable 8-bit two's complement signed
     * integers.
     * <p>
     * This method can also read array of non-nullable int8s.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    Byte[] readArrayOfNullableInt8(@Nonnull String fieldName);


    /**
     * Reads a nullable array of nullable 16-bit two's complement signed
     * integers.
     * <p>
     * This method can also read array of non-nullable int16s.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    Short[] readArrayOfNullableInt16(@Nonnull String fieldName);


    /**
     * Reads a nullable array of nullable 32-bit two's complement signed
     * integers.
     * <p>
     * This method can also read array of non-nullable int32s.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    Integer[] readArrayOfNullableInt32(@Nonnull String fieldName);


    /**
     * Reads a nullable array of nullable 64-bit two's complement signed
     * integers.
     * <p>
     * This method can also read array of non-nullable int64s.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    Long[] readArrayOfNullableInt64(@Nonnull String fieldName);


    /**
     * Reads a nullable array of nullable 32-bit IEEE 754 floating point
     * numbers.
     * <p>
     * This method can also read array of non-nullable float32s.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    Float[] readArrayOfNullableFloat32(@Nonnull String fieldName);


    /**
     * Reads a nullable array of nullable 64-bit IEEE 754 floating point
     * numbers.
     * <p>
     * This method can also read array of non-nullable float64s.
     *
     * @param fieldName name of the field.
     * @return the value of the field.
     * @throws HazelcastSerializationException if the field does not exist in
     *                                         the schema or the type of the
     *                                         field does not match with the one
     *                                         defined in the schema.
     */
    @Nullable
    Double[] readArrayOfNullableFloat64(@Nonnull String fieldName);

}
