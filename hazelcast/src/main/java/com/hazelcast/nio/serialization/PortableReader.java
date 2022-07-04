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

import com.hazelcast.nio.ObjectDataInput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Set;

/**
 * Provides means for reading portable fields from binary data in the form of java primitives,
 * arrays of java primitives, nested portable fields and array of portable fields.
 */
public interface PortableReader {

    /**
     * @return global version of portable classes
     */
    int getVersion();

    /**
     * @param fieldName name of the field (does not support nested paths)
     * @return true if field exist in this class.
     */
    boolean hasField(@Nonnull String fieldName);

    /**
     * @return set of field names on this portable class
     */
    @Nonnull
    Set<String> getFieldNames();

    /**
     * @param fieldName name of the field
     * @return field type of given fieldName
     * @throws java.lang.IllegalArgumentException if the field does not exist.
     */
    @Nonnull
    FieldType getFieldType(@Nonnull String fieldName);

    /**
     * @param fieldName name of the field
     * @return classId of given field
     */
    int getFieldClassId(@Nonnull String fieldName);

    /**
     * @param fieldName name of the field
     * @return the int value read
     * @throws IOException in case of any exceptional case
     */
    int readInt(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the long value read
     * @throws IOException in case of any exceptional case
     */
    long readLong(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the utf string value read
     * @throws IOException in case of any exceptional case
     * @deprecated for the sake of better naming. Use {@link #readString(String)} instead
     */
    @Nullable
    @Deprecated
    String readUTF(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the string value read
     * @throws IOException in case of any exceptional case
     */
    @Nullable
    String readString(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the boolean value read
     * @throws IOException in case of any exceptional case
     */
    boolean readBoolean(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the byte value read
     * @throws IOException in case of any exceptional case
     */
    byte readByte(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the char value read
     * @throws IOException in case of any exceptional case
     */
    char readChar(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the double value read
     * @throws IOException in case of any exceptional case
     */
    double readDouble(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the float value read
     * @throws IOException in case of any exceptional case
     */
    float readFloat(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the short value read
     * @throws IOException in case of any exceptional case
     */
    short readShort(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @param <P>       the type of the portable read
     * @return the portable value read
     * @throws IOException in case of any exceptional case
     */
    @Nullable
    <P extends Portable> P readPortable(@Nonnull String fieldName) throws IOException;

    /**
     * Reads a decimal which is arbitrary precision and scale floating-point number to BigDecimal
     *
     * @param fieldName name of the field
     * @return the BigDecimal value read
     * @throws IOException in case of any exceptional case
     */
    @Nullable
    BigDecimal readDecimal(@Nonnull String fieldName) throws IOException;

    /**
     * Reads a time field consisting of hour, minute, seconds and nanos parts to LocalTime
     *
     * @param fieldName name of the field
     * @return the LocalTime value read
     * @throws IOException in case of any exceptional case
     */
    @Nullable
    LocalTime readTime(@Nonnull String fieldName) throws IOException;

    /**
     * Reads a date field consisting of year, month of the year and day of the month to LocalDate
     *
     * @param fieldName name of the field
     * @return the LocalDate value read
     * @throws IOException in case of any exceptional case
     */
    @Nullable
    LocalDate readDate(@Nonnull String fieldName) throws IOException;

    /**
     * Reads a timestamp field consisting of
     * year, month of the year, day of the month, hour, minute, seconds, nanos parts to LocalDateTime
     *
     * @param fieldName name of the field
     * @return the LocalDateTime value read
     * @throws IOException in case of any exceptional case
     */
    @Nullable
    LocalDateTime readTimestamp(@Nonnull String fieldName) throws IOException;

    /**
     * Reads a timestamp with timezone field consisting of
     * year, month of the year, day of the month, offset seconds, hour, minute, seconds, nanos parts
     * to OffsetDateTime
     *
     * @param fieldName name of the field
     * @return the OffsetDateTime value read
     * @throws IOException in case of any exceptional case
     */
    @Nullable
    OffsetDateTime readTimestampWithTimezone(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the byte array value read
     * @throws IOException in case of any exceptional case
     */
    @Nullable
    byte[] readByteArray(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the boolean array value read
     * @throws IOException in case of any exceptional case
     */
    @Nullable
    boolean[] readBooleanArray(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the char array value read
     * @throws IOException in case of any exceptional case
     */
    @Nullable
    char[] readCharArray(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the int array value read
     * @throws IOException in case of any exceptional case
     */
    @Nullable
    int[] readIntArray(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the long array value read
     * @throws IOException in case of any exceptional case
     */
    @Nullable
    long[] readLongArray(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the double array value read
     * @throws IOException in case of any exceptional case
     */
    @Nullable
    double[] readDoubleArray(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the float array value read
     * @throws IOException in case of any exceptional case
     */
    @Nullable
    float[] readFloatArray(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the short array value read
     * @throws IOException in case of any exceptional case
     */
    short[] readShortArray(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the String array value read
     * @throws IOException in case of any exceptional case
     * @deprecated for the sake of better naming. Use {@link #readStringArray(String)} instead
     */
    @Nullable
    @Deprecated
    String[] readUTFArray(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the String array value read
     * @throws IOException in case of any exceptional case
     */
    @Nullable
    String[] readStringArray(@Nonnull String fieldName) throws IOException;

    /**
     * @param fieldName name of the field
     * @return the portable array read
     * @throws IOException in case of any exceptional case
     */
    @Nullable
    Portable[] readPortableArray(@Nonnull String fieldName) throws IOException;

    /**
     * Reads an array of Decimal's to BigDecimal[]
     *
     * @param fieldName name of the field
     * @return the BigDecimal array read
     * @throws IOException in case of any exceptional case
     * @see #readDecimal(String)
     */
    @Nullable
    BigDecimal[] readDecimalArray(@Nonnull String fieldName) throws IOException;

    /**
     * Reads an array of Time's to LocalTime[]
     *
     * @param fieldName name of the field
     * @return the LocalTime array read
     * @throws IOException in case of any exceptional case
     * @see #readTime(String)
     */
    @Nullable
    LocalTime[] readTimeArray(@Nonnull String fieldName) throws IOException;

    /**
     * Reads an array of Date's to LocalDate[]
     *
     * @param fieldName name of the field
     * @return the LocalDate array read
     * @throws IOException in case of any exceptional case
     * @see #readDate(String)
     */
    @Nullable
    LocalDate[] readDateArray(@Nonnull String fieldName) throws IOException;

    /**
     * Reads an array of Timestamp's to LocalDateTime[]
     *
     * @param fieldName name of the field
     * @return the LocalDateTime array read
     * @throws IOException in case of any exceptional case
     * @see #readTimestamp(String)
     */
    @Nullable
    LocalDateTime[] readTimestampArray(@Nonnull String fieldName) throws IOException;

    /**
     * Reads an array of TimestampWithTimezone's to OffsetDateTime[]
     *
     * @param fieldName name of the field
     * @return the OffsetDateTime array read
     * @throws IOException in case of any exceptional case
     * @see #readTimestampWithTimezone(String)
     */
    @Nullable
    OffsetDateTime[] readTimestampWithTimezoneArray(@Nonnull String fieldName) throws IOException;

    /**
     * {@link PortableWriter#getRawDataOutput()}.
     * <p>
     * Note that portable fields can not be read after this method is called. If this happens,
     * an IOException will be thrown.
     *
     * @return rawDataInput
     * @throws IOException in case of any exceptional case
     */
    @Nonnull
    ObjectDataInput getRawDataInput() throws IOException;
}
