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

import com.hazelcast.internal.serialization.impl.compact.DeserializedCompactRecordBuilder;
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
 * Interface for creating {@link CompactRecord} instances.
 */
@Beta
public
interface CompactRecordBuilder {

    /**
     * @return a new constructed CompactRecord
     */
    @Nonnull
    static CompactRecordBuilder compact(String className) {
        return new DeserializedCompactRecordBuilder(className);
    }

    /**
     * @return a new constructed CompactRecord
     * @throws HazelcastSerializationException when the CompactRecord cannot be build.
     */
    @Nonnull
    CompactRecord build();

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  It should be composed of only alpha-numeric characters.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setBoolean(@Nonnull String fieldName, boolean value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  It should be composed of only alpha-numeric characters.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setByte(@Nonnull String fieldName, byte value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setChar(@Nonnull String fieldName, char value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setDouble(@Nonnull String fieldName, double value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setFloat(@Nonnull String fieldName, float value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setInt(@Nonnull String fieldName, int value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setLong(@Nonnull String fieldName, long value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setShort(@Nonnull String fieldName, short value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setString(@Nonnull String fieldName, @Nullable String value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * This method allows nested structures. Subclass should also created as `CompactRecord`.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setCompactRecord(@Nonnull String fieldName, @Nullable CompactRecord value);

    /**
     * Sets a decimal which is arbitrary precision and scale floating-point number
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setDecimal(@Nonnull String fieldName, @Nullable BigDecimal value);

    /**
     * Sets a time field consisting of hour, minute, seconds and nanos parts
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setTime(@Nonnull String fieldName, @Nullable LocalTime value);

    /**
     * Sets a date field consisting of year , month of the year and day of the month
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setDate(@Nonnull String fieldName, @Nullable LocalDate value);

    /**
     * Sets a timestamp field consisting of
     * year , month of the year and day of the month, hour, minute, seconds, nanos parts
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime value);

    /**
     * Sets a timestamp with timezone field consisting of
     * year , month of the year and day of the month, offset seconds , hour, minute, seconds, nanos parts
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setBooleanArray(@Nonnull String fieldName, @Nullable boolean[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setByteArray(@Nonnull String fieldName, @Nullable byte[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setCharArray(@Nonnull String fieldName, @Nullable char[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setFloatArray(@Nonnull String fieldName, @Nullable float[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setIntArray(@Nonnull String fieldName, @Nullable int[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setDoubleArray(@Nonnull String fieldName, @Nullable double[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setLongArray(@Nonnull String fieldName, @Nullable long[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setShortArray(@Nonnull String fieldName, @Nullable short[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setStringArray(@Nonnull String fieldName, @Nullable String[] value);

    /**
     * Sets an array of Decimals
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     * @see #setDecimal(String, BigDecimal)
     */
    @Nonnull
    CompactRecordBuilder setDecimalArray(@Nonnull String fieldName, @Nullable BigDecimal[] value);

    /**
     * Sets an array of Time's
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     * @see #setTime(String, LocalTime)
     */
    @Nonnull
    CompactRecordBuilder setTimeArray(@Nonnull String fieldName, @Nullable LocalTime[] value);

    /**
     * Sets an array of Date's
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     * @see #setDate(String, LocalDate)
     */
    @Nonnull
    CompactRecordBuilder setDateArray(@Nonnull String fieldName, @Nullable LocalDate[] value);

    /**
     * Sets an array of Timestamp's
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     * @see #setTimestamp(String, LocalDateTime)
     */
    @Nonnull
    CompactRecordBuilder setTimestampArray(@Nonnull String fieldName, @Nullable LocalDateTime[] value);

    /**
     * Sets an array of TimestampWithTimezone's
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     * @see #setTimestampWithTimezone(String, OffsetDateTime)
     */
    @Nonnull
    CompactRecordBuilder setTimestampWithTimezoneArray(@Nonnull String fieldName, @Nullable OffsetDateTime[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link CompactRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * This method allows nested structures. Subclasses should also created as `CompactRecord`.
     * <p>
     * Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     * @param value     to set to CompactRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link CompactRecord#cloneWithBuilder()}.
     */
    @Nonnull
    CompactRecordBuilder setCompactRecordArray(@Nonnull String fieldName, @Nullable CompactRecord[] value);
}
