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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Additionally to GenericRecord, this one has more methods to be used in Query.
 *
 * @see GenericRecordQueryReader
 * <p>
 * read*FromArray methods will return `null`
 * 1. if the array is null or empty
 * 2. there is no data at given index. In other words, the index is bigger than the length of the array
 */
public interface InternalGenericRecord extends GenericRecord {

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    Boolean getBooleanFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    Byte getByteFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    Integer getUnsignedByteFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    Character getCharFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    Double getDoubleFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    Float getFloatFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    Integer getIntFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    Long getUnsignedIntFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    Long getLongFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    BigInteger getUnsignedLongFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    Short getShortFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    Integer getUnsignedShortFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    String getStringFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    GenericRecord getGenericRecordFromArray(@Nonnull String fieldName, int index);

    /**
     * Reads same value {@link InternalGenericRecord#getGenericRecord(String)} }, but in deserialized form.
     * This is used in query system when the object is leaf of the query.
     *
     * @param fieldName the name of the field
     * @param index     array index to read from
     * @return a nested field as a concrete deserialized object rather than generic record
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    Object getObjectFromArray(@Nonnull String fieldName, int index);

    /**
     * Reads same value {@link GenericRecord#getGenericRecordArray(String)}, but in deserialized form.
     * This is used in query system when the object is leaf of the query.
     *
     * @param fieldName the name of the field
     * @return a nested field as array of deserialized objects rather than array of the  generic records
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    <T> T[] getObjectArray(@Nonnull String fieldName, Class<T> componentType);

    /**
     * Reads same value {@link GenericRecord#getGenericRecord(String)} }, but in deserialized form.
     * This is used in query system when the object is leaf of the query.
     *
     * @param fieldName the name of the field
     * @return a nested field as a concrete deserialized object rather than generic record
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    <T> T getObject(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    BigDecimal getDecimalFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    LocalTime getTimeFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    LocalDate getDateFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    LocalDateTime getTimestampFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    OffsetDateTime getTimestampWithTimezoneFromArray(@Nonnull String fieldName, int index);
}
