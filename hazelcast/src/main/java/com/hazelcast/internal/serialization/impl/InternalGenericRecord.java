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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.impl.compact.CompactGenericRecord;
import com.hazelcast.internal.serialization.impl.compact.CompactInternalGenericRecord;
import com.hazelcast.internal.serialization.impl.compact.DefaultCompactReader;
import com.hazelcast.internal.serialization.impl.compact.DeserializedGenericRecord;
import com.hazelcast.internal.serialization.impl.portable.DeserializedPortableGenericRecord;
import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecord;
import com.hazelcast.internal.serialization.impl.portable.PortableInternalGenericRecord;
import com.hazelcast.nio.serialization.AbstractGenericRecord;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.compact.CompactReader;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Additionally to GenericRecord, this one has more methods to be used in Query.
 *
 * @see GenericRecordQueryReader
 * InternalGenericRecord implementations should not deserialize the passed content completely in their constructor when
 * they are created, because they will be used to query a couple of fields from a big object. And deserializing the
 * whole content in the constructor will be a redundant work.
 * @see com.hazelcast.internal.serialization.impl.compact.CompactInternalGenericRecord
 * @see com.hazelcast.internal.serialization.impl.portable.PortableInternalGenericRecord
 * <p>
 * read*FromArray methods will return `null`
 * 1. if the array is null or empty
 * 2. there is no data at given index. In other words, the index is bigger than the length of the array
 * <p>
 * <p>
 * GenericRecord inheritance hierarchy:
 * GenericRecord
 * - {@link InternalGenericRecord}                -> Interface for Query Related Methods
 * -- {@link AbstractGenericRecord}               -> common methods. toString getAny equals hashCode
 * --- {@link CompactGenericRecord}               -> Abstract class implementing getSchema toString for Compact
 * ---- {@link CompactInternalGenericRecord}      -> concrete class used in query {@link GenericRecordQueryReader}
 * ----- {@link DefaultCompactReader}             -> Adaptor to make CompactInternalGenericRecord usable as {@link CompactReader}
 * ---- {@link DeserializedGenericRecord}         -> concrete class passed to the user
 * --- {@link PortableGenericRecord}              -> Abstract class implementing getClassDefinition toString for Portable
 * ---- {@link DeserializedPortableGenericRecord} -> concrete class used in query {@link GenericRecordQueryReader}
 * ---- {@link PortableInternalGenericRecord}     -> concrete class passed to the user
 */
public interface InternalGenericRecord extends GenericRecord {

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    InternalGenericRecord getInternalGenericRecord(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    InternalGenericRecord[] getArrayOfInternalGenericRecord(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    Boolean getBooleanFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    Character getCharFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    Byte getInt8FromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    Short getInt16FromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    Integer getInt32FromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    Long getInt64FromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    Float getFloat32FromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    Double getFloat64FromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    String getStringFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    GenericRecord getGenericRecordFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    InternalGenericRecord getInternalGenericRecordFromArray(@Nonnull String fieldName, int index);

    /**
     * Reads same value {@link InternalGenericRecord#getGenericRecord(String)} }, but in deserialized form.
     * This is used in query system when the object is leaf of the query.
     *
     * @param fieldName the name of the field
     * @param index     array index to read from
     * @return a nested field as a concrete deserialized object rather than generic record
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    <T> T getObjectFromArray(@Nonnull String fieldName, int index);

    /**
     * Reads same value {@link GenericRecord#getArrayOfGenericRecord(String)} (String)}, but in deserialized form.
     * This is used in query system when the object is leaf of the query.
     *
     * @param fieldName the name of the field
     * @return a nested field as array of deserialized objects rather than array of the  generic records
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    <T> T[] getArrayOfObject(@Nonnull String fieldName, Class<T> componentType);

    /**
     * Reads same value {@link GenericRecord#getGenericRecord(String)} }, but in deserialized form.
     * This is used in query system when the object is leaf of the query.
     *
     * @param fieldName the name of the field
     * @return a nested field as a concrete deserialized object rather than generic record
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    <T> T getObject(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    BigDecimal getDecimalFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    LocalTime getTimeFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    LocalDate getDateFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    LocalDateTime getTimestampFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    OffsetDateTime getTimestampWithTimezoneFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    Boolean getNullableBooleanFromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    Byte getNullableInt8FromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    Short getNullableInt16FromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    Integer getNullableInt32FromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    Long getNullableInt64FromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    Float getNullableFloat32FromArray(@Nonnull String fieldName, int index);

    /**
     * @param fieldName the name of the field
     * @return the value from the given index, returns null if index is larger than the array size or if array itself
     * is null
     * @throws HazelcastSerializationException if the field name does not exist in the class definition/schema or
     *                                         the type of the field does not match the one in the class definition/schema.
     */
    @Nullable
    Double getNullableFloat64FromArray(@Nonnull String fieldName, int index);
}
