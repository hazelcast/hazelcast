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

package com.hazelcast.nio.serialization.genericrecord;

import com.hazelcast.collection.IQueue;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Set;

/**
 * A generic object interface that is returned to the user when the domain class
 * can not be created from any of the distributed Hazelcast data structures like
 * {@link IMap}, {@link IQueue} etc.
 * <p>
 * On remote calls like in the distributed executor service or EntryProcessors,
 * you may need access to the domain object. In case the class of the domain
 * object is not available on the cluster, GenericRecord allows you to read and
 * write the object without the domain class on the classpath. Here is an
 * example with EntryProcessor:
 * <pre>{@code
 * map.executeOnKey(key, (EntryProcessor<Object, Object, Object>) entry -> {
 *     Object value = entry.getValue();
 *     GenericRecord genericRecord = (GenericRecord) value;
 *
 *     int id = genericRecord.getInt32("id");
 *
 *     return null;
 * });
 * }</pre>
 * Another example with EntryProcessor to demonstrate how to read, modify and
 * set the value back to the map:
 * <pre>{@code
 * map.executeOnKey("key", (EntryProcessor<Object, Object, Object>) entry -> {
 *     GenericRecord genericRecord = (GenericRecord) entry.getValue();
 *     GenericRecord modifiedGenericRecord = genericRecord.newBuilderWithClone()
 *             .setInt32("age", 22)
 *             .build();
 *     entry.setValue(modifiedGenericRecord);
 *     return null;
 * });
 * }</pre>
 * <p>
 * GenericRecord also allows reading from a cluster without having the classes
 * on the client side. For {@link Portable}, when {@link PortableFactory} is not
 * provided in the config at the start, a
 * {@link HazelcastSerializationException} was thrown stating that a factory
 * could not be found. Starting from 4.1, the objects will be returned as
 * {@link GenericRecord}. This way, the clients can read and write objects back
 * to the cluster without the need to have the domain classes on the classpath.
 * <p>
 * Currently, this is valid for {@link Portable} and
 * {@link com.hazelcast.config.CompactSerializationConfig Compact} serializable
 * objects.
 *
 * @since 5.2
 */
public interface GenericRecord {

    /**
     * Creates a {@link GenericRecordBuilder} allows to create a new object.
     * This method is a convenience method to get a builder, without creating
     * the schema/class definition for this type. Here you can see a new object
     * is constructed from an existing GenericRecord with its schema/class
     * definition:
     *
     * <pre>{@code
     * GenericRecord newGenericRecord = genericRecord.newBuilder()
     *      .setString("name", "bar")
     *      .setInt32("id", 4)
     *      .build();
     * }</pre>
     * <p>
     * See {@link GenericRecordBuilder#portable(ClassDefinition)} to create a
     * GenericRecord in Portable format with a different class definition and
     * {@link GenericRecordBuilder#compact(String)} to create a GenericRecord in
     * Compact format with a different schema.
     *
     * @return an empty generic record builder with same class definition as
     * this one
     */
    @Nonnull
    GenericRecordBuilder newBuilder();

    /**
     * Returned {@link GenericRecordBuilder} can be used to have exact copy and
     * also just to update a couple of fields. By default, it will copy all the
     * fields. So instead of following where only the `id` field is updated,
     * <pre>{@code
     * GenericRecord modifiedGenericRecord = genericRecord.newBuilder()
     *         .setString("name", genericRecord.getString("name"))
     *         .setInt64("id", 4)
     *         .setString("surname", genericRecord.getString("surname"))
     *         .setInt32("age", genericRecord.getInt32("age"))
     *         .build();
     * }</pre>
     * `newBuilderWithClone` used as follows:
     * <pre>{@code
     * GenericRecord modifiedGenericRecord = genericRecord.newBuilderWithClone()
     *         .setInt32("id", 4)
     *         .build();
     * }</pre>
     *
     * @return a generic record builder with same schema/class definition as
     * this one and populated with same values.
     */
    @Nonnull
    GenericRecordBuilder newBuilderWithClone();

    /**
     * @return set of field names of this GenericRecord
     */
    @Nonnull
    Set<String> getFieldNames();

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
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    boolean getBoolean(@Nonnull String fieldName);

    /**
     * Supported only for {@link Portable}. Not applicable for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}
     *
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the class definition or the
     *                                         type of the field does not match
     *                                         the one in the class definition.
     */
    char getChar(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    byte getInt8(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    short getInt16(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    int getInt32(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    long getInt64(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    float getFloat32(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    double getFloat64(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    @Nullable
    String getString(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return decimal which is arbitrary precision and scale floating-point
     * number as {@link BigDecimal}
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    @Nullable
    BigDecimal getDecimal(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return time field consisting of hour, minute, seconds and nanos parts as
     * {@link LocalTime}
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    @Nullable
    LocalTime getTime(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return date field consisting of year, month of the year and day of the
     * month as {@link LocalDate}
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    @Nullable
    LocalDate getDate(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return timestamp field consisting of year, month of the year, day of the
     * month, hour, minute, seconds, nanos parts as {@link LocalDateTime}
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    @Nullable
    LocalDateTime getTimestamp(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return timestamp with timezone field consisting of year, month of the
     * year, day of the month, offset seconds, hour, minute, seconds, nanos
     * parts as {@link OffsetDateTime}
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    @Nullable
    OffsetDateTime getTimestampWithTimezone(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    @Nullable
    GenericRecord getGenericRecord(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    @Nullable
    boolean[] getArrayOfBoolean(@Nonnull String fieldName);

    /**
     * Supported only for {@link Portable}. Not applicable for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}
     *
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the class definition or the
     *                                         type of the field does not match
     *                                         the one in the class definition.
     */
    @Nullable
    char[] getArrayOfChar(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    @Nullable
    byte[] getArrayOfInt8(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    @Nullable
    short[] getArrayOfInt16(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    @Nullable
    int[] getArrayOfInt32(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    @Nullable
    long[] getArrayOfInt64(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    @Nullable
    float[] getArrayOfFloat32(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    @Nullable
    double[] getArrayOfFloat64(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    @Nullable
    String[] getArrayOfString(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     * @see #getDecimal(String)
     */
    @Nullable
    BigDecimal[] getArrayOfDecimal(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     * @see #getTime(String)
     */
    @Nullable
    LocalTime[] getArrayOfTime(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     * @see #getDate(String)
     */
    @Nullable
    LocalDate[] getArrayOfDate(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     * @see #getTimestamp(String)
     */
    @Nullable
    LocalDateTime[] getArrayOfTimestamp(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     * @see #getTimestampWithTimezone(String)
     */
    @Nullable
    OffsetDateTime[] getArrayOfTimestampWithTimezone(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition.
     */
    @Nullable
    GenericRecord[] getArrayOfGenericRecord(@Nonnull String fieldName);

    /**
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema.
     */
    @Nullable
    Boolean getNullableBoolean(@Nonnull String fieldName);

    /**
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema.
     */
    @Nullable
    Byte getNullableInt8(@Nonnull String fieldName);

    /**
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema.
     */
    @Nullable
    Short getNullableInt16(@Nonnull String fieldName);

    /**
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema.
     */
    @Nullable
    Integer getNullableInt32(@Nonnull String fieldName);

    /**
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema.
     */
    @Nullable
    Long getNullableInt64(@Nonnull String fieldName);

    /**
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema.
     */
    @Nullable
    Float getNullableFloat32(@Nonnull String fieldName);

    /**
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema.
     */
    @Nullable
    Double getNullableFloat64(@Nonnull String fieldName);

    /**
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema.
     */
    @Nullable
    Boolean[] getArrayOfNullableBoolean(@Nonnull String fieldName);

    /**
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema.
     */
    @Nullable
    Byte[] getArrayOfNullableInt8(@Nonnull String fieldName);

    /**
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema.
     */
    @Nullable
    Short[] getArrayOfNullableInt16(@Nonnull String fieldName);

    /**
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema.
     */
    @Nullable
    Integer[] getArrayOfNullableInt32(@Nonnull String fieldName);

    /**
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema.
     */
    @Nullable
    Long[] getArrayOfNullableInt64(@Nonnull String fieldName);

    /**
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema.
     */
    @Nullable
    Float[] getArrayOfNullableFloat32(@Nonnull String fieldName);

    /**
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema.
     */
    @Nullable
    Double[] getArrayOfNullableFloat64(@Nonnull String fieldName);
}
