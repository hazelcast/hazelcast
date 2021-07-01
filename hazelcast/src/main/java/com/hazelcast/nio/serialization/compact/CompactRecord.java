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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Set;

/**
 * A generic object interface that is returned to user when the domain class can not be created from any of the distributed
 * hazelcast data structures like {@link com.hazelcast.map.IMap} ,{@link com.hazelcast.collection.IQueue} etc.
 * <p>
 * On remote calls like distributed executor service or EntryProcessors, you may need to access to the domain object. In
 * case class of the domain object is not available on the cluster, CompactRecord allows to access, read and write the objects
 * back without the class of the domain object on the classpath. Here is an read example with EntryProcessor:
 * <pre>
 * map.executeOnKey(key, (EntryProcessor<Object, Object, Object>) entry -> {
 *             Object value = entry.getValue();
 *             CompactRecord genericRecord = (CompactRecord) value;
 *
 *             int id = genericRecord.getInt("id");
 *
 *             return null;
 *         });
 * </pre>
 * Another example with EntryProcessor to demonstrate how to read, modify and set back to the map:
 * <pre>
 * map.executeOnKey("key", (EntryProcessor<Object, Object, Object>) entry -> {
 *             CompactRecord genericRecord = (CompactRecord) entry.getValue();
 *             CompactRecord modifiedCompactRecord = genericRecord.cloneWithBuilder()
 *                     .setInt("age",22).build();
 *             entry.setValue(modifiedCompactRecord);
 *             return null;
 *         });
 * </pre>
 * <p>
 * CompactRecord also allows to read from a cluster without having the classes on the client side.
 * For {@link Portable}, when {@link PortableFactory} is not provided in the config at the start,
 * a {@link HazelcastSerializationException} was thrown stating that a factory could not be found. Starting from 4.1,
 * the objects will be returned as {@link CompactRecord}. This way, the clients can read and write the objects back to
 * the cluster without needing the classes of the domain objects on the classpath.
 * <p>
 * Currently this is valid for {@link Portable} objects.
 *
 * @since 4.1
 */
@Beta
public interface CompactRecord {

    /**
     * Creates a {@link CompactRecordBuilder} allows to create a new object. This method is a convenience method to get a builder,
     * without creating the class definition for this type. Here you can see  a  new object is constructed from an existing
     * CompactRecord with its class definition:
     *
     * <pre>
     *
     * CompactRecord newCompactRecord = genericRecord.newBuilder()
     *      .setString("name", "bar")
     *      .setInt("id", 4).build();
     *
     * </pre>
     * <p>
     * see {@link CompactRecordBuilder#compact(String)} )} to create a CompactRecord
     * with a different class definition.
     *
     * @return an empty generic record builder with same class definition as this one
     */
    @Nonnull
    CompactRecordBuilder newBuilder();

    /**
     * Returned {@link CompactRecordBuilder} can be used to have exact copy and also just to update a couple of fields.
     * By default, it will copy all the fields.
     * So instead of following where only the `id` field is updated,
     * <pre>
     *     CompactRecord modifiedCompactRecord = genericRecord.newBuilder()
     *                         .setString("name", genericRecord.getString("name"))
     *                         .setLong("id", 4)
     *                         .setString("surname", genericRecord.getString("surname"))
     *                         .setInt("age", genericRecord.getInt("age")).build();
     * </pre>
     * `cloneWithBuilder` used as follows:
     * <pre>
     *     CompactRecord modifiedCompactRecord = genericRecord.cloneWithBuilder().setInt("id", 4).build();
     * </pre>
     *
     * @return a generic record builder with same class definition as this one and populated with same values.
     */
    @Nonnull
    CompactRecordBuilder cloneWithBuilder();

    /**
     * @return set of field names of this CompactRecord
     */
    @Nonnull
    Set<String> getFieldNames();

    /**
     * @param fieldName the name of the field
     * @return field type for the given field name
     * @throws IllegalArgumentException if the field name does not exist in the class definition
     */
    @Nonnull
    TypeID getFieldType(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return true if field exists in the definition of the class. Note that returns true even if the field is null.
     */
    boolean hasField(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    boolean getBoolean(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    byte getByte(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    char getChar(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    double getDouble(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    float getFloat(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    int getInt(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    long getLong(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    short getShort(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    String getString(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return decimal which is arbitrary precision and scale floating-point number as BigDecimal
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    BigDecimal getDecimal(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return time field consisting of hour, minute, seconds and nanos parts as LocalTime
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    LocalTime getTime(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return date field consisting of year, month of the year and day of the month as LocalDate
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    LocalDate getDate(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return timestamp field consisting of year, month of the year, day of the month, hour, minute, seconds,
     * nanos parts as LocalDateTime
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    LocalDateTime getTimestamp(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return timestamp with timezone field consisting of
     * year, month of the year, day of the month, offset seconds, hour, minute, seconds, nanos parts as OffsetDateTime
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    OffsetDateTime getTimestampWithTimezone(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    CompactRecord getCompactRecord(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    boolean[] getBooleanArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    byte[] getByteArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    char[] getCharArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    double[] getDoubleArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    float[] getFloatArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    int[] getIntArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    long[] getLongArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    short[] getShortArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    String[] getStringArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return array of Decimal's as BigDecimal[]
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     * @see #getDecimal(String)
     */
    @Nullable
    BigDecimal[] getDecimalArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return array of Time's as LocalTime[]
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     * @see #getTime(String)
     */
    @Nullable
    LocalTime[] getTimeArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return array of Date's to LocalDate[]
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     * @see #getDate(String)
     */
    @Nullable
    LocalDate[] getDateArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return array of Timestamp's as LocalDateTime[]
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     * @see #getTimestamp(String)
     */
    @Nullable
    LocalDateTime[] getTimestampArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return array of TimestampWithTimezone's as OffsetDateTime[]
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     * @see #getTimestampWithTimezone(String)
     */
    @Nullable
    OffsetDateTime[] getTimestampWithTimezoneArray(@Nonnull String fieldName);

    /**
     * @param fieldName the name of the field
     * @return the value of the field
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition.
     */
    @Nullable
    CompactRecord[] getCompactRecordArray(@Nonnull String fieldName);
}
