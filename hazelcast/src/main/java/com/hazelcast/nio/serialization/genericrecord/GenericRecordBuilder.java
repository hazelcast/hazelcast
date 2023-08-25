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

import com.hazelcast.internal.serialization.impl.compact.DeserializedGenericRecordBuilder;
import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecordBuilder;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Interface for creating {@link GenericRecord} instances.
 *
 * @since 5.2
 */
public interface GenericRecordBuilder {

    /**
     * Creates a Builder that will build a {@link GenericRecord} in
     * {@link Portable} format with a new class definition:
     * <pre>{@code
     * ClassDefinition classDefinition = new ClassDefinitionBuilder(FACTORY_ID, CLASS_ID)
     *         .addStringField("name")
     *         .addIntField("id")
     *         .build();
     *
     * GenericRecord genericRecord = GenericRecordBuilder.portable(classDefinition)
     *         .setString("name", "foo")
     *         .setInt32("id", 123)
     *         .build();
     * }</pre>
     *
     * @param classDefinition of the Portable that will be created
     * @return GenericRecordBuilder for Portable format
     */
    @Nonnull
    static GenericRecordBuilder portable(@Nonnull ClassDefinition classDefinition) {
        return new PortableGenericRecordBuilder(classDefinition);
    }

    /**
     * /** Creates a Builder that will build a {@link GenericRecord} in
     * {@link com.hazelcast.config.CompactSerializationConfig Compact} format
     * with the given type name and schema:
     * <pre>{@code
     * GenericRecord genericRecord = GenericRecordBuilder.compact("typeName")
     *         .setString("name", "foo")
     *         .setInt32("id", 123)
     *         .build();
     * }</pre>
     *
     * @param typeName of the schema
     * @return GenericRecordBuilder for Compact format
     */
    @Nonnull
    static GenericRecordBuilder compact(String typeName) {
        return new DeserializedGenericRecordBuilder(typeName);
    }

    /**
     * @return a new constructed GenericRecord
     * @throws HazelcastSerializationException when the GenericRecord cannot be
     *                                         built.
     */
    @Nonnull
    GenericRecord build();

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setBoolean(@Nonnull String fieldName, boolean value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setInt8(@Nonnull String fieldName, byte value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     * <p>
     * Supported only for {@link Portable}. Not applicable for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}
     *
     * @param fieldName name of the field as it is defined in its class
     *                  definition. It should be composed of only alphanumeric
     *                  characters.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the class definition or the
     *                                         type of the field does not match
     *                                         the one in the class definition
     *                                         or the same field is trying to be
     *                                         set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setChar(@Nonnull String fieldName, char value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setInt16(@Nonnull String fieldName, short value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setInt32(@Nonnull String fieldName, int value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setInt64(@Nonnull String fieldName, long value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setFloat32(@Nonnull String fieldName, float value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setFloat64(@Nonnull String fieldName, double value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     * <p>
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its schema.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setNullableBoolean(@Nonnull String fieldName, @Nullable Boolean value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     * <p>
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its schema.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setNullableInt8(@Nonnull String fieldName, @Nullable Byte value);


    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     * <p>
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its schema.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setNullableInt16(@Nonnull String fieldName, @Nullable Short value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     * <p>
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its schema.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setNullableInt32(@Nonnull String fieldName, @Nullable Integer value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     * <p>
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its schema.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setNullableInt64(@Nonnull String fieldName, @Nullable Long value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     * <p>
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its schema.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setNullableFloat32(@Nonnull String fieldName, @Nullable Float value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     * <p>
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its schema.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setNullableFloat64(@Nonnull String fieldName, @Nullable Double value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setString(@Nonnull String fieldName, @Nullable String value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice. This method allows nested structures.
     * Subclass should also be created as `GenericRecord`.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException 1. if the field name does not exist
     *                                         in the schema/class definition.
     *                                         2. The type of the field does not
     *                                         match the one in the schema/class
     *                                         definition. 3. The same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     *                                         4. The type of the generic record is not
     *                                         the same as the generic record that is
     *                                         being built. e.g using portable generic
     *                                         record in a compact generic record builder.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setGenericRecord(@Nonnull String fieldName, @Nullable GenericRecord value);

    /**
     * Sets a decimal which is arbitrary precision and scale floating-point
     * number.
     * <p>
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setDecimal(@Nonnull String fieldName, @Nullable BigDecimal value);

    /**
     * Sets a time field consisting of hour, minute, seconds, and nanos parts.
     * <p>
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setTime(@Nonnull String fieldName, @Nullable LocalTime value);

    /**
     * Sets a date field consisting of year, month of the year, and day of the
     * month.
     * <p>
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setDate(@Nonnull String fieldName, @Nullable LocalDate value);

    /**
     * Sets a timestamp field consisting of year, month of the year, and day of
     * the month, hour, minute, seconds, nanos parts.
     * <p>
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime value);

    /**
     * Sets a timestamp with timezone field consisting of year, month of the
     * year and day of the month, offset seconds, hour, minute, seconds, nanos
     * parts.
     * <p>
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfBoolean(@Nonnull String fieldName, @Nullable boolean[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfInt8(@Nonnull String fieldName, @Nullable byte[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     * <p>
     * Supported only for {@link Portable}. Not applicable for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}
     *
     * @param fieldName name of the field as it is defined in its class
     *                  definition. It should be composed of only alphanumeric
     *                  characters
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the class definition or the
     *                                         type of the field does not match
     *                                         the one in the class definition
     *                                         or the same field is trying to be
     *                                         set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfChar(@Nonnull String fieldName, @Nullable char[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfInt16(@Nonnull String fieldName, @Nullable short[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfInt32(@Nonnull String fieldName, @Nullable int[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfInt64(@Nonnull String fieldName, @Nullable long[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfFloat32(@Nonnull String fieldName, @Nullable float[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfFloat64(@Nonnull String fieldName, @Nullable double[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     * <p>
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its schema.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfNullableBoolean(@Nonnull String fieldName, @Nullable Boolean[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     * <p>
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its schema.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfNullableInt8(@Nonnull String fieldName, @Nullable Byte[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     * <p>
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its schema.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfNullableInt16(@Nonnull String fieldName, @Nullable Short[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     * <p>
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its schema.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfNullableInt32(@Nonnull String fieldName, @Nullable Integer[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     * <p>
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its schema.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfNullableInt64(@Nonnull String fieldName, @Nullable Long[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     * <p>
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its schema.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfNullableFloat32(@Nonnull String fieldName, @Nullable Float[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice.
     * <p>
     * Supported only for
     * {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not
     * applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its schema.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema or the type of the
     *                                         field does not match the one in
     *                                         the schema or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfNullableFloat64(@Nonnull String fieldName, @Nullable Double[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice. Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfString(@Nonnull String fieldName, @Nullable String[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice. Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     * @see #setDecimal(String, BigDecimal)
     */
    @Nonnull
    GenericRecordBuilder setArrayOfDecimal(@Nonnull String fieldName, @Nullable BigDecimal[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice. Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     * @see #setTime(String, LocalTime)
     */
    @Nonnull
    GenericRecordBuilder setArrayOfTime(@Nonnull String fieldName, @Nullable LocalTime[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice. Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     * @see #setDate(String, LocalDate)
     */
    @Nonnull
    GenericRecordBuilder setArrayOfDate(@Nonnull String fieldName, @Nullable LocalDate[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice. Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     * @see #setTimestamp(String, LocalDateTime)
     */
    @Nonnull
    GenericRecordBuilder setArrayOfTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice. Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist
     *                                         in the schema/class definition or
     *                                         the type of the field does not
     *                                         match the one in the schema/class
     *                                         definition or the same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     * @see #setTimestampWithTimezone(String, OffsetDateTime)
     */
    @Nonnull
    GenericRecordBuilder setArrayOfTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime[] value);

    /**
     * It is legal to set the field again only when Builder is created with
     * {@link GenericRecord#newBuilderWithClone()}. Otherwise, it is illegal to
     * set to the same field twice. This method allows nested structures.
     * Subclasses should also be created as `GenericRecord`.
     * <p>
     * Array items can not be null.
     * <p>
     * For {@link com.hazelcast.config.CompactSerializationConfig Compact}
     * objects, it is not allowed write an array containing different item
     * types or a {@link HazelcastSerializationException} will be thrown.
     *
     * @param fieldName name of the field as it is defined in its schema/class
     *                  definition. It should be composed of only alphanumeric
     *                  characters for {@link Portable} GenericRecords.
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException 1. if the field name does not exist
     *                                         in the schema/class definition.
     *                                         2. The type of the field does not
     *                                         match the one in the schema/class
     *                                         definition. 3. The same field is
     *                                         trying to be set without using
     *                                         {@link
     *                                         GenericRecord#newBuilderWithClone()}.
     *                                         4. The type of the generic record is not
     *                                         the same as the generic record that is
     *                                         being built. e.g using portable generic
     *                                         record in a compact generic record builder.
     * @throws UnsupportedOperationException   if the setter is called after a
     *                                         GenericRecord is built by
     *                                         {@link GenericRecordBuilder#build}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfGenericRecord(@Nonnull String fieldName, @Nullable GenericRecord[] value);
}
