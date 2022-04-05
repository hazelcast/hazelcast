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

import com.hazelcast.internal.serialization.impl.compact.DeserializedGenericRecordBuilder;
import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecordBuilder;
import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

/**
 * Interface for creating {@link GenericRecord} instances.
 */
@Beta
public
interface GenericRecordBuilder {

    /**
     * Creates a Builder that will build a {@link GenericRecord} in {@link Portable} format with a new class definition:
     * <pre>
     *     ClassDefinition classDefinition =
     *                 new ClassDefinitionBuilder(FACTORY_ID, CLASS_ID)
     *                         .addStringField("name").addIntField("id").build();
     *
     *     GenericRecord genericRecord = GenericRecordBuilder.portable(classDefinition)
     *           .setString("name", "foo")
     *           .setInt("id", 123).build();
     * </pre>
     *
     * @param classDefinition of the portable that we will create
     * @return GenericRecordBuilder for Portable format
     */
    @Nonnull
    static GenericRecordBuilder portable(@Nonnull ClassDefinition classDefinition) {
        return new PortableGenericRecordBuilder(classDefinition);
    }

    /**
     * @return a new constructed GenericRecord which the resulting record will be serialized in Compact Format
     * see {@link com.hazelcast.config.CompactSerializationConfig}
     * @since Hazelcast 5.0 as BETA
     */
    @Beta
    @Nonnull
    static GenericRecordBuilder compact(String typeName) {
        return new DeserializedGenericRecordBuilder(typeName);
    }

    /**
     * @return a new constructed GenericRecord
     * @throws HazelcastSerializationException when the GenericRecord cannot be build.
     */
    @Nonnull
    GenericRecord build();

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  It should be composed of only alpha-numeric characters.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setBoolean(@Nonnull String fieldName, boolean value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  It should be composed of only alpha-numeric characters.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setInt8(@Nonnull String fieldName, byte value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * <p>
     * Supported only for {@link Portable}. Not applicable for {@link com.hazelcast.config.CompactSerializationConfig Compact}
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setChar(@Nonnull String fieldName, char value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setInt16(@Nonnull String fieldName, short value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setInt32(@Nonnull String fieldName, int value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setInt64(@Nonnull String fieldName, long value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setFloat32(@Nonnull String fieldName, float value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setFloat64(@Nonnull String fieldName, double value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * <p>
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  It should be composed of only alpha-numeric characters.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setNullableBoolean(@Nonnull String fieldName, @Nullable Boolean value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * <p>
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  It should be composed of only alpha-numeric characters.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setNullableInt8(@Nonnull String fieldName, @Nullable Byte value);


    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * <p>
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setNullableInt16(@Nonnull String fieldName, @Nullable Short value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * <p>
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setNullableInt32(@Nonnull String fieldName, @Nullable Integer value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * <p>
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setNullableInt64(@Nonnull String fieldName, @Nullable Long value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * <p>
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setNullableFloat32(@Nonnull String fieldName, @Nullable Float value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * <p>
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setNullableFloat64(@Nonnull String fieldName, @Nullable Double value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setString(@Nonnull String fieldName, @Nullable String value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * This method allows nested structures. Subclass should also created as `GenericRecord`.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setGenericRecord(@Nonnull String fieldName, @Nullable GenericRecord value);

    /**
     * Sets a decimal which is arbitrary precision and scale floating-point number
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setDecimal(@Nonnull String fieldName, @Nullable BigDecimal value);

    /**
     * Sets a time field consisting of hour, minute, seconds and nanos parts
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setTime(@Nonnull String fieldName, @Nullable LocalTime value);

    /**
     * Sets a date field consisting of year , month of the year and day of the month
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setDate(@Nonnull String fieldName, @Nullable LocalDate value);

    /**
     * Sets a timestamp field consisting of
     * year , month of the year and day of the month, hour, minute, seconds, nanos parts
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime value);

    /**
     * Sets a timestamp with timezone field consisting of
     * year , month of the year and day of the month, offset seconds , hour, minute, seconds, nanos parts
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfBoolean(@Nonnull String fieldName, @Nullable boolean[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfInt8(@Nonnull String fieldName, @Nullable byte[] value);

    /**
     * Supported only for {@link Portable}. Not applicable for {@link com.hazelcast.config.CompactSerializationConfig Compact}
     * <p>
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfChar(@Nonnull String fieldName, @Nullable char[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfInt16(@Nonnull String fieldName, @Nullable short[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfInt32(@Nonnull String fieldName, @Nullable int[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfInt64(@Nonnull String fieldName, @Nullable long[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfFloat32(@Nonnull String fieldName, @Nullable float[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfFloat64(@Nonnull String fieldName, @Nullable double[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * <p>
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfNullableBoolean(@Nonnull String fieldName, @Nullable Boolean[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * <p>
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfNullableInt8(@Nonnull String fieldName, @Nullable Byte[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * <p>
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfNullableInt16(@Nonnull String fieldName, @Nullable Short[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * <p>
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfNullableInt32(@Nonnull String fieldName, @Nullable Integer[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * <p>
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfNullableInt64(@Nonnull String fieldName, @Nullable Long[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * <p>
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfNullableFloat32(@Nonnull String fieldName, @Nullable Float[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * <p>
     * Supported only for {@link com.hazelcast.config.CompactSerializationConfig Compact}. Not applicable to {@link Portable}.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfNullableFloat64(@Nonnull String fieldName, @Nullable Double[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfString(@Nonnull String fieldName, @Nullable String[] value);

    /**
     * Sets an array of Decimals
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     * @see #setDecimal(String, BigDecimal)
     */
    @Nonnull
    GenericRecordBuilder setArrayOfDecimal(@Nonnull String fieldName, @Nullable BigDecimal[] value);

    /**
     * Sets an array of Time's
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     * @see #setTime(String, LocalTime)
     */
    @Nonnull
    GenericRecordBuilder setArrayOfTime(@Nonnull String fieldName, @Nullable LocalTime[] value);

    /**
     * Sets an array of Date's
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     * @see #setDate(String, LocalDate)
     */
    @Nonnull
    GenericRecordBuilder setArrayOfDate(@Nonnull String fieldName, @Nullable LocalDate[] value);

    /**
     * Sets an array of Timestamp's
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     * @see #setTimestamp(String, LocalDateTime)
     */
    @Nonnull
    GenericRecordBuilder setArrayOfTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime[] value);

    /**
     * Sets an array of TimestampWithTimezone's
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     * @see #setTimestampWithTimezone(String, OffsetDateTime)
     */
    @Nonnull
    GenericRecordBuilder setArrayOfTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime[] value);

    /**
     * It is legal to set the field again only when Builder is created with {@link GenericRecord#cloneWithBuilder()}.
     * Otherwise, it is illegal to set to the same field twice.
     * This method allows nested structures. Subclasses should also created as `GenericRecord`.
     * <p>
     * Array items can not be null.
     *
     * @param fieldName name of the field as it is defined in its class definition.
     *                  See {@link ClassDefinition} for {@link Portable}
     * @param value     to set to GenericRecord
     * @return itself for chaining
     * @throws HazelcastSerializationException if the field name does not exist in the class definition or
     *                                         the type of the field does not match the one in the class definition or
     *                                         Same field is trying to be set without using
     *                                         {@link GenericRecord#cloneWithBuilder()}.
     */
    @Nonnull
    GenericRecordBuilder setArrayOfGenericRecord(@Nonnull String fieldName, @Nullable GenericRecord[] value);
}
