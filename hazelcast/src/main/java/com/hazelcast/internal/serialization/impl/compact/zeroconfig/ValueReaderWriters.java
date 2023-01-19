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

package com.hazelcast.internal.serialization.impl.compact.zeroconfig;

import com.hazelcast.internal.serialization.impl.compact.CompactStreamSerializer;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.charArrayAsShortArray;
import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.charArrayFromShortArray;
import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.characterArrayAsShortArray;
import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.characterArrayFromShortArray;
import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.characterAsShort;
import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.characterFromShort;
import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.enumArrayAsStringNameArray;
import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.enumArrayFromStringNameArray;
import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.enumAsStringName;
import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.enumFromStringName;
import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.isFieldExist;
import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.verifyFieldClassIsCompactSerializable;
import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.verifyFieldClassShouldBeSerializedAsCompact;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_COMPACT;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DATE;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DECIMAL;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT16;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT32;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT64;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT8;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT16;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT32;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT64;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT8;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_STRING;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIME;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMP;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE;
import static com.hazelcast.nio.serialization.FieldKind.BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.COMPACT;
import static com.hazelcast.nio.serialization.FieldKind.DATE;
import static com.hazelcast.nio.serialization.FieldKind.DECIMAL;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.INT16;
import static com.hazelcast.nio.serialization.FieldKind.INT32;
import static com.hazelcast.nio.serialization.FieldKind.INT64;
import static com.hazelcast.nio.serialization.FieldKind.INT8;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT16;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT32;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT64;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT8;
import static com.hazelcast.nio.serialization.FieldKind.STRING;
import static com.hazelcast.nio.serialization.FieldKind.TIME;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP_WITH_TIMEZONE;

/**
 * Class that stores all the value reader writers and returns the appropriate
 * one for the class that requested it.
 */
@SuppressWarnings("checkstyle:executablestatementcount")
public final class ValueReaderWriters {
    private static final Map<Class<?>, Function<String, ValueReaderWriter<?>>> CONSTRUCTORS = new HashMap<>();
    private static final Map<Class<?>, Function<String, ValueReaderWriter<?>>> ARRAY_CONSTRUCTORS = new HashMap<>();

    static {
        CONSTRUCTORS.put(String.class, StringReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(String.class, StringArrayReaderWriter::new);

        CONSTRUCTORS.put(BigDecimal.class, BigDecimalReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(BigDecimal.class, BigDecimalArrayReaderWriter::new);

        CONSTRUCTORS.put(LocalTime.class, LocalTimeReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(LocalTime.class, LocalTimeArrayReaderWriter::new);

        CONSTRUCTORS.put(LocalDate.class, LocalDateReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(LocalDate.class, LocalDateArrayReaderWriter::new);

        CONSTRUCTORS.put(LocalDateTime.class, LocalDateTimeReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(LocalDateTime.class, LocalDateTimeArrayReaderWriter::new);

        CONSTRUCTORS.put(OffsetDateTime.class, OffsetDateTimeReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(OffsetDateTime.class, OffsetDateTimeArrayReaderWriter::new);

        CONSTRUCTORS.put(Boolean.class, NullableBooleanReaderWriter::new);
        CONSTRUCTORS.put(Boolean.TYPE, BooleanReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(Boolean.class, NullableBooleanArrayReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(Boolean.TYPE, BooleanArrayReaderWriter::new);

        CONSTRUCTORS.put(Byte.class, NullableByteReaderWriter::new);
        CONSTRUCTORS.put(Byte.TYPE, ByteReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(Byte.class, NullableByteArrayReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(Byte.TYPE, ByteArrayReaderWriter::new);

        CONSTRUCTORS.put(Character.class, NullableCharacterReaderWriter::new);
        CONSTRUCTORS.put(Character.TYPE, CharacterReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(Character.class, NullableCharacterArrayReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(Character.TYPE, CharArrayReaderWriter::new);

        CONSTRUCTORS.put(Short.class, NullableShortReaderWriter::new);
        CONSTRUCTORS.put(Short.TYPE, ShortReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(Short.class, NullableShortArrayReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(Short.TYPE, ShortArrayReaderWriter::new);

        CONSTRUCTORS.put(Integer.class, NullableIntegerReaderWriter::new);
        CONSTRUCTORS.put(Integer.TYPE, IntegerReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(Integer.class, NullableIntegerArrayReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(Integer.TYPE, IntArrayReaderWriter::new);

        CONSTRUCTORS.put(Long.class, NullableLongReaderWriter::new);
        CONSTRUCTORS.put(Long.TYPE, LongReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(Long.class, NullableLongArrayReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(Long.TYPE, LongArrayReaderWriter::new);

        CONSTRUCTORS.put(Float.class, NullableFloatReaderWriter::new);
        CONSTRUCTORS.put(Float.TYPE, FloatReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(Float.class, NullableFloatArrayReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(Float.TYPE, FloatArrayReaderWriter::new);

        CONSTRUCTORS.put(Double.class, NullableDoubleReaderWriter::new);
        CONSTRUCTORS.put(Double.TYPE, DoubleReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(Double.class, NullableDoubleArrayReaderWriter::new);
        ARRAY_CONSTRUCTORS.put(Double.TYPE, DoubleArrayReaderWriter::new);
    }

    private ValueReaderWriters() {
    }

    /**
     * Returns the reader writer for the given {@code type}.
     *
     * @param clazz       Top level class
     * @param type        Class to return the reader writer for
     * @param genericType Generic type of the {@code type}
     * @param fieldName   Name of the field
     * @return Appropriate reader for the given {@code type}
     */
    public static ValueReaderWriter<?> readerWriterFor(CompactStreamSerializer compactStreamSerializer, Class<?> clazz,
                                                       Class<?> type, Type genericType, String fieldName) {
        if (type.isArray()) {
            Class<?> componentType = type.getComponentType();
            return createReaderWriterForArray(compactStreamSerializer, clazz, componentType, fieldName);
        } else if (isList(type)) {
            Class<?> componentType = getSingleComponentType(genericType);
            ValueReaderWriter readerWriter = createReaderWriterForArray(compactStreamSerializer, clazz,
                    componentType, fieldName);
            return new ArrayListReaderWriter(fieldName, componentType, readerWriter);
        } else if (isSet(type)) {
            Class<?> componentType = getSingleComponentType(genericType);
            ValueReaderWriter readerWriter = createReaderWriterForArray(compactStreamSerializer, clazz,
                    componentType, fieldName);
            return new HashSetReaderWriter(fieldName, componentType, readerWriter);
        } else if (isMap(type)) {
            BiTuple<Class<?>, Class<?>> componentTypes = getTupleComponentTypes(genericType);
            ValueReaderWriter keyReaderWriter
                    = createReaderWriterForArray(compactStreamSerializer,
                    clazz, componentTypes.element1, fieldName + "!keys");
            ValueReaderWriter valueReaderWriter
                    = createReaderWriterForArray(compactStreamSerializer, clazz, componentTypes.element2, fieldName + "!values");
            return new HashMapReaderWriter(fieldName, componentTypes.element1,
                    componentTypes.element2, keyReaderWriter, valueReaderWriter);
        } else if (type.isEnum()) {
            return new EnumReaderWriter(fieldName, (Class<? extends Enum>) type);
        }

        Function<String, ValueReaderWriter<?>> constructor = CONSTRUCTORS.get(type);
        if (constructor != null) {
            return constructor.apply(fieldName);
        }

        boolean isRegisteredAsCompact = compactStreamSerializer.isRegisteredAsCompact(type);
        // We allow serializing classes regardless of the following checks if there is an explicit serializer for them.
        if (!isRegisteredAsCompact) {
            // The nested field might not be Compact serializable
            verifyFieldClassIsCompactSerializable(type, clazz);
            verifyFieldClassShouldBeSerializedAsCompact(compactStreamSerializer, type, clazz);
        }
        return new CompactReaderWriter(fieldName);
    }

    private static ValueReaderWriter createReaderWriterForArray(CompactStreamSerializer compactStreamSerializer,
                                                                Class<?> clazz, Class<?> componentType,
                                                                String fieldName) {
        if (componentType.isEnum()) {
            return new EnumArrayReaderWriter(fieldName, (Class<? extends Enum>) componentType);
        }

        Function<String, ValueReaderWriter<?>> constructor = ARRAY_CONSTRUCTORS.get(componentType);
        if (constructor != null) {
            return constructor.apply(fieldName);
        }

        boolean isRegisteredAsCompact = compactStreamSerializer.isRegisteredAsCompact(componentType);
        // We allow serializing classes regardless of the following checks if there is an explicit serializer for them.
        if (!isRegisteredAsCompact) {
            // Elements of the array might not be Compact serializable
            verifyFieldClassIsCompactSerializable(componentType, clazz);
            verifyFieldClassShouldBeSerializedAsCompact(compactStreamSerializer, componentType, clazz);
        }
        return new CompactArrayReaderWriter(fieldName, componentType);
    }

    private static boolean isList(Class<?> clazz) {
        return List.class.equals(clazz) || ArrayList.class.equals(clazz);
    }

    private static boolean isSet(Class<?> clazz) {
        return Set.class.equals(clazz) || HashSet.class.equals(clazz);
    }

    private static boolean isMap(Class<?> clazz) {
        return Map.class.equals(clazz) || HashMap.class.equals(clazz);
    }

    private static Class<?> getSingleComponentType(Type genericType) {
        if (!(genericType instanceof ParameterizedType)) {
            throw new HazelcastSerializationException(
                    "It is required that the type " + genericType + " must be parameterized."
            );
        }

        ParameterizedType parameterizedType = (ParameterizedType) genericType;
        Type[] typeArguments = parameterizedType.getActualTypeArguments();
        if (typeArguments.length != 1) {
            throw new HazelcastSerializationException(
                    "Expected type " + genericType + " to have a single type argument."
            );
        }

        Type typeArgument = typeArguments[0];
        if (!(typeArgument instanceof Class)) {
            throw new HazelcastSerializationException(
                    "Expected type argument of type " + genericType + " to be a class"
            );
        }

        return (Class<?>) typeArgument;
    }

    private static BiTuple<Class<?>, Class<?>> getTupleComponentTypes(Type genericType) {
        if (!(genericType instanceof ParameterizedType)) {
            throw new HazelcastSerializationException(
                    "Expected the type " + genericType + " to be parameterized."
            );
        }

        ParameterizedType parameterizedType = (ParameterizedType) genericType;
        Type[] typeArguments = parameterizedType.getActualTypeArguments();
        if (typeArguments.length != 2) {
            throw new HazelcastSerializationException(
                    "Expected type " + genericType + " to have two type arguments."
            );
        }

        Type keyTypeArgument = typeArguments[0];
        Type valueTypeArgument = typeArguments[1];
        if (!(keyTypeArgument instanceof Class) || !(valueTypeArgument instanceof Class)) {
            throw new HazelcastSerializationException(
                    "Expected type arguments of type " + genericType + " to be classes"
            );
        }

        return BiTuple.of((Class<?>) keyTypeArgument, (Class<?>) valueTypeArgument);
    }

    private static final class StringReaderWriter extends ValueReaderWriter<String> {

        private StringReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public String read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, STRING)) {
                return null;
            }
            return reader.readString(fieldName);
        }

        @Override
        public void write(CompactWriter writer, String value) {
            writer.writeString(fieldName, value);
        }
    }

    private static final class StringArrayReaderWriter extends ValueReaderWriter<String[]> {

        private StringArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public String[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_STRING)) {
                return null;
            }
            return reader.readArrayOfString(fieldName);
        }

        @Override
        public void write(CompactWriter writer, String[] value) {
            writer.writeArrayOfString(fieldName, value);
        }
    }

    private static final class BigDecimalReaderWriter extends ValueReaderWriter<BigDecimal> {

        private BigDecimalReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public BigDecimal read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, DECIMAL)) {
                return null;
            }
            return reader.readDecimal(fieldName);
        }

        @Override
        public void write(CompactWriter writer, BigDecimal value) {
            writer.writeDecimal(fieldName, value);
        }
    }

    private static final class BigDecimalArrayReaderWriter extends ValueReaderWriter<BigDecimal[]> {

        private BigDecimalArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public BigDecimal[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_DECIMAL)) {
                return null;
            }
            return reader.readArrayOfDecimal(fieldName);
        }

        @Override
        public void write(CompactWriter writer, BigDecimal[] value) {
            writer.writeArrayOfDecimal(fieldName, value);
        }
    }

    private static final class LocalTimeReaderWriter extends ValueReaderWriter<LocalTime> {

        private LocalTimeReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public LocalTime read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, TIME)) {
                return null;
            }
            return reader.readTime(fieldName);
        }

        @Override
        public void write(CompactWriter writer, LocalTime value) {
            writer.writeTime(fieldName, value);
        }
    }

    private static final class LocalTimeArrayReaderWriter extends ValueReaderWriter<LocalTime[]> {

        private LocalTimeArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public LocalTime[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_TIME)) {
                return null;
            }
            return reader.readArrayOfTime(fieldName);
        }

        @Override
        public void write(CompactWriter writer, LocalTime[] value) {
            writer.writeArrayOfTime(fieldName, value);
        }
    }

    private static final class LocalDateReaderWriter extends ValueReaderWriter<LocalDate> {

        private LocalDateReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public LocalDate read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, DATE)) {
                return null;
            }
            return reader.readDate(fieldName);
        }

        @Override
        public void write(CompactWriter writer, LocalDate value) {
            writer.writeDate(fieldName, value);
        }
    }

    private static final class LocalDateArrayReaderWriter extends ValueReaderWriter<LocalDate[]> {

        private LocalDateArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public LocalDate[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_DATE)) {
                return null;
            }
            return reader.readArrayOfDate(fieldName);
        }

        @Override
        public void write(CompactWriter writer, LocalDate[] value) {
            writer.writeArrayOfDate(fieldName, value);
        }
    }

    private static final class LocalDateTimeReaderWriter extends ValueReaderWriter<LocalDateTime> {

        private LocalDateTimeReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public LocalDateTime read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, TIMESTAMP)) {
                return null;
            }
            return reader.readTimestamp(fieldName);
        }

        @Override
        public void write(CompactWriter writer, LocalDateTime value) {
            writer.writeTimestamp(fieldName, value);
        }
    }

    private static final class LocalDateTimeArrayReaderWriter extends ValueReaderWriter<LocalDateTime[]> {

        private LocalDateTimeArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public LocalDateTime[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_TIMESTAMP)) {
                return null;
            }
            return reader.readArrayOfTimestamp(fieldName);
        }

        @Override
        public void write(CompactWriter writer, LocalDateTime[] value) {
            writer.writeArrayOfTimestamp(fieldName, value);
        }
    }

    private static final class OffsetDateTimeReaderWriter extends ValueReaderWriter<OffsetDateTime> {

        private OffsetDateTimeReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public OffsetDateTime read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, TIMESTAMP_WITH_TIMEZONE)) {
                return null;
            }
            return reader.readTimestampWithTimezone(fieldName);
        }

        @Override
        public void write(CompactWriter writer, OffsetDateTime value) {
            writer.writeTimestampWithTimezone(fieldName, value);
        }
    }

    private static final class OffsetDateTimeArrayReaderWriter extends ValueReaderWriter<OffsetDateTime[]> {

        private OffsetDateTimeArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public OffsetDateTime[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_TIMESTAMP_WITH_TIMEZONE)) {
                return null;
            }
            return reader.readArrayOfTimestampWithTimezone(fieldName);
        }

        @Override
        public void write(CompactWriter writer, OffsetDateTime[] value) {
            writer.writeArrayOfTimestampWithTimezone(fieldName, value);
        }
    }

    private static final class NullableBooleanReaderWriter extends ValueReaderWriter<Boolean> {

        private NullableBooleanReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Boolean read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, NULLABLE_BOOLEAN, BOOLEAN)) {
                return null;
            }
            return reader.readNullableBoolean(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Boolean value) {
            writer.writeNullableBoolean(fieldName, value);
        }
    }

    private static final class BooleanReaderWriter extends ValueReaderWriter<Boolean> {

        private BooleanReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Boolean read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, BOOLEAN, NULLABLE_BOOLEAN)) {
                return false;
            }
            return reader.readBoolean(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Boolean value) {
            writer.writeBoolean(fieldName, value);
        }
    }

    private static final class NullableBooleanArrayReaderWriter extends ValueReaderWriter<Boolean[]> {

        private NullableBooleanArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Boolean[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_NULLABLE_BOOLEAN, ARRAY_OF_BOOLEAN)) {
                return null;
            }
            return reader.readArrayOfNullableBoolean(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Boolean[] value) {
            writer.writeArrayOfNullableBoolean(fieldName, value);
        }
    }

    private static final class BooleanArrayReaderWriter extends ValueReaderWriter<boolean[]> {

        private BooleanArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public boolean[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_BOOLEAN, ARRAY_OF_NULLABLE_BOOLEAN)) {
                return null;
            }
            return reader.readArrayOfBoolean(fieldName);
        }

        @Override
        public void write(CompactWriter writer, boolean[] value) {
            writer.writeArrayOfBoolean(fieldName, value);
        }
    }

    private static final class NullableByteReaderWriter extends ValueReaderWriter<Byte> {

        private NullableByteReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Byte read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, NULLABLE_INT8, INT8)) {
                return null;
            }
            return reader.readNullableInt8(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Byte value) {
            writer.writeNullableInt8(fieldName, value);
        }
    }

    private static final class ByteReaderWriter extends ValueReaderWriter<Byte> {

        private ByteReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Byte read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, INT8, NULLABLE_INT8)) {
                return 0;
            }
            return reader.readInt8(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Byte value) {
            writer.writeInt8(fieldName, value);
        }
    }

    private static final class NullableByteArrayReaderWriter extends ValueReaderWriter<Byte[]> {

        private NullableByteArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Byte[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_NULLABLE_INT8, ARRAY_OF_INT8)) {
                return null;
            }
            return reader.readArrayOfNullableInt8(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Byte[] value) {
            writer.writeArrayOfNullableInt8(fieldName, value);
        }
    }

    private static final class ByteArrayReaderWriter extends ValueReaderWriter<byte[]> {

        private ByteArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public byte[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_INT8, ARRAY_OF_NULLABLE_INT8)) {
                return null;
            }
            return reader.readArrayOfInt8(fieldName);
        }

        @Override
        public void write(CompactWriter writer, byte[] value) {
            writer.writeArrayOfInt8(fieldName, value);
        }
    }

    private static final class NullableCharacterReaderWriter extends ValueReaderWriter<Character> {

        private NullableCharacterReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Character read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, NULLABLE_INT16, INT16)) {
                return null;
            }
            return characterFromShort(reader.readNullableInt16(fieldName));
        }

        @Override
        public void write(CompactWriter writer, Character value) {
            writer.writeNullableInt16(fieldName, characterAsShort(value));
        }
    }

    private static final class CharacterReaderWriter extends ValueReaderWriter<Character> {

        private CharacterReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Character read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, INT16, NULLABLE_INT16)) {
                return 0;
            }
            return (char) reader.readInt16(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Character value) {
            writer.writeInt16(fieldName, (short) ((char) value));
        }
    }

    private static final class NullableCharacterArrayReaderWriter extends ValueReaderWriter<Character[]> {

        private NullableCharacterArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Character[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_NULLABLE_INT16, ARRAY_OF_INT16)) {
                return null;
            }
            return characterArrayFromShortArray(reader.readArrayOfNullableInt16(fieldName));
        }

        @Override
        public void write(CompactWriter writer, Character[] value) {
            writer.writeArrayOfNullableInt16(fieldName, characterArrayAsShortArray(value));
        }
    }

    private static final class CharArrayReaderWriter extends ValueReaderWriter<char[]> {

        private CharArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public char[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_INT16, ARRAY_OF_NULLABLE_INT16)) {
                return null;
            }
            return charArrayFromShortArray(reader.readArrayOfInt16(fieldName));
        }

        @Override
        public void write(CompactWriter writer, char[] value) {
            writer.writeArrayOfInt16(fieldName, charArrayAsShortArray(value));
        }
    }

    private static final class NullableShortReaderWriter extends ValueReaderWriter<Short> {

        private NullableShortReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Short read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, NULLABLE_INT16, INT16)) {
                return null;
            }
            return reader.readNullableInt16(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Short value) {
            writer.writeNullableInt16(fieldName, value);
        }
    }

    private static final class ShortReaderWriter extends ValueReaderWriter<Short> {

        private ShortReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Short read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, INT16, NULLABLE_INT16)) {
                return 0;
            }
            return reader.readInt16(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Short value) {
            writer.writeInt16(fieldName, value);
        }
    }

    private static final class NullableShortArrayReaderWriter extends ValueReaderWriter<Short[]> {

        private NullableShortArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Short[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_NULLABLE_INT16, ARRAY_OF_INT16)) {
                return null;
            }
            return reader.readArrayOfNullableInt16(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Short[] value) {
            writer.writeArrayOfNullableInt16(fieldName, value);
        }
    }

    private static final class ShortArrayReaderWriter extends ValueReaderWriter<short[]> {

        private ShortArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public short[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_INT16, ARRAY_OF_NULLABLE_INT16)) {
                return null;
            }
            return reader.readArrayOfInt16(fieldName);
        }

        @Override
        public void write(CompactWriter writer, short[] value) {
            writer.writeArrayOfInt16(fieldName, value);
        }
    }

    private static final class NullableIntegerReaderWriter extends ValueReaderWriter<Integer> {

        private NullableIntegerReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Integer read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, NULLABLE_INT32, INT32)) {
                return null;
            }
            return reader.readNullableInt32(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Integer value) {
            writer.writeNullableInt32(fieldName, value);
        }
    }

    private static final class IntegerReaderWriter extends ValueReaderWriter<Integer> {

        private IntegerReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Integer read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, INT32, NULLABLE_INT32)) {
                return 0;
            }
            return reader.readInt32(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Integer value) {
            writer.writeInt32(fieldName, value);
        }
    }

    private static final class NullableIntegerArrayReaderWriter extends ValueReaderWriter<Integer[]> {

        private NullableIntegerArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Integer[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_NULLABLE_INT32, ARRAY_OF_INT32)) {
                return null;
            }
            return reader.readArrayOfNullableInt32(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Integer[] value) {
            writer.writeArrayOfNullableInt32(fieldName, value);
        }
    }

    private static final class IntArrayReaderWriter extends ValueReaderWriter<int[]> {

        private IntArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public int[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_INT32, ARRAY_OF_NULLABLE_INT32)) {
                return null;
            }
            return reader.readArrayOfInt32(fieldName);
        }

        @Override
        public void write(CompactWriter writer, int[] value) {
            writer.writeArrayOfInt32(fieldName, value);
        }
    }

    private static final class NullableLongReaderWriter extends ValueReaderWriter<Long> {

        private NullableLongReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Long read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, NULLABLE_INT64, INT64)) {
                return null;
            }
            return reader.readNullableInt64(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Long value) {
            writer.writeNullableInt64(fieldName, value);
        }
    }

    private static final class LongReaderWriter extends ValueReaderWriter<Long> {

        private LongReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Long read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, INT64, NULLABLE_INT64)) {
                return 0L;
            }
            return reader.readInt64(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Long value) {
            writer.writeInt64(fieldName, value);
        }
    }

    private static final class NullableLongArrayReaderWriter extends ValueReaderWriter<Long[]> {

        private NullableLongArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Long[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_NULLABLE_INT64, ARRAY_OF_INT64)) {
                return null;
            }
            return reader.readArrayOfNullableInt64(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Long[] value) {
            writer.writeArrayOfNullableInt64(fieldName, value);
        }
    }

    private static final class LongArrayReaderWriter extends ValueReaderWriter<long[]> {

        private LongArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public long[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_INT64, ARRAY_OF_NULLABLE_INT64)) {
                return null;
            }
            return reader.readArrayOfInt64(fieldName);
        }

        @Override
        public void write(CompactWriter writer, long[] value) {
            writer.writeArrayOfInt64(fieldName, value);
        }
    }

    private static final class NullableFloatReaderWriter extends ValueReaderWriter<Float> {

        private NullableFloatReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Float read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, NULLABLE_FLOAT32, FLOAT32)) {
                return null;
            }
            return reader.readNullableFloat32(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Float value) {
            writer.writeNullableFloat32(fieldName, value);
        }
    }

    private static final class FloatReaderWriter extends ValueReaderWriter<Float> {

        private FloatReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Float read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, FLOAT32, NULLABLE_FLOAT32)) {
                return 0F;
            }
            return reader.readFloat32(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Float value) {
            writer.writeFloat32(fieldName, value);
        }
    }

    private static final class NullableFloatArrayReaderWriter extends ValueReaderWriter<Float[]> {

        private NullableFloatArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Float[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_NULLABLE_FLOAT32, ARRAY_OF_FLOAT32)) {
                return null;
            }
            return reader.readArrayOfNullableFloat32(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Float[] value) {
            writer.writeArrayOfNullableFloat32(fieldName, value);
        }
    }

    private static final class FloatArrayReaderWriter extends ValueReaderWriter<float[]> {

        private FloatArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public float[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_FLOAT32, ARRAY_OF_NULLABLE_FLOAT32)) {
                return null;
            }
            return reader.readArrayOfFloat32(fieldName);
        }

        @Override
        public void write(CompactWriter writer, float[] value) {
            writer.writeArrayOfFloat32(fieldName, value);
        }
    }

    private static final class NullableDoubleReaderWriter extends ValueReaderWriter<Double> {

        private NullableDoubleReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Double read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, NULLABLE_FLOAT64, FLOAT64)) {
                return null;
            }
            return reader.readNullableFloat64(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Double value) {
            writer.writeNullableFloat64(fieldName, value);
        }
    }

    private static final class DoubleReaderWriter extends ValueReaderWriter<Double> {

        private DoubleReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Double read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, FLOAT64, NULLABLE_FLOAT64)) {
                return 0D;
            }
            return reader.readFloat64(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Double value) {
            writer.writeFloat64(fieldName, value);
        }
    }

    private static final class NullableDoubleArrayReaderWriter extends ValueReaderWriter<Double[]> {

        private NullableDoubleArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Double[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_NULLABLE_FLOAT64, ARRAY_OF_FLOAT64)) {
                return null;
            }
            return reader.readArrayOfNullableFloat64(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Double[] value) {
            writer.writeArrayOfNullableFloat64(fieldName, value);
        }
    }

    private static final class DoubleArrayReaderWriter extends ValueReaderWriter<double[]> {

        private DoubleArrayReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public double[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_FLOAT64, ARRAY_OF_NULLABLE_FLOAT64)) {
                return null;
            }
            return reader.readArrayOfFloat64(fieldName);
        }

        @Override
        public void write(CompactWriter writer, double[] value) {
            writer.writeArrayOfFloat64(fieldName, value);
        }
    }

    private static final class EnumReaderWriter extends ValueReaderWriter<Enum> {

        private final Class<? extends Enum> clazz;

        private EnumReaderWriter(String fieldName, Class<? extends Enum> clazz) {
            super(fieldName);
            this.clazz = clazz;
        }

        @Override
        public Enum read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, STRING)) {
                return null;
            }

            String value = reader.readString(fieldName);
            return enumFromStringName(clazz, value);
        }

        @Override
        public void write(CompactWriter writer, Enum value) {
            writer.writeString(fieldName, enumAsStringName(value));
        }
    }

    private static final class EnumArrayReaderWriter extends ValueReaderWriter<Enum[]> {

        private final Class<? extends Enum> clazz;

        private EnumArrayReaderWriter(String fieldName, Class<? extends Enum> clazz) {
            super(fieldName);
            this.clazz = clazz;
        }

        @Override
        public Enum[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_STRING)) {
                return null;
            }

            String[] values = reader.readArrayOfString(fieldName);
            return enumArrayFromStringNameArray(clazz, values);
        }

        @Override
        public void write(CompactWriter writer, Enum[] value) {
            writer.writeArrayOfString(fieldName, enumArrayAsStringNameArray(value));
        }
    }

    private static final class CompactReaderWriter extends ValueReaderWriter<Object> {

        private CompactReaderWriter(String fieldName) {
            super(fieldName);
        }

        @Override
        public Object read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, COMPACT)) {
                return null;
            }

            return reader.readCompact(fieldName);
        }

        @Override
        public void write(CompactWriter writer, Object value) {
            writer.writeCompact(fieldName, value);
        }
    }

    private static final class CompactArrayReaderWriter extends ValueReaderWriter<Object[]> {

        private final Class<?> clazz;

        private CompactArrayReaderWriter(String fieldName, Class<?> clazz) {
            super(fieldName);
            this.clazz = clazz;
        }

        @Override
        public Object[] read(CompactReader reader, Schema schema) {
            if (!isFieldExist(schema, fieldName, ARRAY_OF_COMPACT)) {
                return null;
            }

            return reader.readArrayOfCompact(fieldName, clazz);
        }

        @Override
        public void write(CompactWriter writer, Object[] value) {
            writer.writeArrayOfCompact(fieldName, value);
        }
    }

    private static final class ArrayListReaderWriter extends ValueReaderWriter<List> {

        private final Class<?> componentType;
        private final ValueReaderWriter valueReaderWriter;

        private ArrayListReaderWriter(String fieldName, Class<?> componentType, ValueReaderWriter valueReaderWriter) {
            super(fieldName);
            this.componentType = componentType;
            this.valueReaderWriter = valueReaderWriter;
        }

        @Override
        public List read(CompactReader reader, Schema schema) {
            Object[] value = (Object[]) valueReaderWriter.read(reader, schema);
            if (value == null) {
                return null;
            }

            return new ArrayList(Arrays.asList(value));
        }

        @Override
        public void write(CompactWriter writer, List value) {
            if (value == null) {
                valueReaderWriter.write(writer, null);
                return;
            }

            Object o = Array.newInstance(componentType, value.size());
            Object array = value.toArray((Object[]) o);
            valueReaderWriter.write(writer, array);
        }
    }

    private static final class HashSetReaderWriter extends ValueReaderWriter<Set> {

        private final Class<?> componentType;
        private final ValueReaderWriter valueReaderWriter;

        private HashSetReaderWriter(String fieldName, Class<?> componentType, ValueReaderWriter valueReaderWriter) {
            super(fieldName);
            this.componentType = componentType;
            this.valueReaderWriter = valueReaderWriter;
        }

        @Override
        public Set read(CompactReader reader, Schema schema) {
            Object[] value = (Object[]) valueReaderWriter.read(reader, schema);
            if (value == null) {
                return null;
            }

            return new HashSet(Arrays.asList(value));
        }

        @Override
        public void write(CompactWriter writer, Set value) {
            if (value == null) {
                valueReaderWriter.write(writer, null);
                return;
            }

            Object o = Array.newInstance(componentType, value.size());
            Object array = value.toArray((Object[]) o);
            valueReaderWriter.write(writer, array);
        }
    }

    private static final class HashMapReaderWriter extends ValueReaderWriter<Map> {

        private final Class<?> keyComponentType;
        private final Class<?> valueComponentType;
        private final ValueReaderWriter keyReaderWriter;
        private final ValueReaderWriter valueReaderWriter;

        private HashMapReaderWriter(String fieldName,
                                    Class<?> keyComponentType,
                                    Class<?> valueComponentType,
                                    ValueReaderWriter keyReaderWriter,
                                    ValueReaderWriter valueReaderWriter) {
            super(fieldName);
            this.keyComponentType = keyComponentType;
            this.valueComponentType = valueComponentType;
            this.keyReaderWriter = keyReaderWriter;
            this.valueReaderWriter = valueReaderWriter;
        }

        @Override
        public Map read(CompactReader reader, Schema schema) {
            Object[] keys = (Object[]) keyReaderWriter.read(reader, schema);
            Object[] values = (Object[]) valueReaderWriter.read(reader, schema);
            if (keys == null || values == null) {
                return null;
            }

            HashMap map = new HashMap(keys.length);
            for (int i = 0; i < keys.length; i++) {
                map.put(keys[i], values[i]);
            }
            return map;
        }

        @Override
        public void write(CompactWriter writer, Map value) {
            if (value == null) {
                keyReaderWriter.write(writer, null);
                valueReaderWriter.write(writer, null);
                return;
            }

            Object keys = Array.newInstance(keyComponentType, value.size());
            Object values = Array.newInstance(valueComponentType, value.size());

            keys = value.keySet().toArray((Object[]) keys);
            values = value.values().toArray((Object[]) values);

            keyReaderWriter.write(writer, keys);
            valueReaderWriter.write(writer, values);
        }
    }
}
