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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.internal.serialization.impl.compact.zeroconfig.ValueReaderWriter;
import com.hazelcast.internal.serialization.impl.compact.zeroconfig.ValueReaderWriters;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.internal.nio.InstanceCreationUtil.createNewInstance;
import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.isFieldExist;
import static com.hazelcast.nio.serialization.FieldKind.BOOLEAN;
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
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Reflective serializer works for Compact format in zero-config case.
 * Specifically when explicit serializer is not given via
 * {@link com.hazelcast.config.CompactSerializationConfig#addSerializer(CompactSerializer)}
 * or when a class is registered as reflectively serializable with
 * {@link com.hazelcast.config.CompactSerializationConfig#addClass(Class)}.
 * <p>
 * ReflectiveCompactSerializer can de/serialize classes having an accessible empty constructor only.
 * Only types in {@link CompactWriter}/{@link CompactReader} interface are supported as fields.
 * For any other class as the field type, it will work recursively and try to de/serialize a sub-class.
 * Thus, if any sub-fields does not have an accessible empty constructor, deserialization fails with
 * HazelcastSerializationException.
 */
public class ReflectiveCompactSerializer<T> implements CompactSerializer<T> {

    private final Map<Class, ReaderWriter[]> readerWritersCache = new ConcurrentHashMap<>();
    private final CompactStreamSerializer compactStreamSerializer;

    public ReflectiveCompactSerializer(CompactStreamSerializer compactStreamSerializer) {
        this.compactStreamSerializer = compactStreamSerializer;
    }

    @Override
    public void write(@Nonnull CompactWriter writer, @Nonnull T object) {
        Class<?> clazz = object.getClass();
        if (writeFast(clazz, writer, object)) {
            return;
        }
        createFastReadWriteCaches(clazz);
        writeFast(clazz, writer, object);
    }

    @Nonnull
    @Override
    public String getTypeName() {
        throw new IllegalStateException("getTypeName should not be called for the reflective serializer");
    }

    @Nonnull
    @Override
    public Class<T> getCompactClass() {
        throw new IllegalStateException("getCompactClass should not be called for the reflective serializer");
    }

    private boolean writeFast(Class clazz, CompactWriter compactWriter, Object object) {
        ReaderWriter[] readerWriters = readerWritersCache.get(clazz);
        if (readerWriters == null) {
            return false;
        }
        for (ReaderWriter readerWriter : readerWriters) {
            try {
                readerWriter.write(compactWriter, object);
            } catch (Exception e) {
                throw new HazelcastSerializationException(e);
            }
        }
        return true;
    }

    private boolean readFast(Class clazz, DefaultCompactReader compactReader, Object object) {
        ReaderWriter[] readerWriters = readerWritersCache.get(clazz);
        if (readerWriters == null) {
            return false;
        }

        Schema schema = compactReader.getSchema();
        for (ReaderWriter readerWriter : readerWriters) {
            try {
                readerWriter.read(compactReader, schema, object);
            } catch (Exception e) {
                throw new HazelcastSerializationException(e);
            }
        }
        return true;
    }

    @Nonnull
    @Override
    public T read(@Nonnull CompactReader reader) {
        // We always fed DefaultCompactReader to this serializer.
        DefaultCompactReader compactReader = (DefaultCompactReader) reader;
        Class associatedClass = requireNonNull(compactReader.getAssociatedClass(),
                "AssociatedClass is required for ReflectiveCompactSerializer");

        T object;
        object = (T) createObject(associatedClass);
        if (readFast(associatedClass, compactReader, object)) {
            return object;
        }
        createFastReadWriteCaches(associatedClass);
        readFast(associatedClass, compactReader, object);
        return object;
    }

    @Nonnull
    private Object createObject(Class associatedClass) {
        try {
            return createNewInstance(associatedClass);
        } catch (Exception e) {
            throw new HazelcastSerializationException("Could not construct the class " + associatedClass, e);
        }
    }

    private static List<Field> getAllFields(List<Field> fields, Class<?> type) {
        fields.addAll(Arrays.stream(type.getDeclaredFields())
                .filter(f -> !Modifier.isStatic(f.getModifiers()))
                .filter(f -> !Modifier.isTransient(f.getModifiers()))
                .collect(toList()));
        if (type.getSuperclass() != null && type.getSuperclass() != Object.class) {
            getAllFields(fields, type.getSuperclass());
        }
        return fields;
    }

    private void createFastReadWriteCaches(Class clazz) {
        // The top level class might not be Compact serializable
        CompactUtil.verifyClassIsCompactSerializable(clazz);

        // get inherited fields as well
        List<Field> allFields = getAllFields(new LinkedList<>(), clazz);
        ReaderWriter[] readerWriters = new ReaderWriter[allFields.size()];

        int index = 0;
        for (Field field : allFields) {
            field.setAccessible(true);
            Class<?> type = field.getType();
            String name = field.getName();


            // Use normal reader-writers for the primitive types to avoid boxing-unboxing
            if (Byte.TYPE.equals(type)) {
                readerWriters[index] = new ReaderWriter() {
                    @Override
                    public void read(CompactReader reader, Schema schema, Object o) throws Exception {
                        if (isFieldExist(schema, name, INT8, NULLABLE_INT8)) {
                            field.setByte(o, reader.readInt8(name));
                        }
                    }

                    @Override
                    public void write(CompactWriter writer, Object o) throws Exception {
                        writer.writeInt8(name, field.getByte(o));
                    }
                };
            } else if (Character.TYPE.equals(type)) {
                readerWriters[index] = new ReaderWriter() {
                    @Override
                    public void read(CompactReader reader, Schema schema, Object o) throws Exception {
                        if (isFieldExist(schema, name, INT16, NULLABLE_INT16)) {
                            field.setChar(o, (char) reader.readInt16(name));
                        }
                    }

                    @Override
                    public void write(CompactWriter writer, Object o) throws Exception {
                        writer.writeInt16(name, (short) field.getChar(o));
                    }
                };
            } else if (Short.TYPE.equals(type)) {
                readerWriters[index] = new ReaderWriter() {
                    @Override
                    public void read(CompactReader reader, Schema schema, Object o) throws Exception {
                        if (isFieldExist(schema, name, INT16, NULLABLE_INT16)) {
                            field.setShort(o, reader.readInt16(name));
                        }
                    }

                    @Override
                    public void write(CompactWriter writer, Object o) throws Exception {
                        writer.writeInt16(name, field.getShort(o));
                    }
                };
            } else if (Integer.TYPE.equals(type)) {
                readerWriters[index] = new ReaderWriter() {
                    @Override
                    public void read(CompactReader reader, Schema schema, Object o) throws Exception {
                        if (isFieldExist(schema, name, INT32, NULLABLE_INT32)) {
                            field.setInt(o, reader.readInt32(name));
                        }
                    }

                    @Override
                    public void write(CompactWriter writer, Object o) throws Exception {
                        writer.writeInt32(name, field.getInt(o));
                    }
                };
            } else if (Long.TYPE.equals(type)) {
                readerWriters[index] = new ReaderWriter() {
                    @Override
                    public void read(CompactReader reader, Schema schema, Object o) throws Exception {
                        if (isFieldExist(schema, name, INT64, NULLABLE_INT64)) {
                            field.setLong(o, reader.readInt64(name));
                        }
                    }

                    @Override
                    public void write(CompactWriter writer, Object o) throws Exception {
                        writer.writeInt64(name, field.getLong(o));
                    }
                };
            } else if (Float.TYPE.equals(type)) {
                readerWriters[index] = new ReaderWriter() {
                    @Override
                    public void read(CompactReader reader, Schema schema, Object o) throws Exception {
                        if (isFieldExist(schema, name, FLOAT32, NULLABLE_FLOAT32)) {
                            field.setFloat(o, reader.readFloat32(name));
                        }
                    }

                    @Override
                    public void write(CompactWriter writer, Object o) throws Exception {
                        writer.writeFloat32(name, field.getFloat(o));
                    }
                };
            } else if (Double.TYPE.equals(type)) {
                readerWriters[index] = new ReaderWriter() {
                    @Override
                    public void read(CompactReader reader, Schema schema, Object o) throws Exception {
                        if (isFieldExist(schema, name, FLOAT64, NULLABLE_FLOAT64)) {
                            field.setDouble(o, reader.readFloat64(name));
                        }
                    }

                    @Override
                    public void write(CompactWriter writer, Object o) throws Exception {
                        writer.writeFloat64(name, field.getDouble(o));
                    }
                };
            } else if (Boolean.TYPE.equals(type)) {
                readerWriters[index] = new ReaderWriter() {
                    @Override
                    public void read(CompactReader reader, Schema schema, Object o) throws Exception {
                        if (isFieldExist(schema, name, BOOLEAN, NULLABLE_BOOLEAN)) {
                            field.setBoolean(o, reader.readBoolean(name));
                        }
                    }

                    @Override
                    public void write(CompactWriter writer, Object o) throws Exception {
                        writer.writeBoolean(name, field.getBoolean(o));
                    }
                };
            } else {
                // For anything else, rely on value reader writers to re-use the code we have
                readerWriters[index] = new ReaderWriterAdapter(
                        ValueReaderWriters.readerWriterFor(compactStreamSerializer, clazz, type,
                                field.getGenericType(), name),
                        field
                );
            }

            index++;
        }

        readerWritersCache.put(clazz, readerWriters);
    }

    private static final class ReaderWriterAdapter implements ReaderWriter {

        private final ValueReaderWriter readerWriter;
        private final Field field;


        ReaderWriterAdapter(ValueReaderWriter readerWriter, Field field) {
            this.readerWriter = readerWriter;
            this.field = field;
        }

        @Override
        public void read(CompactReader reader, Schema schema, Object o) throws Exception {
            field.set(o, readerWriter.read(reader, schema));
        }

        @Override
        public void write(CompactWriter writer, Object o) throws Exception {
            readerWriter.write(writer, field.get(o));
        }
    }

    private interface ReaderWriter {
        void read(CompactReader reader, Schema schema, Object o) throws Exception;

        void write(CompactWriter writer, Object o) throws Exception;
    }
}
