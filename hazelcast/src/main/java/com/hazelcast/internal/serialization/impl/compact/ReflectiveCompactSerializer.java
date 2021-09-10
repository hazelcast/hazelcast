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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.nio.serialization.FieldType.*;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Reflective serializer works for Compact format in zero-config case.
 * Specifically when explicit serializer is not given via
 * {@link com.hazelcast.config.CompactSerializationConfig#register(Class, String, CompactSerializer)}
 * or when a class is registered as reflectively serializable with
 * {@link com.hazelcast.config.CompactSerializationConfig#register(Class)}.
 * <p>
 * ReflectiveCompactSerializer can de/serialize classes having an accessible empty constructor only.
 * Only types in {@link CompactWriter}/{@link CompactReader} interface are supported as fields.
 * For any other class as the field type, it will work recursively and try to de/serialize a sub-class.
 * Thus, if any sub-fields does not have an accessible empty constructor, deserialization fails with
 * HazelcastSerializationException.
 */
public class ReflectiveCompactSerializer implements CompactSerializer<Object> {

    private final Map<Class, Writer[]> writersCache = new ConcurrentHashMap<>();
    private final Map<Class, Reader[]> readersCache = new ConcurrentHashMap<>();

    @Override
    public void write(@Nonnull CompactWriter writer, @Nonnull Object object) throws IOException {
        Class<?> clazz = object.getClass();
        if (writeFast(clazz, writer, object)) {
            return;
        }
        createFastReadWriteCaches(clazz);
        writeFast(clazz, writer, object);
    }

    private boolean writeFast(Class clazz, CompactWriter compactWriter, Object object) throws IOException {
        Writer[] writers = writersCache.get(clazz);
        if (writers == null) {
            return false;
        }
        for (Writer writer : writers) {
            try {
                writer.write(compactWriter, object);
            } catch (Exception e) {
                ExceptionUtil.rethrow(e, IOException.class);
            }
        }
        return true;
    }

    private boolean readFast(Class clazz, DefaultCompactReader compactReader, Object object) throws IOException {
        Reader[] readers = readersCache.get(clazz);
        Schema schema = compactReader.getSchema();
        if (readers == null) {
            return false;
        }
        for (Reader reader : readers) {
            try {
                reader.read(compactReader, schema, object);
            } catch (Exception e) {
                ExceptionUtil.rethrow(e, IOException.class);
            }
        }
        return true;
    }

    @Nonnull
    @Override
    public Object read(@Nonnull CompactReader reader) throws IOException {
        // We always fed DefaultCompactReader to this serializer.
        DefaultCompactReader compactReader = (DefaultCompactReader) reader;
        Class associatedClass = requireNonNull(compactReader.getAssociatedClass(),
                "AssociatedClass is required for ReflectiveCompactSerializer");

        Object object;
        object = createObject(associatedClass);
        try {
            if (readFast(associatedClass, compactReader, object)) {
                return object;
            }
            createFastReadWriteCaches(associatedClass);
            readFast(associatedClass, compactReader, object);
            return object;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Nonnull
    private Object createObject(Class associatedClass) {
        try {
            return ClassLoaderUtil.newInstance(associatedClass.getClassLoader(), associatedClass);
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

    private boolean fieldExists(Schema schema, String name, FieldType fieldType) {
        FieldDescriptor fieldDescriptor = schema.getField(name);
        return fieldDescriptor != null && fieldDescriptor.getType().equals(fieldType);
    }

    private void createFastReadWriteCaches(Class clazz) throws IOException {
        //Create object to test if it is empty constructable to fail-fast on the write path
        createObject(clazz);

        //get inherited fields as well
        List<Field> allFields = getAllFields(new LinkedList<>(), clazz);
        Writer[] writers = new Writer[allFields.size()];
        Reader[] readers = new Reader[allFields.size()];

        int index = 0;
        for (Field field : allFields) {
            field.setAccessible(true);
            Class<?> type = field.getType();
            String name = field.getName();
            if (Byte.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, BYTE)) {
                        field.setByte(o, reader.readByte(name));
                    }
                };
                writers[index] = (w, o) -> w.writeByte(name, field.getByte(o));
            } else if (Short.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, SHORT)) {
                        field.setShort(o, reader.readShort(name));
                    }
                };
                writers[index] = (w, o) -> w.writeShort(name, field.getShort(o));
            } else if (Integer.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, INT)) {
                        field.setInt(o, reader.readInt(name));
                    }
                };
                writers[index] = (w, o) -> w.writeInt(name, field.getInt(o));
            } else if (Long.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, LONG)) {
                        field.setLong(o, reader.readLong(name));
                    }
                };
                writers[index] = (w, o) -> w.writeLong(name, field.getLong(o));
            } else if (Float.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, FLOAT)) {
                        field.setFloat(o, reader.readFloat(name));
                    }
                };
                writers[index] = (w, o) -> w.writeFloat(name, field.getFloat(o));
            } else if (Double.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, DOUBLE)) {
                        field.setDouble(o, reader.readDouble(name));
                    }
                };
                writers[index] = (w, o) -> w.writeDouble(name, field.getDouble(o));
            } else if (Boolean.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, BOOLEAN)) {
                        field.setBoolean(o, reader.readBoolean(name));
                    }
                };
                writers[index] = (w, o) -> w.writeBoolean(name, field.getBoolean(o));
            } else if (Character.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, CHAR)) {
                        field.setChar(o, reader.readChar(name));
                    }
                };
                writers[index] = (w, o) -> w.writeChar(name, field.getChar(o));
            } else if (String.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, UTF)) {
                        field.set(o, reader.readString(name));
                    }
                };
                writers[index] = (w, o) -> w.writeString(name, (String) field.get(o));
            } else if (BigDecimal.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, DECIMAL)) {
                        field.set(o, reader.readDecimal(name));
                    }
                };
                writers[index] = (w, o) -> w.writeDecimal(name, (BigDecimal) field.get(o));
            } else if (LocalTime.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, TIME)) {
                        field.set(o, reader.readTime(name));
                    }
                };
                writers[index] = (w, o) -> w.writeTime(name, (LocalTime) field.get(o));
            } else if (LocalDate.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, DATE)) {
                        field.set(o, reader.readDate(name));
                    }
                };
                writers[index] = (w, o) -> w.writeDate(name, (LocalDate) field.get(o));
            } else if (LocalDateTime.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, TIMESTAMP)) {
                        field.set(o, reader.readTimestamp(name));
                    }
                };
                writers[index] = (w, o) -> w.writeTimestamp(name, (LocalDateTime) field.get(o));
            } else if (OffsetDateTime.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, TIMESTAMP_WITH_TIMEZONE)) {
                        field.set(o, reader.readTimestampWithTimezone(name));
                    }
                };
                writers[index] = (w, o) -> w.writeTimestampWithTimezone(name, (OffsetDateTime) field.get(o));
            } else if (type.isEnum()) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, UTF)) {
                        String enumName = reader.readString(name);
                        field.set(o, enumName == null ? null : Enum.valueOf((Class<? extends Enum>) type, enumName));
                    }
                };
                writers[index] = (w, o) -> {
                    Object rawValue = field.get(o);
                    String value = rawValue == null ? null : ((Enum) rawValue).name();
                    w.writeString(name, value);
                };
            } else if (type.isArray()) {
                Class<?> componentType = type.getComponentType();
                if (Byte.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, BYTE_ARRAY)) {
                            field.set(o, reader.readByteArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeByteArray(name, (byte[]) field.get(o));
                } else if (Short.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, SHORT_ARRAY)) {
                            field.set(o, reader.readShortArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeShortArray(name, (short[]) field.get(o));
                } else if (Integer.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, INT_ARRAY)) {
                            field.set(o, reader.readIntArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeIntArray(name, (int[]) field.get(o));
                } else if (Long.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, LONG_ARRAY)) {
                            field.set(o, reader.readLongArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeLongArray(name, (long[]) field.get(o));
                } else if (Float.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, FLOAT_ARRAY)) {
                            field.set(o, reader.readFloatArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeFloatArray(name, (float[]) field.get(o));
                } else if (Double.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, DOUBLE_ARRAY)) {
                            field.set(o, reader.readDoubleArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeDoubleArray(name, (double[]) field.get(o));
                } else if (Boolean.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, BOOLEAN_ARRAY)) {
                            field.set(o, reader.readBooleanArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeBooleanArray(name, (boolean[]) field.get(o));
                } else if (Character.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, CHAR_ARRAY)) {
                            field.set(o, reader.readCharArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeCharArray(name, (char[]) field.get(o));
                } else if (String.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, UTF_ARRAY)) {
                            field.set(o, reader.readStringArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeStringArray(name, (String[]) field.get(o));
                } else if (BigDecimal.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, DECIMAL_ARRAY)) {
                            field.set(o, reader.readDecimalArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeDecimalArray(name, (BigDecimal[]) field.get(o));
                } else if (LocalTime.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, TIME_ARRAY)) {
                            field.set(o, reader.readTimeArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeTimeArray(name, (LocalTime[]) field.get(o));
                } else if (LocalDate.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, DATE_ARRAY)) {
                            field.set(o, reader.readDateArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeDateArray(name, (LocalDate[]) field.get(o));
                } else if (LocalDateTime.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, TIMESTAMP_ARRAY)) {
                            field.set(o, reader.readTimestampArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeTimestampArray(name, (LocalDateTime[]) field.get(o));
                } else if (OffsetDateTime.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, TIMESTAMP_WITH_TIMEZONE_ARRAY)) {
                            field.set(o, reader.readTimestampWithTimezoneArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeTimestampWithTimezoneArray(name, (OffsetDateTime[]) field.get(o));
                } else if (componentType.isEnum()) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, UTF_ARRAY)) {
                            String[] stringArray = reader.readStringArray(name);
                            Enum[] enumArray = null;
                            if (stringArray != null) {
                                enumArray = new Enum[stringArray.length];
                                for (int i = 0; i < stringArray.length; i++) {
                                    enumArray[i] = stringArray[i] == null
                                        ? null
                                        : Enum.valueOf((Class<? extends Enum>) componentType, stringArray[i]);
                                }
                            }
                            field.set(o, enumArray);
                        }
                    };
                    writers[index] = (w, o) -> {
                        Enum[] values = (Enum[]) field.get(o);
                        String[] stringArray = null;
                        if (values != null) {
                            stringArray = new String[values.length];
                            for (int i = 0; i < values.length; i++) {
                                stringArray[i] = values[i] == null ? null : values[i].name();
                            }
                        }
                       w.writeStringArray(name, stringArray);
                    };
                } else {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, COMPOSED_ARRAY)) {
                            field.set(o, reader.readObjectArray(name, componentType));
                        }
                    };
                    writers[index] = (w, o) -> w.writeObjectArray(name, (Object[]) field.get(o));
                }
            } else {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, COMPOSED)) {
                        field.set(o, reader.readObject(name));
                    }
                };
                writers[index] = (w, o) -> w.writeObject(name, field.get(o));
            }
            index++;
        }

        writersCache.put(clazz, writers);
        readersCache.put(clazz, readers);
    }

    interface Reader {
        void read(CompactReader reader, Schema schema, Object o) throws Exception;
    }

    interface Writer {
        void write(CompactWriter writer, Object o) throws Exception;
    }
}
