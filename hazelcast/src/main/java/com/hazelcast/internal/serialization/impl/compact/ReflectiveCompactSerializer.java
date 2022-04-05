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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;
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

import static com.hazelcast.internal.nio.InstanceCreationUtil.createNewInstance;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT8;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_COMPACT;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DATE;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DECIMAL;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT32;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT64;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT8;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT32;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT64;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INT16;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INT16;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_STRING;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIME;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMP;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE;
import static com.hazelcast.nio.serialization.FieldKind.BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.INT8;
import static com.hazelcast.nio.serialization.FieldKind.COMPACT;
import static com.hazelcast.nio.serialization.FieldKind.DATE;
import static com.hazelcast.nio.serialization.FieldKind.DECIMAL;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.INT32;
import static com.hazelcast.nio.serialization.FieldKind.INT64;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT8;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_FLOAT64;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_FLOAT32;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT32;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT64;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT16;
import static com.hazelcast.nio.serialization.FieldKind.INT16;
import static com.hazelcast.nio.serialization.FieldKind.STRING;
import static com.hazelcast.nio.serialization.FieldKind.TIME;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP;
import static com.hazelcast.nio.serialization.FieldKind.TIMESTAMP_WITH_TIMEZONE;
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
public class ReflectiveCompactSerializer<T> implements CompactSerializer<T> {

    private final Map<Class, Writer[]> writersCache = new ConcurrentHashMap<>();
    private final Map<Class, Reader[]> readersCache = new ConcurrentHashMap<>();

    @Override
    public void write(@Nonnull CompactWriter writer, @Nonnull T object) {
        Class<?> clazz = object.getClass();
        if (writeFast(clazz, writer, object)) {
            return;
        }
        createFastReadWriteCaches(clazz);
        writeFast(clazz, writer, object);
    }

    private boolean writeFast(Class clazz, CompactWriter compactWriter, Object object) {
        Writer[] writers = writersCache.get(clazz);
        if (writers == null) {
            return false;
        }
        for (Writer writer : writers) {
            try {
                writer.write(compactWriter, object);
            } catch (Exception e) {
                throw new HazelcastSerializationException(e);
            }
        }
        return true;
    }

    private boolean readFast(Class clazz, DefaultCompactReader compactReader, Object object) {
        Reader[] readers = readersCache.get(clazz);
        Schema schema = compactReader.getSchema();
        if (readers == null) {
            return false;
        }
        for (Reader reader : readers) {
            try {
                reader.read(compactReader, schema, object);
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

    /**
     * @return true if one of the given fieldKinds exist on the schema with the given `name`
     */
    private boolean fieldExists(Schema schema, String name, FieldKind... fieldKinds) {
        FieldDescriptor fieldDescriptor = schema.getField(name);
        if (fieldDescriptor == null) {
            return false;
        }
        for (FieldKind fieldKind : fieldKinds) {
            if (fieldDescriptor.getKind() == fieldKind) {
                return true;
            }
        }
        return false;
    }

    private void createFastReadWriteCaches(Class clazz) {
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
                    if (fieldExists(schema, name, INT8, NULLABLE_INT8)) {
                        field.setByte(o, reader.readInt8(name));
                    }
                };
                writers[index] = (w, o) -> w.writeInt8(name, field.getByte(o));
            } else if (Character.TYPE.equals(type)) {
                throwUnsupportedFieldTypeException("char");
            } else if (Short.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, INT16, NULLABLE_INT16)) {
                        field.setShort(o, reader.readInt16(name));
                    }
                };
                writers[index] = (w, o) -> w.writeInt16(name, field.getShort(o));
            } else if (Integer.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, INT32, NULLABLE_INT32)) {
                        field.setInt(o, reader.readInt32(name));
                    }
                };
                writers[index] = (w, o) -> w.writeInt32(name, field.getInt(o));
            } else if (Long.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, INT64, NULLABLE_INT64)) {
                        field.setLong(o, reader.readInt64(name));
                    }
                };
                writers[index] = (w, o) -> w.writeInt64(name, field.getLong(o));
            } else if (Float.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, FLOAT32, NULLABLE_FLOAT32)) {
                        field.setFloat(o, reader.readFloat32(name));
                    }
                };
                writers[index] = (w, o) -> w.writeFloat32(name, field.getFloat(o));
            } else if (Double.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, FLOAT64, NULLABLE_FLOAT64)) {
                        field.setDouble(o, reader.readFloat64(name));
                    }
                };
                writers[index] = (w, o) -> w.writeFloat64(name, field.getDouble(o));
            } else if (Boolean.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, BOOLEAN, NULLABLE_BOOLEAN)) {
                        field.setBoolean(o, reader.readBoolean(name));
                    }
                };
                writers[index] = (w, o) -> w.writeBoolean(name, field.getBoolean(o));
            } else if (String.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, STRING)) {
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
            } else if (Byte.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, INT8, NULLABLE_INT8)) {
                        field.set(o, reader.readNullableInt8(name));
                    }
                };
                writers[index] = (w, o) -> w.writeNullableInt8(name, (Byte) field.get(o));
            } else if (Character.class.equals(type)) {
                throwUnsupportedFieldTypeException("Character");
            } else if (Boolean.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, BOOLEAN, NULLABLE_BOOLEAN)) {
                        field.set(o, reader.readNullableBoolean(name));
                    }
                };
                writers[index] = (w, o) -> w.writeNullableBoolean(name, (Boolean) field.get(o));
            } else if (Short.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, INT16, NULLABLE_INT16)) {
                        field.set(o, reader.readNullableInt16(name));
                    }
                };
                writers[index] = (w, o) -> w.writeNullableInt16(name, (Short) field.get(o));
            } else if (Integer.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, INT32, NULLABLE_INT32)) {
                        field.set(o, reader.readNullableInt32(name));
                    }
                };
                writers[index] = (w, o) -> w.writeNullableInt32(name, (Integer) field.get(o));
            } else if (Long.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, INT64, NULLABLE_INT64)) {
                        field.set(o, reader.readNullableInt64(name));
                    }
                };
                writers[index] = (w, o) -> w.writeNullableInt64(name, (Long) field.get(o));
            } else if (Float.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, FLOAT32, NULLABLE_FLOAT32)) {
                        field.set(o, reader.readNullableFloat32(name));
                    }
                };
                writers[index] = (w, o) -> w.writeNullableFloat32(name, (Float) field.get(o));
            } else if (Double.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, FLOAT64, NULLABLE_FLOAT64)) {
                        field.set(o, reader.readNullableFloat64(name));
                    }
                };
                writers[index] = (w, o) -> w.writeNullableFloat64(name, (Double) field.get(o));
            } else if (type.isEnum()) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, STRING)) {
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
                if (Boolean.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_BOOLEAN, ARRAY_OF_NULLABLE_BOOLEAN)) {
                            field.set(o, reader.readArrayOfBoolean(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfBoolean(name, (boolean[]) field.get(o));
                } else if (Byte.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_INT8, ARRAY_OF_NULLABLE_INT8)) {
                            field.set(o, reader.readArrayOfInt8(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfInt8(name, (byte[]) field.get(o));
                } else if (Character.TYPE.equals(componentType)) {
                    throwUnsupportedFieldTypeException("char[]");
                } else if (Short.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_INT16, ARRAY_OF_NULLABLE_INT16)) {
                            field.set(o, reader.readArrayOfInt16(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfInt16(name, (short[]) field.get(o));
                } else if (Integer.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_INT32, ARRAY_OF_NULLABLE_INT32)) {
                            field.set(o, reader.readArrayOfInt32(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfInt32(name, (int[]) field.get(o));
                } else if (Long.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_INT64, ARRAY_OF_NULLABLE_INT64)) {
                            field.set(o, reader.readArrayOfInt64(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfInt64(name, (long[]) field.get(o));
                } else if (Float.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_FLOAT32, ARRAY_OF_NULLABLE_FLOAT32)) {
                            field.set(o, reader.readArrayOfFloat32(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfFloat32(name, (float[]) field.get(o));
                } else if (Double.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_FLOAT64, ARRAY_OF_NULLABLE_FLOAT64)) {
                            field.set(o, reader.readArrayOfFloat64(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfFloat64(name, (double[]) field.get(o));
                } else if (Boolean.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_BOOLEAN, ARRAY_OF_NULLABLE_BOOLEAN)) {
                            field.set(o, reader.readArrayOfNullableBoolean(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfNullableBoolean(name, (Boolean[]) field.get(o));
                } else if (Byte.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_INT8, ARRAY_OF_NULLABLE_INT8)) {
                            field.set(o, reader.readArrayOfNullableInt8(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfNullableInt8(name, (Byte[]) field.get(o));
                } else if (Character.class.equals(componentType)) {
                    throwUnsupportedFieldTypeException("Character[]");
                } else if (Short.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_INT16, ARRAY_OF_NULLABLE_INT16)) {
                            field.set(o, reader.readArrayOfNullableInt16(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfNullableInt16(name, (Short[]) field.get(o));
                } else if (Integer.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_INT32, ARRAY_OF_NULLABLE_INT32)) {
                            field.set(o, reader.readArrayOfNullableInt32(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfNullableInt32(name, (Integer[]) field.get(o));
                } else if (Long.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_INT64, ARRAY_OF_NULLABLE_INT64)) {
                            field.set(o, reader.readArrayOfNullableInt64(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfNullableInt64(name, (Long[]) field.get(o));
                } else if (Float.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_FLOAT32, ARRAY_OF_NULLABLE_FLOAT32)) {
                            field.set(o, reader.readArrayOfNullableFloat32(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfNullableFloat32(name, (Float[]) field.get(o));
                } else if (Double.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_FLOAT64, ARRAY_OF_NULLABLE_FLOAT64)) {
                            field.set(o, reader.readArrayOfNullableFloat64(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfNullableFloat64(name, (Double[]) field.get(o));
                } else if (String.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_STRING)) {
                            field.set(o, reader.readArrayOfString(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfString(name, (String[]) field.get(o));
                } else if (BigDecimal.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_DECIMAL)) {
                            field.set(o, reader.readArrayOfDecimal(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfDecimal(name, (BigDecimal[]) field.get(o));
                } else if (LocalTime.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_TIME)) {
                            field.set(o, reader.readArrayOfTime(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfTime(name, (LocalTime[]) field.get(o));
                } else if (LocalDate.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_DATE)) {
                            field.set(o, reader.readArrayOfDate(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfDate(name, (LocalDate[]) field.get(o));
                } else if (LocalDateTime.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_TIMESTAMP)) {
                            field.set(o, reader.readArrayOfTimestamp(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfTimestamp(name, (LocalDateTime[]) field.get(o));
                } else if (OffsetDateTime.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_TIMESTAMP_WITH_TIMEZONE)) {
                            field.set(o, reader.readArrayOfTimestampWithTimezone(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfTimestampWithTimezone(name, (OffsetDateTime[]) field.get(o));
                } else if (componentType.isEnum()) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_STRING)) {
                            String[] stringArray = reader.readArrayOfString(name);
                            Enum[] enumArray = enumsFromString((Class<? extends Enum>) componentType, stringArray);
                            field.set(o, enumArray);
                        }
                    };
                    writers[index] = (w, o) -> {
                        Enum[] values = (Enum[]) field.get(o);
                        String[] stringArray = enumsAsStrings(values);
                        w.writeArrayOfString(name, stringArray);
                    };
                } else {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_COMPACT)) {
                            field.set(o, reader.readArrayOfCompact(name, componentType));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfCompact(name, (Object[]) field.get(o));
                }
            } else {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, COMPACT)) {
                        field.set(o, reader.readCompact(name));
                    }
                };
                writers[index] = (w, o) -> w.writeCompact(name, field.get(o));
            }
            index++;
        }

        writersCache.put(clazz, writers);
        readersCache.put(clazz, readers);
    }

    private String[] enumsAsStrings(Enum[] values) {
        String[] stringArray = null;
        if (values != null) {
            stringArray = new String[values.length];
            for (int i = 0; i < values.length; i++) {
                stringArray[i] = values[i] == null ? null : values[i].name();
            }
        }
        return stringArray;
    }

    private Enum[] enumsFromString(Class<? extends Enum> componentType, String[] stringArray) {
        Enum[] enumArray = null;
        if (stringArray != null) {
            enumArray = new Enum[stringArray.length];
            for (int i = 0; i < stringArray.length; i++) {
                enumArray[i] = stringArray[i] == null
                        ? null
                        : Enum.valueOf(componentType, stringArray[i]);
            }
        }
        return enumArray;
    }

    private void throwUnsupportedFieldTypeException(String typeName) {
        throw new HazelcastSerializationException("Compact serialization format does not support "
                + "fields of type '" + typeName + "'. If you want to use such fields with the compact"
                + " serialization format, consider adding an explicit serializer for it.");
    }

    interface Reader {
        void read(CompactReader reader, Schema schema, Object o) throws Exception;
    }

    interface Writer {
        void write(CompactWriter writer, Object o) throws Exception;
    }
}
