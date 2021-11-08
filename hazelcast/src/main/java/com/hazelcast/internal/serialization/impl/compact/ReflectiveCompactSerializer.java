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
import com.hazelcast.nio.serialization.FieldKind;
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

import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_BOOLEANS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_BYTES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_COMPACTS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DATES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DECIMALS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_DOUBLES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_FLOATS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_INTS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_LONGS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_BOOLEANS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_BYTES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_DOUBLES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_FLOATS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_INTS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_LONGS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_NULLABLE_SHORTS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_SHORTS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_STRINGS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMES;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMPS;
import static com.hazelcast.nio.serialization.FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONES;
import static com.hazelcast.nio.serialization.FieldKind.BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.BYTE;
import static com.hazelcast.nio.serialization.FieldKind.COMPACT;
import static com.hazelcast.nio.serialization.FieldKind.DATE;
import static com.hazelcast.nio.serialization.FieldKind.DECIMAL;
import static com.hazelcast.nio.serialization.FieldKind.DOUBLE;
import static com.hazelcast.nio.serialization.FieldKind.FLOAT;
import static com.hazelcast.nio.serialization.FieldKind.INT;
import static com.hazelcast.nio.serialization.FieldKind.LONG;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_BOOLEAN;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_BYTE;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_DOUBLE;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_FLOAT;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_INT;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_LONG;
import static com.hazelcast.nio.serialization.FieldKind.NULLABLE_SHORT;
import static com.hazelcast.nio.serialization.FieldKind.SHORT;
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
    public void write(@Nonnull CompactWriter writer, @Nonnull T object) throws IOException {
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
    public T read(@Nonnull CompactReader reader) throws IOException {
        // We always fed DefaultCompactReader to this serializer.
        DefaultCompactReader compactReader = (DefaultCompactReader) reader;
        Class associatedClass = requireNonNull(compactReader.getAssociatedClass(),
                "AssociatedClass is required for ReflectiveCompactSerializer");

        T object;
        object = (T) createObject(associatedClass);
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
                    if (fieldExists(schema, name, BYTE, NULLABLE_BYTE)) {
                        field.setByte(o, reader.readByte(name));
                    }
                };
                writers[index] = (w, o) -> w.writeByte(name, field.getByte(o));
            } else if (Short.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, SHORT, NULLABLE_SHORT)) {
                        field.setShort(o, reader.readShort(name));
                    }
                };
                writers[index] = (w, o) -> w.writeShort(name, field.getShort(o));
            } else if (Integer.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, INT, NULLABLE_INT)) {
                        field.setInt(o, reader.readInt(name));
                    }
                };
                writers[index] = (w, o) -> w.writeInt(name, field.getInt(o));
            } else if (Long.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, LONG, NULLABLE_LONG)) {
                        field.setLong(o, reader.readLong(name));
                    }
                };
                writers[index] = (w, o) -> w.writeLong(name, field.getLong(o));
            } else if (Float.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, FLOAT, NULLABLE_FLOAT)) {
                        field.setFloat(o, reader.readFloat(name));
                    }
                };
                writers[index] = (w, o) -> w.writeFloat(name, field.getFloat(o));
            } else if (Double.TYPE.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, DOUBLE, NULLABLE_DOUBLE)) {
                        field.setDouble(o, reader.readDouble(name));
                    }
                };
                writers[index] = (w, o) -> w.writeDouble(name, field.getDouble(o));
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
                    if (fieldExists(schema, name, BYTE, NULLABLE_BYTE)) {
                        field.set(o, reader.readNullableByte(name));
                    }
                };
                writers[index] = (w, o) -> w.writeNullableByte(name, (Byte) field.get(o));
            } else if (Boolean.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, BOOLEAN, NULLABLE_BOOLEAN)) {
                        field.set(o, reader.readNullableBoolean(name));
                    }
                };
                writers[index] = (w, o) -> w.writeNullableBoolean(name, (Boolean) field.get(o));
            } else if (Short.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, SHORT, NULLABLE_SHORT)) {
                        field.set(o, reader.readNullableShort(name));
                    }
                };
                writers[index] = (w, o) -> w.writeNullableShort(name, (Short) field.get(o));
            } else if (Integer.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, INT, NULLABLE_INT)) {
                        field.set(o, reader.readNullableInt(name));
                    }
                };
                writers[index] = (w, o) -> w.writeNullableInt(name, (Integer) field.get(o));
            } else if (Long.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, LONG, NULLABLE_LONG)) {
                        field.set(o, reader.readNullableLong(name));
                    }
                };
                writers[index] = (w, o) -> w.writeNullableLong(name, (Long) field.get(o));
            } else if (Float.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, FLOAT, NULLABLE_FLOAT)) {
                        field.set(o, reader.readNullableFloat(name));
                    }
                };
                writers[index] = (w, o) -> w.writeNullableFloat(name, (Float) field.get(o));
            } else if (Double.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, DOUBLE, NULLABLE_DOUBLE)) {
                        field.set(o, reader.readNullableDouble(name));
                    }
                };
                writers[index] = (w, o) -> w.writeNullableDouble(name, (Double) field.get(o));
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
                        if (fieldExists(schema, name, ARRAY_OF_BOOLEANS, ARRAY_OF_NULLABLE_BOOLEANS)) {
                            field.set(o, reader.readArrayOfBooleans(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfBooleans(name, (boolean[]) field.get(o));
                } else if (Byte.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_BYTES, ARRAY_OF_NULLABLE_BYTES)) {
                            field.set(o, reader.readArrayOfBytes(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfBytes(name, (byte[]) field.get(o));
                } else if (Short.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_SHORTS, ARRAY_OF_NULLABLE_SHORTS)) {
                            field.set(o, reader.readArrayOfShorts(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfShorts(name, (short[]) field.get(o));
                } else if (Integer.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_INTS, ARRAY_OF_NULLABLE_INTS)) {
                            field.set(o, reader.readArrayOfInts(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfInts(name, (int[]) field.get(o));
                } else if (Long.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_LONGS, ARRAY_OF_NULLABLE_LONGS)) {
                            field.set(o, reader.readArrayOfLongs(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfLongs(name, (long[]) field.get(o));
                } else if (Float.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_FLOATS, ARRAY_OF_NULLABLE_FLOATS)) {
                            field.set(o, reader.readArrayOfFloats(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfFloats(name, (float[]) field.get(o));
                } else if (Double.TYPE.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_DOUBLES, ARRAY_OF_NULLABLE_DOUBLES)) {
                            field.set(o, reader.readArrayOfDoubles(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfDoubles(name, (double[]) field.get(o));
                } else if (Boolean.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_BOOLEANS, ARRAY_OF_NULLABLE_BOOLEANS)) {
                            field.set(o, reader.readArrayOfNullableBooleans(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfNullableBooleans(name, (Boolean[]) field.get(o));
                } else if (Byte.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_BYTES, ARRAY_OF_NULLABLE_BYTES)) {
                            field.set(o, reader.readArrayOfNullableBytes(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfNullableBytes(name, (Byte[]) field.get(o));
                } else if (Short.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_SHORTS, ARRAY_OF_NULLABLE_SHORTS)) {
                            field.set(o, reader.readArrayOfNullableShorts(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfNullableShorts(name, (Short[]) field.get(o));
                } else if (Integer.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_INTS, ARRAY_OF_NULLABLE_INTS)) {
                            field.set(o, reader.readArrayOfNullableInts(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfNullableInts(name, (Integer[]) field.get(o));
                } else if (Long.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_LONGS, ARRAY_OF_NULLABLE_LONGS)) {
                            field.set(o, reader.readArrayOfNullableLongs(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfNullableLongs(name, (Long[]) field.get(o));
                } else if (Float.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_FLOATS, ARRAY_OF_NULLABLE_FLOATS)) {
                            field.set(o, reader.readArrayOfNullableFloats(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfNullableFloats(name, (Float[]) field.get(o));
                } else if (Double.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_DOUBLES, ARRAY_OF_NULLABLE_DOUBLES)) {
                            field.set(o, reader.readArrayOfNullableDoubles(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfNullableDoubles(name, (Double[]) field.get(o));
                } else if (String.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_STRINGS)) {
                            field.set(o, reader.readArrayOfStrings(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfStrings(name, (String[]) field.get(o));
                } else if (BigDecimal.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_DECIMALS)) {
                            field.set(o, reader.readArrayOfDecimals(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfDecimals(name, (BigDecimal[]) field.get(o));
                } else if (LocalTime.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_TIMES)) {
                            field.set(o, reader.readArrayOfTimes(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfTimes(name, (LocalTime[]) field.get(o));
                } else if (LocalDate.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_DATES)) {
                            field.set(o, reader.readArrayOfDates(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfDates(name, (LocalDate[]) field.get(o));
                } else if (LocalDateTime.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_TIMESTAMPS)) {
                            field.set(o, reader.readArrayOfTimestamps(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfTimestamps(name, (LocalDateTime[]) field.get(o));
                } else if (OffsetDateTime.class.equals(componentType)) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_TIMESTAMP_WITH_TIMEZONES)) {
                            field.set(o, reader.readArrayOfTimestampWithTimezones(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfTimestampWithTimezones(name, (OffsetDateTime[]) field.get(o));
                } else if (componentType.isEnum()) {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_STRINGS)) {
                            String[] stringArray = reader.readArrayOfStrings(name);
                            Enum[] enumArray = enumsFromString((Class<? extends Enum>) componentType, stringArray);
                            field.set(o, enumArray);
                        }
                    };
                    writers[index] = (w, o) -> {
                        Enum[] values = (Enum[]) field.get(o);
                        String[] stringArray = enumsAsStrings(values);
                        w.writeArrayOfStrings(name, stringArray);
                    };
                } else {
                    readers[index] = (reader, schema, o) -> {
                        if (fieldExists(schema, name, ARRAY_OF_COMPACTS)) {
                            field.set(o, reader.readArrayOfCompacts(name, componentType));
                        }
                    };
                    writers[index] = (w, o) -> w.writeArrayOfCompacts(name, (Object[]) field.get(o));
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

    interface Reader {
        void read(CompactReader reader, Schema schema, Object o) throws Exception;
    }

    interface Writer {
        void write(CompactWriter writer, Object o) throws Exception;
    }
}
