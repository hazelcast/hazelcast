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
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.nio.serialization.compact.TypeID;

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

import static com.hazelcast.nio.serialization.compact.TypeID.BOOLEAN;
import static com.hazelcast.nio.serialization.compact.TypeID.BOOLEAN_ARRAY;
import static com.hazelcast.nio.serialization.compact.TypeID.BYTE;
import static com.hazelcast.nio.serialization.compact.TypeID.BYTE_ARRAY;
import static com.hazelcast.nio.serialization.compact.TypeID.CHAR;
import static com.hazelcast.nio.serialization.compact.TypeID.CHAR_ARRAY;
import static com.hazelcast.nio.serialization.compact.TypeID.COMPOSED;
import static com.hazelcast.nio.serialization.compact.TypeID.COMPOSED_ARRAY;
import static com.hazelcast.nio.serialization.compact.TypeID.DATE;
import static com.hazelcast.nio.serialization.compact.TypeID.DATE_ARRAY;
import static com.hazelcast.nio.serialization.compact.TypeID.DECIMAL;
import static com.hazelcast.nio.serialization.compact.TypeID.DECIMAL_ARRAY;
import static com.hazelcast.nio.serialization.compact.TypeID.DOUBLE;
import static com.hazelcast.nio.serialization.compact.TypeID.DOUBLE_ARRAY;
import static com.hazelcast.nio.serialization.compact.TypeID.FLOAT;
import static com.hazelcast.nio.serialization.compact.TypeID.FLOAT_ARRAY;
import static com.hazelcast.nio.serialization.compact.TypeID.INT;
import static com.hazelcast.nio.serialization.compact.TypeID.INT_ARRAY;
import static com.hazelcast.nio.serialization.compact.TypeID.LONG;
import static com.hazelcast.nio.serialization.compact.TypeID.LONG_ARRAY;
import static com.hazelcast.nio.serialization.compact.TypeID.SHORT;
import static com.hazelcast.nio.serialization.compact.TypeID.SHORT_ARRAY;
import static com.hazelcast.nio.serialization.compact.TypeID.TIME;
import static com.hazelcast.nio.serialization.compact.TypeID.TIMESTAMP;
import static com.hazelcast.nio.serialization.compact.TypeID.TIMESTAMP_ARRAY;
import static com.hazelcast.nio.serialization.compact.TypeID.TIMESTAMP_WITH_TIMEZONE;
import static com.hazelcast.nio.serialization.compact.TypeID.TIMESTAMP_WITH_TIMEZONE_ARRAY;
import static com.hazelcast.nio.serialization.compact.TypeID.TIME_ARRAY;
import static com.hazelcast.nio.serialization.compact.TypeID.STRING;
import static com.hazelcast.nio.serialization.compact.TypeID.STRING_ARRAY;
import static java.util.stream.Collectors.toList;

public class ReflectiveCompactSerializer implements InternalCompactSerializer<Object, DefaultCompactReader> {

    private final Map<Class, Writer[]> writersCache = new ConcurrentHashMap<>();
    private final Map<Class, Reader[]> readersCache = new ConcurrentHashMap<>();

    public ReflectiveCompactSerializer() {
    }

    @Override
    public void write(CompactWriter writer, Object object) throws IOException {
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

    private boolean readFast(Class clazz, CompactReader compactReader, Object object) throws IOException {
        Reader[] readers = readersCache.get(clazz);
        Schema schema = ((DefaultCompactReader) compactReader).getSchema();
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

    public Object read(DefaultCompactReader reader) throws IOException {
        Class associatedClass = reader.getAssociatedClass();
        Object object;
        try {
            object = ClassLoaderUtil.newInstance(associatedClass.getClassLoader(), associatedClass);
        } catch (Exception e) {
            throw new IOException("Class " + associatedClass + " must have an empty constructor", e);
        }
        try {
            if (readFast(associatedClass, reader, object)) {
                return object;
            }
            createFastReadWriteCaches(associatedClass);
            readFast(associatedClass, reader, object);
            return object;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static List<Field> getAllFields(List<Field> fields, Class<?> type) {
        fields.addAll(Arrays.stream(type.getDeclaredFields())
                .filter(f -> !Modifier.isStatic(f.getModifiers()))
                .filter(f -> !Modifier.isTransient(f.getModifiers()))
                .collect(toList()));
        if (type.getSuperclass() != null) {
            getAllFields(fields, type.getSuperclass());
        }
        return fields;
    }

    private boolean fieldExists(Schema schema, String name, TypeID fieldType) {
        FieldDescriptor fieldDescriptor = schema.getField(name);
        return fieldDescriptor != null && fieldDescriptor.getType().equals(fieldType);
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
                        field.set(o, reader.readLong(name));
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
                    if (fieldExists(schema, name, STRING)) {
                        field.set(o, reader.readString(name));
                    }
                };
                writers[index] = (w, o) -> w.writeString(name, (String) field.get(o));
            } else if (Byte.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, BYTE)) {
                        field.set(o, reader.readByte(name));
                    }
                };
                writers[index] = (w, o) -> w.writeByte(name, (byte) field.get(o));
            } else if (Short.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, SHORT)) {
                        field.set(o, reader.readShort(name));
                    }
                };
                writers[index] = (w, o) -> w.writeShort(name, (short) field.get(o));
            } else if (Integer.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, INT)) {
                        field.set(o, reader.readInt(name));
                    }
                };
                writers[index] = (w, o) -> w.writeInt(name, (int) field.get(o));
            } else if (Long.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, LONG)) {
                        field.set(o, reader.readLong(name));
                    }
                };
                writers[index] = (w, o) -> w.writeLong(name, (long) field.get(o));
            } else if (Double.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, FLOAT)) {
                        field.set(o, reader.readDouble(name));
                    }
                };
                writers[index] = (w, o) -> w.writeDouble(name, (double) field.get(o));
            } else if (Boolean.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, BOOLEAN)) {
                        field.set(o, reader.readBoolean(name));
                    }
                };
                writers[index] = (w, o) -> w.writeBoolean(name, (boolean) field.get(o));
            } else if (Character.class.equals(type)) {
                readers[index] = (reader, schema, o) -> {
                    if (fieldExists(schema, name, CHAR)) {
                        field.set(o, reader.readChar(name));
                    }
                };
                writers[index] = (w, o) -> w.writeChar(name, (char) field.get(o));
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
                        if (fieldExists(schema, name, STRING_ARRAY)) {
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
