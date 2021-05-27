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

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactWriter;

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

import static com.hazelcast.nio.serialization.FieldType.BOOLEAN;
import static com.hazelcast.nio.serialization.FieldType.BOOLEAN_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.BYTE;
import static com.hazelcast.nio.serialization.FieldType.BYTE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.CHAR;
import static com.hazelcast.nio.serialization.FieldType.CHAR_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.COMPOSED;
import static com.hazelcast.nio.serialization.FieldType.COMPOSED_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.DATE;
import static com.hazelcast.nio.serialization.FieldType.DATE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.DECIMAL;
import static com.hazelcast.nio.serialization.FieldType.DECIMAL_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.DOUBLE;
import static com.hazelcast.nio.serialization.FieldType.DOUBLE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.FLOAT;
import static com.hazelcast.nio.serialization.FieldType.FLOAT_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.INT;
import static com.hazelcast.nio.serialization.FieldType.INT_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.LONG;
import static com.hazelcast.nio.serialization.FieldType.LONG_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.SHORT;
import static com.hazelcast.nio.serialization.FieldType.SHORT_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIME;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP_WITH_TIMEZONE;
import static com.hazelcast.nio.serialization.FieldType.TIMESTAMP_WITH_TIMEZONE_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.TIME_ARRAY;
import static com.hazelcast.nio.serialization.FieldType.UTF;
import static com.hazelcast.nio.serialization.FieldType.UTF_ARRAY;
import static java.util.stream.Collectors.toList;

public class ReflectiveCompactSerializer implements InternalCompactSerializer<Object, DefaultCompactReader> {

    private final Map<Class, BiConsumerEx<CompactWriter, Object>[]> writersCache = new ConcurrentHashMap<>();
    private final Map<Class, BiConsumerEx<CompactReader, Object>[]> readersCache = new ConcurrentHashMap<>();

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

    private boolean writeFast(Class clazz, CompactWriter writer, Object object) {
        BiConsumerEx<CompactWriter, Object>[] consumers = writersCache.get(clazz);
        if (consumers != null) {
            for (BiConsumerEx<CompactWriter, Object> consumer : consumers) {
                consumer.accept(writer, object);
            }
            return true;
        }
        return false;
    }

    private boolean readFast(Class clazz, CompactReader reader, Object object) {
        BiConsumerEx<CompactReader, Object>[] consumers = readersCache.get(clazz);
        if (consumers != null) {
            for (BiConsumerEx<CompactReader, Object> consumer : consumers) {
                consumer.accept(reader, object);
            }
            return true;
        }
        return false;
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

    public interface RunnableEx {
        void run() throws Exception;
    }

    private void readIfExists(CompactReader reader, String name, FieldType fieldType, RunnableEx actualReader) throws Exception {
        Schema schema = ((DefaultCompactReader) reader).getSchema();
        FieldDescriptor fieldDescriptor = schema.getField(name);
        if (fieldDescriptor != null && fieldDescriptor.getType().equals(fieldType)) {
            actualReader.run();
        }
    }

    private boolean fieldExists(CompactReader reader, String name, FieldType fieldType) {
        Schema schema = ((DefaultCompactReader) reader).getSchema();
        FieldDescriptor fieldDescriptor = schema.getField(name);
        return fieldDescriptor != null && fieldDescriptor.getType().equals(fieldType);
    }

    private void createFastReadWriteCaches(Class clazz) {
        //get inherited fields as well
        List<Field> allFields = getAllFields(new LinkedList<>(), clazz);
        BiConsumerEx<CompactWriter, Object>[] writers = new BiConsumerEx[allFields.size()];
        BiConsumerEx<CompactReader, Object>[] readers = new BiConsumerEx[allFields.size()];

        int index = 0;
        for (Field field : allFields) {
            field.setAccessible(true);
            Class<?> type = field.getType();
            String name = field.getName();
            if (Byte.TYPE.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, BYTE)) {
                        field.setByte(o, reader.readByte(name));
                    }
                };
                writers[index] = (w, o) -> w.writeByte(name, field.getByte(o));
            } else if (Short.TYPE.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, SHORT)) {
                        field.setShort(o, reader.readShort(name));
                    }
                };
                writers[index] = (w, o) -> w.writeShort(name, field.getShort(o));
            } else if (Integer.TYPE.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, INT)) {
                        field.setInt(o, reader.readInt(name));
                    }
                };
                writers[index] = (w, o) -> w.writeInt(name, field.getInt(o));
            } else if (Long.TYPE.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, LONG)) {
                        field.set(o, reader.readLong(name));
                    }
                };
                writers[index] = (w, o) -> w.writeLong(name, field.getLong(o));
            } else if (Float.TYPE.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, FLOAT)) {
                        field.setFloat(o, reader.readFloat(name));
                    }
                };
                writers[index] = (w, o) -> w.writeFloat(name, field.getFloat(o));
            } else if (Double.TYPE.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, DOUBLE)) {
                        field.setDouble(o, reader.readDouble(name));
                    }
                };
                writers[index] = (w, o) -> w.writeDouble(name, field.getDouble(o));
            } else if (Boolean.TYPE.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, BOOLEAN)) {
                        field.setBoolean(o, reader.readBoolean(name));
                    }
                };
                writers[index] = (w, o) -> w.writeBoolean(name, field.getBoolean(o));
            } else if (Character.TYPE.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, CHAR)) {
                        field.setChar(o, reader.readChar(name));
                    }
                };
                writers[index] = (w, o) -> w.writeChar(name, field.getChar(o));
            } else if (String.class.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, UTF)) {
                        field.set(o, reader.readString(name));
                    }
                };
                writers[index] = (w, o) -> w.writeString(name, (String) field.get(o));
            } else if (Byte.class.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, BYTE)) {
                        field.set(o, reader.readByte(name));
                    }
                };
                writers[index] = (w, o) -> w.writeByte(name, (byte) field.get(o));
            } else if (Short.class.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, SHORT)) {
                        field.set(o, reader.readShort(name));
                    }
                };
                writers[index] = (w, o) -> w.writeShort(name, (short) field.get(o));
            } else if (Integer.class.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, INT)) {
                        field.set(o, reader.readInt(name));
                    }
                };
                writers[index] = (w, o) -> w.writeInt(name, (int) field.get(o));
            } else if (Long.class.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, LONG)) {
                        field.set(o, reader.readLong(name));
                    }
                };
                writers[index] = (w, o) -> w.writeLong(name, (long) field.get(o));
            } else if (Double.class.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, FLOAT)) {
                        field.set(o, reader.readDouble(name));
                    }
                };
                writers[index] = (w, o) -> w.writeDouble(name, (double) field.get(o));
            } else if (Boolean.class.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, BOOLEAN)) {
                        field.set(o, reader.readBoolean(name));
                    }
                };
                writers[index] = (w, o) -> w.writeBoolean(name, (boolean) field.get(o));
            } else if (Character.class.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, CHAR)) {
                        field.set(o, reader.readChar(name));
                    }
                };
                writers[index] = (w, o) -> w.writeChar(name, (char) field.get(o));
            } else if (BigDecimal.class.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, DECIMAL)) {
                        field.set(o, reader.readDecimal(name));
                    }
                };
                writers[index] = (w, o) -> w.writeDecimal(name, (BigDecimal) field.get(o));
            } else if (LocalTime.class.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, TIME)) {
                        field.set(o, reader.readTime(name));
                    }
                };
                writers[index] = (w, o) -> w.writeTime(name, (LocalTime) field.get(o));
            } else if (LocalDate.class.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, DATE)) {
                        field.set(o, reader.readDate(name));
                    }
                };
                writers[index] = (w, o) -> w.writeDate(name, (LocalDate) field.get(o));
            } else if (LocalDateTime.class.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, TIMESTAMP)) {
                        field.set(o, reader.readTimestamp(name));
                    }
                };
                writers[index] = (w, o) -> w.writeTimestamp(name, (LocalDateTime) field.get(o));
            } else if (OffsetDateTime.class.equals(type)) {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, TIMESTAMP_WITH_TIMEZONE)) {
                        field.set(o, reader.readTimestampWithTimezone(name));
                    }
                };
                writers[index] = (w, o) -> w.writeTimestampWithTimezone(name, (OffsetDateTime) field.get(o));
            } else if (type.isArray()) {
                Class<?> componentType = type.getComponentType();
                if (Byte.TYPE.equals(componentType)) {
                    readers[index] = (reader, o) -> {
                        if (fieldExists(reader, name, BYTE_ARRAY)) {
                            field.set(o, reader.readByteArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeByteArray(name, (byte[]) field.get(o));
                } else if (Short.TYPE.equals(componentType)) {
                    readers[index] = (reader, o) -> {
                        if (fieldExists(reader, name, SHORT_ARRAY)) {
                            field.set(o, reader.readShortArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeShortArray(name, (short[]) field.get(o));
                } else if (Integer.TYPE.equals(componentType)) {
                    readers[index] = (reader, o) -> {
                        if (fieldExists(reader, name, INT_ARRAY)) {
                            field.set(o, reader.readIntArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeIntArray(name, (int[]) field.get(o));
                } else if (Long.TYPE.equals(componentType)) {
                    readers[index] = (reader, o) -> {
                        if (fieldExists(reader, name, LONG_ARRAY)) {
                            field.set(o, reader.readLongArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeLongArray(name, (long[]) field.get(o));
                } else if (Float.TYPE.equals(componentType)) {
                    readers[index] = (reader, o) -> {
                        if (fieldExists(reader, name, FLOAT_ARRAY)) {
                            field.set(o, reader.readFloatArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeFloatArray(name, (float[]) field.get(o));
                } else if (Double.TYPE.equals(componentType)) {
                    readers[index] = (reader, o) -> {
                        if (fieldExists(reader, name, DOUBLE_ARRAY)) {
                            field.set(o, reader.readDoubleArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeDoubleArray(name, (double[]) field.get(o));
                } else if (Boolean.TYPE.equals(componentType)) {
                    readers[index] = (reader, o) -> {
                        if (fieldExists(reader, name, BOOLEAN_ARRAY)) {
                            field.set(o, reader.readBooleanArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeBooleanArray(name, (boolean[]) field.get(o));
                } else if (Character.TYPE.equals(componentType)) {
                    readers[index] = (reader, o) -> {
                        if (fieldExists(reader, name, CHAR_ARRAY)) {
                            field.set(o, reader.readCharArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeCharArray(name, (char[]) field.get(o));
                } else if (String.class.equals(componentType)) {
                    readers[index] = (reader, o) -> {
                        if (fieldExists(reader, name, UTF_ARRAY)) {
                            field.set(o, reader.readStringArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeStringArray(name, (String[]) field.get(o));
                } else if (BigDecimal.class.equals(componentType)) {
                    readers[index] = (reader, o) -> {
                        if (fieldExists(reader, name, DECIMAL_ARRAY)) {
                            field.set(o, reader.readDecimalArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeDecimalArray(name, (BigDecimal[]) field.get(o));
                } else if (LocalTime.class.equals(componentType)) {
                    readers[index] = (reader, o) -> {
                        if (fieldExists(reader, name, TIME_ARRAY)) {
                            field.set(o, reader.readTimeArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeTimeArray(name, (LocalTime[]) field.get(o));
                } else if (LocalDate.class.equals(componentType)) {
                    readers[index] = (reader, o) -> {
                        if (fieldExists(reader, name, DATE_ARRAY)) {
                            field.set(o, reader.readDateArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeDateArray(name, (LocalDate[]) field.get(o));
                } else if (LocalDateTime.class.equals(componentType)) {
                    readers[index] = (reader, o) -> {
                        if (fieldExists(reader, name, TIMESTAMP_ARRAY)) {
                            field.set(o, reader.readTimestampArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeTimestampArray(name, (LocalDateTime[]) field.get(o));
                } else if (OffsetDateTime.class.equals(componentType)) {
                    readers[index] = (reader, o) -> {
                        if (fieldExists(reader, name, TIMESTAMP_WITH_TIMEZONE_ARRAY)) {
                            field.set(o, reader.readTimestampWithTimezoneArray(name));
                        }
                    };
                    writers[index] = (w, o) -> w.writeTimestampWithTimezoneArray(name, (OffsetDateTime[]) field.get(o));
                } else {
                    readers[index] = (reader, o) -> {
                        if (fieldExists(reader, name, COMPOSED_ARRAY)) {
                            field.set(o, reader.readObjectArray(name, componentType));
                        }
                    };
                    writers[index] = (w, o) -> w.writeObjectArray(name, (Object[]) field.get(o));
                }
            } else {
                readers[index] = (reader, o) -> {
                    if (fieldExists(reader, name, COMPOSED)) {
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
}
