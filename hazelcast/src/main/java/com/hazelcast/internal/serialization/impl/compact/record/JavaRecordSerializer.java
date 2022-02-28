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

package com.hazelcast.internal.serialization.impl.compact.record;

import com.hazelcast.internal.serialization.impl.compact.DefaultCompactReader;
import com.hazelcast.internal.serialization.impl.compact.FieldDescriptor;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

import javax.annotation.Nonnull;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


import static java.util.Objects.requireNonNull;

public class JavaRecordSerializer implements CompactSerializer<Object> {

    private final Method isRecordMethod;
    private final Method getRecordComponentsMethod;
    private final Method getTypeMethod;
    private final Method getNameMethod;

    private final Map<Class<?>, JavaRecordReader> readersCache = new ConcurrentHashMap<>();
    private final Map<Class<?>, ComponentWriter[]> writersCache = new ConcurrentHashMap<>();

    public JavaRecordSerializer() {
        Method isRecordMethod;
        Method getRecordComponentsMethod;
        Method getTypeMethod;
        Method getNameMethod;

        try {
            isRecordMethod = Class.class.getMethod("isRecord");
            getRecordComponentsMethod = Class.class.getMethod("getRecordComponents");
            Class<?> recordComponentClass = Class.forName("java.lang.reflect.RecordComponent");
            getTypeMethod = recordComponentClass.getMethod("getType");
            getNameMethod = recordComponentClass.getMethod("getName");
        } catch (Throwable t) {
            isRecordMethod = null;
            getRecordComponentsMethod = null;
            getTypeMethod = null;
            getNameMethod = null;
        }

        this.isRecordMethod = isRecordMethod;
        this.getRecordComponentsMethod = getRecordComponentsMethod;
        this.getTypeMethod = getTypeMethod;
        this.getNameMethod = getNameMethod;
    }

    public boolean isRecord(Class<?> clazz) {
        if (isRecordMethod == null) {
            return false;
        }

        try {
            return (boolean) isRecordMethod.invoke(clazz);
        } catch (Throwable t) {
            return false;
        }
    }

    @Nonnull
    @Override
    public Object read(@Nonnull CompactReader reader) {
        DefaultCompactReader compactReader = (DefaultCompactReader) reader;
        Class<?> associatedClass = requireNonNull(compactReader.getAssociatedClass(),
                "AssociatedClass is required for JavaRecordSerializer");

        JavaRecordReader recordReader = readersCache.get(associatedClass);
        if (recordReader == null) {
            populateReadersWriters(associatedClass);
            recordReader = readersCache.get(associatedClass);
        }

        return recordReader.readRecord(compactReader, compactReader.getSchema());
    }

    @Override
    public void write(@Nonnull CompactWriter compactWriter, @Nonnull Object object) {
        Class<?> clazz = object.getClass();
        ComponentWriter[] componentWriters = writersCache.get(clazz);
        if (componentWriters == null) {
            populateReadersWriters(clazz);
            componentWriters = writersCache.get(clazz);
        }

        try {
            for (ComponentWriter componentWriter : componentWriters) {
                componentWriter.writeComponent(compactWriter, object);
            }
        } catch (Exception e) {
            throw new HazelcastSerializationException("Failed to write the Java record", e);
        }
    }

    private void populateReadersWriters(Class<?> clazz) {
        try {
            Object[] recordComponents = (Object[]) getRecordComponentsMethod.invoke(clazz);
            Class<?>[] componentTypes = new Class<?>[recordComponents.length];

            ComponentReader[] componentReaders = new ComponentReader[recordComponents.length];
            ComponentWriter[] componentWriters = new ComponentWriter[recordComponents.length];

            for (int i = 0; i < recordComponents.length; i++) {
                Object recordComponent = recordComponents[i];
                Class<?> type = (Class<?>) getTypeMethod.invoke(recordComponent);
                String name = (String) getNameMethod.invoke(recordComponent);

                componentTypes[i] = type;
                Method componentGetter = clazz.getDeclaredMethod(name);
                componentGetter.setAccessible(true);

                // In case the component we want to read does not exist, we use
                // a default value for it, so that the schema evolution works
                // frictionless for reflectively serialized record objects.
                if (Byte.TYPE.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.INT8, FieldKind.NULLABLE_INT8)) {
                            return (byte) 0;
                        }
                        return compactReader.readInt8(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeInt8(name, (byte) componentGetter.invoke(object));
                } else if (Byte.class.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.NULLABLE_INT8, FieldKind.INT8)) {
                            return null;
                        }
                        return compactReader.readNullableInt8(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeNullableInt8(name, (Byte) componentGetter.invoke(object));
                } else if (Character.TYPE.equals(type)) {
                    throwUnsupportedFieldTypeException("char");
                } else if (Character.class.equals(type)) {
                    throwUnsupportedFieldTypeException("Character");
                } else if (Short.TYPE.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.INT16, FieldKind.NULLABLE_INT16)) {
                            return (short) 0;
                        }
                        return compactReader.readInt16(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeInt16(name, (short) componentGetter.invoke(object));
                } else if (Short.class.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.NULLABLE_INT16, FieldKind.INT16)) {
                            return null;
                        }
                        return compactReader.readNullableInt16(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeNullableInt16(name, (Short) componentGetter.invoke(object));
                } else if (Integer.TYPE.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.INT32, FieldKind.NULLABLE_INT32)) {
                            return 0;
                        }
                        return compactReader.readInt32(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeInt32(name, (int) componentGetter.invoke(object));
                } else if (Integer.class.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.NULLABLE_INT32, FieldKind.INT32)) {
                            return null;
                        }
                        return compactReader.readNullableInt32(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeNullableInt32(name, (Integer) componentGetter.invoke(object));
                } else if (Long.TYPE.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.INT64, FieldKind.NULLABLE_INT64)) {
                            return 0L;
                        }
                        return compactReader.readInt64(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeInt64(name, (long) componentGetter.invoke(object));
                } else if (Long.class.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.NULLABLE_INT64, FieldKind.INT64)) {
                            return null;
                        }
                        return compactReader.readNullableInt64(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeNullableInt64(name, (Long) componentGetter.invoke(object));
                } else if (Float.TYPE.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.FLOAT32, FieldKind.NULLABLE_FLOAT32)) {
                            return 0F;
                        }
                        return compactReader.readFloat32(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeFloat32(name, (float) componentGetter.invoke(object));
                } else if (Float.class.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.NULLABLE_FLOAT32, FieldKind.FLOAT32)) {
                            return null;
                        }
                        return compactReader.readNullableFloat32(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeNullableFloat32(name, (Float) componentGetter.invoke(object));
                } else if (Double.TYPE.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.FLOAT64, FieldKind.NULLABLE_FLOAT64)) {
                            return 0D;
                        }
                        return compactReader.readFloat64(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeFloat64(name, (double) componentGetter.invoke(object));
                } else if (Double.class.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.NULLABLE_FLOAT64, FieldKind.FLOAT64)) {
                            return null;
                        }
                        return compactReader.readNullableFloat64(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeNullableFloat64(name, (Double) componentGetter.invoke(object));
                } else if (Boolean.TYPE.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.BOOLEAN, FieldKind.NULLABLE_BOOLEAN)) {
                            return false;
                        }
                        return compactReader.readBoolean(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeBoolean(name, (boolean) componentGetter.invoke(object));
                } else if (Boolean.class.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.NULLABLE_BOOLEAN, FieldKind.BOOLEAN)) {
                            return null;
                        }
                        return compactReader.readNullableBoolean(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeNullableBoolean(name, (Boolean) componentGetter.invoke(object));
                } else if (String.class.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.STRING)) {
                            return null;
                        }
                        return compactReader.readString(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeString(name, (String) componentGetter.invoke(object));
                } else if (BigDecimal.class.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.DECIMAL)) {
                            return null;
                        }
                        return compactReader.readDecimal(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeDecimal(name, (BigDecimal) componentGetter.invoke(object));
                } else if (LocalTime.class.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.TIME)) {
                            return null;
                        }
                        return compactReader.readTime(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeTime(name, (LocalTime) componentGetter.invoke(object));
                } else if (LocalDate.class.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.DATE)) {
                            return null;
                        }
                        return compactReader.readDate(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeDate(name, (LocalDate) componentGetter.invoke(object));
                } else if (LocalDateTime.class.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.TIMESTAMP)) {
                            return null;
                        }
                        return compactReader.readTimestamp(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeTimestamp(name, (LocalDateTime) componentGetter.invoke(object));
                } else if (OffsetDateTime.class.equals(type)) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.TIMESTAMP_WITH_TIMEZONE)) {
                            return null;
                        }
                        return compactReader.readTimestampWithTimezone(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeTimestampWithTimezone(name, (OffsetDateTime) componentGetter.invoke(object));
                } else if (type.isEnum()) {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.STRING)) {
                            return null;
                        }
                        String enumName = compactReader.readString(name, null);
                        return enumName == null ? null : Enum.valueOf((Class<? extends Enum>) type, enumName);
                    };
                    componentWriters[i] = (compactWriter, object) -> {
                        Object rawValue = componentGetter.invoke(object);
                        String value = rawValue == null ? null : ((Enum) rawValue).name();
                        compactWriter.writeString(name, value);
                    };
                } else if (type.isArray()) {
                    Class<?> componentType = type.getComponentType();
                    if (Byte.TYPE.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_INT8, FieldKind.ARRAY_OF_NULLABLE_INT8)) {
                                return null;
                            }
                            return compactReader.readArrayOfInt8(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfInt8(name, (byte[]) componentGetter.invoke(object));
                    } else if (Byte.class.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_NULLABLE_INT8, FieldKind.ARRAY_OF_INT8)) {
                                return null;
                            }
                            return compactReader.readArrayOfNullableInt8(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfNullableInt8(name, (Byte[]) componentGetter.invoke(object));
                    } else if (Character.TYPE.equals(componentType)) {
                        throwUnsupportedFieldTypeException("char[]");
                    } else if (Character.class.equals(componentType)) {
                        throwUnsupportedFieldTypeException("Character[]");
                    } else if (Short.TYPE.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_INT16, FieldKind.ARRAY_OF_NULLABLE_INT16)) {
                                return null;
                            }
                            return compactReader.readArrayOfInt16(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfInt16(name, (short[]) componentGetter.invoke(object));
                    } else if (Short.class.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_NULLABLE_INT16, FieldKind.ARRAY_OF_INT16)) {
                                return null;
                            }
                            return compactReader.readArrayOfNullableInt16(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfNullableInt16(name, (Short[]) componentGetter.invoke(object));
                    } else if (Integer.TYPE.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_INT32, FieldKind.ARRAY_OF_NULLABLE_INT32)) {
                                return null;
                            }
                            return compactReader.readArrayOfInt32(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfInt32(name, (int[]) componentGetter.invoke(object));
                    } else if (Integer.class.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_NULLABLE_INT32, FieldKind.ARRAY_OF_INT32)) {
                                return null;
                            }
                            return compactReader.readArrayOfNullableInt32(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfNullableInt32(name, (Integer[]) componentGetter.invoke(object));
                    } else if (Long.TYPE.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_INT64, FieldKind.ARRAY_OF_NULLABLE_INT64)) {
                                return null;
                            }
                            return compactReader.readArrayOfInt64(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfInt64(name, (long[]) componentGetter.invoke(object));
                    } else if (Long.class.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_NULLABLE_INT64, FieldKind.ARRAY_OF_INT64)) {
                                return null;
                            }
                            return compactReader.readArrayOfNullableInt64(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfNullableInt64(name, (Long[]) componentGetter.invoke(object));
                    } else if (Float.TYPE.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_FLOAT32, FieldKind.ARRAY_OF_NULLABLE_FLOAT32)) {
                                return null;
                            }
                            return compactReader.readArrayOfFloat32(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfFloat32(name, (float[]) componentGetter.invoke(object));
                    } else if (Float.class.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_NULLABLE_FLOAT32, FieldKind.ARRAY_OF_FLOAT32)) {
                                return null;
                            }
                            return compactReader.readArrayOfNullableFloat32(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfNullableFloat32(name, (Float[]) componentGetter.invoke(object));
                    } else if (Double.TYPE.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_FLOAT64, FieldKind.ARRAY_OF_NULLABLE_FLOAT64)) {
                                return null;
                            }
                            return compactReader.readArrayOfFloat64(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfFloat64(name, (double[]) componentGetter.invoke(object));
                    } else if (Double.class.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_NULLABLE_FLOAT64, FieldKind.ARRAY_OF_FLOAT64)) {
                                return null;
                            }
                            return compactReader.readArrayOfNullableFloat64(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfNullableFloat64(name, (Double[]) componentGetter.invoke(object));
                    } else if (Boolean.TYPE.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_BOOLEAN, FieldKind.ARRAY_OF_NULLABLE_BOOLEAN)) {
                                return null;
                            }
                            return compactReader.readArrayOfBoolean(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfBoolean(name, (boolean[]) componentGetter.invoke(object));
                    } else if (Boolean.class.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_NULLABLE_BOOLEAN, FieldKind.ARRAY_OF_BOOLEAN)) {
                                return null;
                            }
                            return compactReader.readArrayOfNullableBoolean(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfNullableBoolean(name, (Boolean[]) componentGetter.invoke(object));
                    } else if (String.class.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_STRING)) {
                                return null;
                            }
                            return compactReader.readArrayOfString(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfString(name, (String[]) componentGetter.invoke(object));
                    } else if (BigDecimal.class.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_DECIMAL)) {
                                return null;
                            }
                            return compactReader.readArrayOfDecimal(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfDecimal(name, (BigDecimal[]) componentGetter.invoke(object));
                    } else if (LocalTime.class.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_TIME)) {
                                return null;
                            }
                            return compactReader.readArrayOfTime(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfTime(name, (LocalTime[]) componentGetter.invoke(object));
                    } else if (LocalDate.class.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_DATE)) {
                                return null;
                            }
                            return compactReader.readArrayOfDate(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfDate(name, (LocalDate[]) componentGetter.invoke(object));
                    } else if (LocalDateTime.class.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_TIMESTAMP)) {
                                return null;
                            }
                            return compactReader.readArrayOfTimestamp(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfTimestamp(name, (LocalDateTime[]) componentGetter.invoke(object));
                    } else if (OffsetDateTime.class.equals(componentType)) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE)) {
                                return null;
                            }
                            return compactReader.readArrayOfTimestampWithTimezone(name);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfTimestampWithTimezone(
                                        name, (OffsetDateTime[]) componentGetter.invoke(object));
                    } else if (componentType.isEnum()) {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_STRING)) {
                                return null;
                            }
                            String[] enumNames = compactReader.readArrayOfString(name);
                            if (enumNames == null) {
                                return null;
                            }

                            Class<? extends Enum> enumType = (Class<? extends Enum>) componentType;
                            Enum[] enums = (Enum[]) Array.newInstance(enumType, enumNames.length);
                            for (int j = 0; j < enumNames.length; j++) {
                                String enumName = enumNames[j];
                                enums[j] = enumName == null ? null : Enum.valueOf(enumType, enumName);
                            }
                            return enums;
                        };
                        componentWriters[i] = (compactWriter, object) -> {
                            Enum[] enums = (Enum[]) componentGetter.invoke(object);
                            String[] enumNames = null;

                            if (enums != null) {
                                enumNames = new String[enums.length];
                                for (int j = 0; j < enums.length; j++) {
                                    Enum e = enums[j];
                                    enumNames[j] = e == null ? null : e.name();
                                }
                            }

                            compactWriter.writeArrayOfString(name, enumNames);
                        };
                    } else {
                        componentReaders[i] = (compactReader, schema) -> {
                            if (!isFieldExist(schema, name, FieldKind.ARRAY_OF_COMPACT)) {
                                return null;
                            }
                            return compactReader.readArrayOfCompact(name, componentType);
                        };
                        componentWriters[i] = (compactWriter, object)
                                -> compactWriter.writeArrayOfCompact(name, (Object[]) componentGetter.invoke(object));
                    }
                } else {
                    componentReaders[i] = (compactReader, schema) -> {
                        if (!isFieldExist(schema, name, FieldKind.COMPACT)) {
                            return null;
                        }
                        return compactReader.readCompact(name);
                    };
                    componentWriters[i] = (compactWriter, object)
                            -> compactWriter.writeCompact(name, componentGetter.invoke(object));
                }
            }

            Constructor<?> constructor = clazz.getDeclaredConstructor(componentTypes);
            constructor.setAccessible(true);

            JavaRecordReader recordReader = new JavaRecordReader(constructor, componentReaders);
            readersCache.put(clazz, recordReader);
            writersCache.put(clazz, componentWriters);
        } catch (Exception e) {
            throw new HazelcastSerializationException("Failed to construct the readers/writers for the Java record", e);
        }
    }

    private boolean isFieldExist(Schema schema, String fieldName, FieldKind expectedFieldKind) {
        FieldDescriptor field = schema.getField(fieldName);
        if (field == null) {
            return false;
        }

        return field.getKind() == expectedFieldKind;
    }

    private boolean isFieldExist(Schema schema, String fieldName, FieldKind expectedFieldKind, FieldKind compatibleFieldKind) {
        FieldDescriptor field = schema.getField(fieldName);
        if (field == null) {
            return false;
        }

        return field.getKind() == expectedFieldKind || field.getKind() == compatibleFieldKind;
    }

    private void throwUnsupportedFieldTypeException(String typeName) {
        throw new HazelcastSerializationException("Compact serialization format does not support "
                + "fields of type '" + typeName + "'. If you want to use such fields with the compact"
                + " serialization format, consider adding an explicit serializer for it.");
    }
}
