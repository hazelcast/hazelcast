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

import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeMap;

import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.exceptionForUnexpectedNullValue;
import static com.hazelcast.internal.serialization.impl.compact.CompactUtil.exceptionForUnexpectedNullValueInArray;
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
 * See the javadoc of {@link InternalGenericRecord} for GenericRecord class hierarchy.
 */
public class DeserializedGenericRecord extends CompactGenericRecord {

    private final TreeMap<String, Object> objects;
    private final Schema schema;

    public DeserializedGenericRecord(Schema schema, TreeMap<String, Object> objects) {
        this.schema = schema;
        this.objects = objects;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Nonnull
    @Override
    public GenericRecordBuilder newBuilder() {
        return new DeserializedSchemaBoundGenericRecordBuilder(schema);
    }

    @Nonnull
    @Override
    public GenericRecordBuilder cloneWithBuilder() {
        return new DeserializedGenericRecordCloner(schema, objects);
    }

    @Nonnull
    @Override
    public FieldKind getFieldKind(@Nonnull String fieldName) {
        FieldDescriptor field = schema.getField(fieldName);
        if (field == null) {
            throw new IllegalArgumentException("Field name " + fieldName + " does not exist in the schema");
        }
        return field.getKind();
    }

    @Override
    public boolean hasField(@Nonnull String fieldName) {
        return objects.containsKey(fieldName);
    }

    @Override
    @Nonnull
    public Set<String> getFieldNames() {
        return objects.keySet();
    }

    @Override
    public boolean getBoolean(@Nonnull String fieldName) {
        return getNonNull(fieldName, BOOLEAN, NULLABLE_BOOLEAN, "Boolean");
    }

    @Override
    public byte getInt8(@Nonnull String fieldName) {
        return getNonNull(fieldName, INT8, NULLABLE_INT8, "Int8");
    }

    @Override
    public char getChar(@Nonnull String fieldName) {
        throw new UnsupportedOperationException("Compact format does not support reading a char field");
    }

    @Override
    public double getFloat64(@Nonnull String fieldName) {
        return getNonNull(fieldName, FLOAT64, NULLABLE_FLOAT64, "Float64");
    }

    @Override
    public float getFloat32(@Nonnull String fieldName) {
        return getNonNull(fieldName, FLOAT32, NULLABLE_FLOAT32, "Float32");
    }

    @Override
    public int getInt32(@Nonnull String fieldName) {
        return getNonNull(fieldName, INT32, NULLABLE_INT32, "Int32");
    }

    @Override
    public long getInt64(@Nonnull String fieldName) {
        return getNonNull(fieldName, INT64, NULLABLE_INT64, "Int64");
    }

    @Override
    public short getInt16(@Nonnull String fieldName) {
        return getNonNull(fieldName, INT16, NULLABLE_INT16, "Int16");
    }

    @Override
    @Nullable
    public String getString(@Nonnull String fieldName) {
        return get(fieldName, STRING);
    }

    @Override
    @Nullable
    public BigDecimal getDecimal(@Nonnull String fieldName) {
        return get(fieldName, DECIMAL);
    }

    @Nullable
    @Override
    public LocalTime getTime(@Nonnull String fieldName) {
        return get(fieldName, TIME);
    }

    @Nullable
    @Override
    public LocalDate getDate(@Nonnull String fieldName) {
        return get(fieldName, DATE);
    }

    @Nullable
    @Override
    public LocalDateTime getTimestamp(@Nonnull String fieldName) {
        return get(fieldName, TIMESTAMP);
    }

    @Nullable
    @Override
    public OffsetDateTime getTimestampWithTimezone(@Nonnull String fieldName) {
        return get(fieldName, TIMESTAMP_WITH_TIMEZONE);
    }

    @Nullable
    @Override
    public GenericRecord getGenericRecord(@Nonnull String fieldName) {
        return get(fieldName, COMPACT);
    }

    @Override
    @Nullable
    public boolean[] getArrayOfBoolean(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_BOOLEAN, ARRAY_OF_NULLABLE_BOOLEAN);
        if (fieldKind == ARRAY_OF_NULLABLE_BOOLEAN) {
            Boolean[] array = (Boolean[]) objects.get(fieldName);
            boolean[] result = new boolean[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] == null) {
                    throw exceptionForUnexpectedNullValueInArray(fieldName, "Boolean");
                }
                result[i] = array[i];
            }
            return result;
        }
        return (boolean[]) objects.get(fieldName);
    }

    @Override
    @Nullable
    public byte[] getArrayOfInt8(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_INT8, ARRAY_OF_NULLABLE_INT8);
        if (fieldKind == ARRAY_OF_NULLABLE_INT8) {
            Byte[] array = (Byte[]) objects.get(fieldName);
            byte[] result = new byte[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] == null) {
                    throw exceptionForUnexpectedNullValueInArray(fieldName, "Int8");
                }
                result[i] = array[i];
            }
            return result;
        }
        return (byte[]) objects.get(fieldName);
    }

    @Override
    @Nullable
    public char[] getArrayOfChar(@Nonnull String fieldName) {
        throw new UnsupportedOperationException("Compact format does not support reading an array of chars field");
    }

    @Override
    @Nullable
    public double[] getArrayOfFloat64(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_FLOAT64, ARRAY_OF_NULLABLE_FLOAT64);
        if (fieldKind == ARRAY_OF_NULLABLE_FLOAT64) {
            Double[] array = (Double[]) objects.get(fieldName);
            double[] result = new double[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] == null) {
                    throw exceptionForUnexpectedNullValueInArray(fieldName, "Float64");
                }
                result[i] = array[i];
            }
            return result;
        }
        return (double[]) objects.get(fieldName);
    }

    @Override
    @Nullable
    public float[] getArrayOfFloat32(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_FLOAT32, ARRAY_OF_NULLABLE_FLOAT32);
        if (fieldKind == ARRAY_OF_NULLABLE_FLOAT32) {
            Float[] array = (Float[]) objects.get(fieldName);
            float[] result = new float[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] == null) {
                    throw exceptionForUnexpectedNullValueInArray(fieldName, "Float32");
                }
                result[i] = array[i];
            }
            return result;
        }
        return (float[]) objects.get(fieldName);
    }

    @Override
    @Nullable
    public int[] getArrayOfInt32(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_INT32, ARRAY_OF_NULLABLE_INT32);
        if (fieldKind == ARRAY_OF_NULLABLE_INT32) {
            Integer[] array = (Integer[]) objects.get(fieldName);
            int[] result = new int[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] == null) {
                    throw exceptionForUnexpectedNullValueInArray(fieldName, "Int32");
                }
                result[i] = array[i];
            }
            return result;
        }
        return (int[]) objects.get(fieldName);
    }

    @Override
    @Nullable
    public long[] getArrayOfInt64(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_INT64, ARRAY_OF_NULLABLE_INT64);
        if (fieldKind == ARRAY_OF_NULLABLE_INT64) {
            Long[] array = (Long[]) objects.get(fieldName);
            long[] result = new long[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] == null) {
                    throw exceptionForUnexpectedNullValueInArray(fieldName, "Int64");
                }
                result[i] = array[i];
            }
            return result;
        }
        return (long[]) objects.get(fieldName);
    }

    @Override
    @Nullable
    public short[] getArrayOfInt16(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_INT16, ARRAY_OF_NULLABLE_INT16);
        if (fieldKind == ARRAY_OF_NULLABLE_INT16) {
            Short[] array = (Short[]) objects.get(fieldName);
            short[] result = new short[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] == null) {
                    throw exceptionForUnexpectedNullValueInArray(fieldName, "Int16");
                }
                result[i] = array[i];
            }
            return result;
        }
        return (short[]) objects.get(fieldName);
    }

    @Override
    @Nullable
    public String[] getArrayOfString(@Nonnull String fieldName) {
        return get(fieldName, ARRAY_OF_STRING);
    }

    @Override
    @Nullable
    public BigDecimal[] getArrayOfDecimal(@Nonnull String fieldName) {
        return get(fieldName, ARRAY_OF_DECIMAL);
    }

    @Override
    @Nullable
    public LocalTime[] getArrayOfTime(@Nonnull String fieldName) {
        return get(fieldName, ARRAY_OF_TIME);
    }

    @Override
    @Nullable
    public LocalDate[] getArrayOfDate(@Nonnull String fieldName) {
        return get(fieldName, ARRAY_OF_DATE);
    }

    @Override
    @Nullable
    public LocalDateTime[] getArrayOfTimestamp(@Nonnull String fieldName) {
        return get(fieldName, ARRAY_OF_TIMESTAMP);
    }

    @Override
    @Nullable
    public OffsetDateTime[] getArrayOfTimestampWithTimezone(@Nonnull String fieldName) {
        return get(fieldName, ARRAY_OF_TIMESTAMP_WITH_TIMEZONE);
    }

    @Nullable
    @Override
    public GenericRecord[] getArrayOfGenericRecord(@Nonnull String fieldName) {
        return get(fieldName, ARRAY_OF_COMPACT);
    }

    @Nullable
    @Override
    public Boolean getNullableBoolean(@Nonnull String fieldName) {
        return get(fieldName, BOOLEAN, NULLABLE_BOOLEAN);
    }

    @Nullable
    @Override
    public Byte getNullableInt8(@Nonnull String fieldName) {
        return get(fieldName, INT8, NULLABLE_INT8);
    }

    @Nullable
    @Override
    public Double getNullableFloat64(@Nonnull String fieldName) {
        return get(fieldName, FLOAT64, NULLABLE_FLOAT64);
    }

    @Nullable
    @Override
    public Float getNullableFloat32(@Nonnull String fieldName) {
        return get(fieldName, FLOAT32, NULLABLE_FLOAT32);
    }

    @Nullable
    @Override
    public Integer getNullableInt32(@Nonnull String fieldName) {
        return get(fieldName, INT32, NULLABLE_INT32);
    }

    @Nullable
    @Override
    public Long getNullableInt64(@Nonnull String fieldName) {
        return get(fieldName, INT64, NULLABLE_INT64);
    }

    @Nullable
    @Override
    public Short getNullableInt16(@Nonnull String fieldName) {
        return get(fieldName, INT16, NULLABLE_INT16);
    }

    @Nullable
    @Override
    public Boolean[] getArrayOfNullableBoolean(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_BOOLEAN, ARRAY_OF_NULLABLE_BOOLEAN);
        if (fieldKind == ARRAY_OF_BOOLEAN) {
            boolean[] array = (boolean[]) objects.get(fieldName);
            Boolean[] result = new Boolean[array.length];
            Arrays.setAll(result, i -> array[i]);
            return result;
        }
        return (Boolean[]) objects.get(fieldName);
    }

    @Nullable
    @Override
    public Byte[] getArrayOfNullableInt8(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_INT8, ARRAY_OF_NULLABLE_INT8);
        if (fieldKind == ARRAY_OF_INT8) {
            byte[] array = (byte[]) objects.get(fieldName);
            Byte[] result = new Byte[array.length];
            Arrays.setAll(result, i -> array[i]);
            return result;
        }
        return (Byte[]) objects.get(fieldName);
    }

    @Nullable
    @Override
    public Double[] getArrayOfNullableFloat64(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_FLOAT64, ARRAY_OF_NULLABLE_FLOAT64);
        if (fieldKind == ARRAY_OF_FLOAT64) {
            double[] array = (double[]) objects.get(fieldName);
            Double[] result = new Double[array.length];
            Arrays.setAll(result, i -> array[i]);
            return result;
        }
        return (Double[]) objects.get(fieldName);
    }

    @Nullable
    @Override
    public Float[] getArrayOfNullableFloat32(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_FLOAT32, ARRAY_OF_NULLABLE_FLOAT32);
        if (fieldKind == ARRAY_OF_FLOAT32) {
            float[] array = (float[]) objects.get(fieldName);
            Float[] result = new Float[array.length];
            Arrays.setAll(result, i -> array[i]);
            return result;
        }
        return (Float[]) objects.get(fieldName);
    }

    @Nullable
    @Override
    public Integer[] getArrayOfNullableInt32(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_INT32, ARRAY_OF_NULLABLE_INT32);
        if (fieldKind == ARRAY_OF_INT32) {
            int[] array = (int[]) objects.get(fieldName);
            Integer[] result = new Integer[array.length];
            Arrays.setAll(result, i -> array[i]);
            return result;
        }
        return (Integer[]) objects.get(fieldName);
    }

    @Nullable
    @Override
    public Long[] getArrayOfNullableInt64(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_INT64, ARRAY_OF_NULLABLE_INT64);
        if (fieldKind == ARRAY_OF_INT64) {
            long[] array = (long[]) objects.get(fieldName);
            Long[] result = new Long[array.length];
            Arrays.setAll(result, i -> array[i]);
            return result;
        }
        return (Long[]) objects.get(fieldName);
    }

    @Nullable
    @Override
    public Short[] getArrayOfNullableInt16(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_INT16, ARRAY_OF_NULLABLE_INT16);
        if (fieldKind == ARRAY_OF_INT16) {
            short[] array = (short[]) objects.get(fieldName);
            Short[] result = new Short[array.length];
            Arrays.setAll(result, i -> array[i]);
            return result;
        }
        return (Short[]) objects.get(fieldName);
    }

    private <T> T get(@Nonnull String fieldName, @Nonnull FieldKind primitiveFieldKind,
                      @Nonnull FieldKind nullableFieldKind) {
        check(fieldName, primitiveFieldKind, nullableFieldKind);
        return (T) objects.get(fieldName);
    }

    private <T> T getNonNull(@Nonnull String fieldName, @Nonnull FieldKind primitiveFieldKind,
                             @Nonnull FieldKind nullableFieldKind, String methodSuffix) {
        check(fieldName, primitiveFieldKind, nullableFieldKind);
        T t = (T) objects.get(fieldName);
        if (t == null) {
            throw exceptionForUnexpectedNullValue(fieldName, methodSuffix);
        }
        return t;
    }

    private <T> T get(@Nonnull String fieldName, @Nonnull FieldKind fieldKind) {
        check(fieldName, fieldKind);
        return (T) objects.get(fieldName);
    }

    private FieldKind check(@Nonnull String fieldName, @Nonnull FieldKind... kinds) {
        FieldDescriptor fd = schema.getField(fieldName);
        if (fd == null) {
            throw new HazelcastSerializationException("Invalid field name: '" + fieldName + " for " + schema);
        }
        boolean valid = false;
        FieldKind fieldKind = fd.getKind();
        for (FieldKind kind : kinds) {
            valid |= fieldKind == kind;
        }
        if (!valid) {
            throw new HazelcastSerializationException("Invalid field kind: '" + fieldName + " for " + schema
                    + ", valid field kinds : " + Arrays.toString(kinds) + ", found : " + fieldKind);
        }
        return fieldKind;
    }

    @Override
    protected Object getClassIdentifier() {
        return schema.getTypeName();
    }

    @Nullable
    @Override
    public InternalGenericRecord getInternalGenericRecord(@Nonnull String fieldName) {
        return get(fieldName, COMPACT);
    }

    @Nullable
    @Override
    public InternalGenericRecord[] getArrayOfInternalGenericRecord(@Nonnull String fieldName) {
        return get(fieldName, ARRAY_OF_COMPACT);
    }

    @Nullable
    @Override
    public Boolean getBooleanFromArray(@Nonnull String fieldName, int index) {
        boolean[] array = getArrayOfBoolean(fieldName);
        if (array == null || array.length <= index) {
            return null;
        }
        return array[index];
    }

    @Nullable
    @Override
    public Byte getInt8FromArray(@Nonnull String fieldName, int index) {
        byte[] array = getArrayOfInt8(fieldName);
        if (array == null || array.length <= index) {
            return null;
        }
        return array[index];
    }

    @Nullable
    @Override
    public Character getCharFromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException("Compact format does not support reading a char field");
    }

    @Nullable
    @Override
    public Double getFloat64FromArray(@Nonnull String fieldName, int index) {
        double[] array = getArrayOfFloat64(fieldName);
        if (array == null || array.length <= index) {
            return null;
        }
        return array[index];
    }

    @Nullable
    @Override
    public Float getFloat32FromArray(@Nonnull String fieldName, int index) {
        float[] array = getArrayOfFloat32(fieldName);
        if (array == null || array.length <= index) {
            return null;
        }
        return array[index];
    }

    @Nullable
    @Override
    public Integer getInt32FromArray(@Nonnull String fieldName, int index) {
        int[] array = getArrayOfInt32(fieldName);
        if (array == null || array.length <= index) {
            return null;
        }
        return array[index];
    }

    @Nullable
    @Override
    public Long getInt64FromArray(@Nonnull String fieldName, int index) {
        long[] array = getArrayOfInt64(fieldName);
        if (array == null || array.length <= index) {
            return null;
        }
        return array[index];
    }

    @Nullable
    @Override
    public Short getInt16FromArray(@Nonnull String fieldName, int index) {
        short[] array = getArrayOfInt16(fieldName);
        if (array == null || array.length <= index) {
            return null;
        }
        return array[index];
    }

    @Nullable
    @Override
    public String getStringFromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfString(fieldName), index);
    }

    @Nullable
    @Override
    public GenericRecord getGenericRecordFromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfGenericRecord(fieldName), index);
    }

    @Nullable
    @Override
    public InternalGenericRecord getInternalGenericRecordFromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfInternalGenericRecord(fieldName), index);
    }

    @Nullable
    @Override
    public Object getObjectFromArray(@Nonnull String fieldName, int index) {
        return getGenericRecordFromArray(fieldName, index);
    }

    @Nullable
    @Override
    public <T> T[] getArrayOfObject(@Nonnull String fieldName, Class<T> componentType) {
        return (T[]) getArrayOfGenericRecord(fieldName);
    }

    @Nullable
    @Override
    public Object getObject(@Nonnull String fieldName) {
        return getGenericRecord(fieldName);
    }

    @Nullable
    @Override
    public BigDecimal getDecimalFromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfDecimal(fieldName), index);
    }

    @Nullable
    @Override
    public LocalTime getTimeFromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfTime(fieldName), index);
    }

    @Nullable
    @Override
    public LocalDate getDateFromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfDate(fieldName), index);
    }

    @Nullable
    @Override
    public LocalDateTime getTimestampFromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfTimestamp(fieldName), index);
    }

    @Nullable
    @Override
    public OffsetDateTime getTimestampWithTimezoneFromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfTimestampWithTimezone(fieldName), index);
    }

    @Nullable
    @Override
    public Boolean getNullableBooleanFromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfNullableBoolean(fieldName), index);
    }

    @Nullable
    @Override
    public Byte getNullableInt8FromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfNullableInt8(fieldName), index);
    }

    @Nullable
    @Override
    public Short getNullableInt16FromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfNullableInt16(fieldName), index);
    }

    @Nullable
    @Override
    public Integer getNullableInt32FromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfNullableInt32(fieldName), index);
    }

    @Nullable
    @Override
    public Long getNullableInt64FromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfNullableInt64(fieldName), index);
    }

    @Nullable
    @Override
    public Float getNullableFloat32FromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfNullableFloat32(fieldName), index);
    }

    @Nullable
    @Override
    public Double getNullableFloat64FromArray(@Nonnull String fieldName, int index) {
        return getFromArray(getArrayOfNullableFloat64(fieldName), index);
    }

    private <T> T getFromArray(T[] array, int index) {
        if (array == null || array.length <= index) {
            return null;
        }
        return array[index];
    }
}
