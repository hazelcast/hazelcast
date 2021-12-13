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
        return schema.getField(fieldName).getKind();
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
    public byte getByte(@Nonnull String fieldName) {
        return getNonNull(fieldName, BYTE, NULLABLE_BYTE, "Byte");
    }

    @Override
    public char getChar(@Nonnull String fieldName) {
        throw new UnsupportedOperationException("Compact format does not support reading a char field");
    }

    @Override
    public double getDouble(@Nonnull String fieldName) {
        return getNonNull(fieldName, DOUBLE, NULLABLE_DOUBLE, "Double");
    }

    @Override
    public float getFloat(@Nonnull String fieldName) {
        return getNonNull(fieldName, FLOAT, NULLABLE_FLOAT, "Float");
    }

    @Override
    public int getInt(@Nonnull String fieldName) {
        return getNonNull(fieldName, INT, NULLABLE_INT, "Int");
    }

    @Override
    public long getLong(@Nonnull String fieldName) {
        return getNonNull(fieldName, LONG, NULLABLE_LONG, "Long");
    }

    @Override
    public short getShort(@Nonnull String fieldName) {
        return getNonNull(fieldName, SHORT, NULLABLE_SHORT, "Short");
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
    public boolean[] getArrayOfBooleans(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_BOOLEANS, ARRAY_OF_NULLABLE_BOOLEANS);
        if (fieldKind == ARRAY_OF_NULLABLE_BOOLEANS) {
            Boolean[] array = (Boolean[]) objects.get(fieldName);
            boolean[] result = new boolean[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] == null) {
                    throw exceptionForUnexpectedNullValueInArray(fieldName, "Booleans");
                }
                result[i] = array[i];
            }
            return result;
        }
        return (boolean[]) objects.get(fieldName);
    }

    @Override
    @Nullable
    public byte[] getArrayOfBytes(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_BYTES, ARRAY_OF_NULLABLE_BYTES);
        if (fieldKind == ARRAY_OF_NULLABLE_BYTES) {
            Byte[] array = (Byte[]) objects.get(fieldName);
            byte[] result = new byte[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] == null) {
                    throw exceptionForUnexpectedNullValueInArray(fieldName, "Bytes");
                }
                result[i] = array[i];
            }
            return result;
        }
        return (byte[]) objects.get(fieldName);
    }

    @Override
    @Nullable
    public char[] getArrayOfChars(@Nonnull String fieldName) {
        throw new UnsupportedOperationException("Compact format does not support reading an array of chars field");
    }

    @Override
    @Nullable
    public double[] getArrayOfDoubles(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_DOUBLES, ARRAY_OF_NULLABLE_DOUBLES);
        if (fieldKind == ARRAY_OF_NULLABLE_DOUBLES) {
            Double[] array = (Double[]) objects.get(fieldName);
            double[] result = new double[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] == null) {
                    throw exceptionForUnexpectedNullValueInArray(fieldName, "Doubles");
                }
                result[i] = array[i];
            }
            return result;
        }
        return (double[]) objects.get(fieldName);
    }

    @Override
    @Nullable
    public float[] getArrayOfFloats(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_FLOATS, ARRAY_OF_NULLABLE_FLOATS);
        if (fieldKind == ARRAY_OF_NULLABLE_FLOATS) {
            Float[] array = (Float[]) objects.get(fieldName);
            float[] result = new float[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] == null) {
                    throw exceptionForUnexpectedNullValueInArray(fieldName, "Floats");
                }
                result[i] = array[i];
            }
            return result;
        }
        return (float[]) objects.get(fieldName);
    }

    @Override
    @Nullable
    public int[] getArrayOfInts(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_INTS, ARRAY_OF_NULLABLE_INTS);
        if (fieldKind == ARRAY_OF_NULLABLE_INTS) {
            Integer[] array = (Integer[]) objects.get(fieldName);
            int[] result = new int[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] == null) {
                    throw exceptionForUnexpectedNullValueInArray(fieldName, "Ints");
                }
                result[i] = array[i];
            }
            return result;
        }
        return (int[]) objects.get(fieldName);
    }

    @Override
    @Nullable
    public long[] getArrayOfLongs(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_LONGS, ARRAY_OF_NULLABLE_LONGS);
        if (fieldKind == ARRAY_OF_NULLABLE_LONGS) {
            Long[] array = (Long[]) objects.get(fieldName);
            long[] result = new long[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] == null) {
                    throw exceptionForUnexpectedNullValueInArray(fieldName, "Longs");
                }
                result[i] = array[i];
            }
            return result;
        }
        return (long[]) objects.get(fieldName);
    }

    @Override
    @Nullable
    public short[] getArrayOfShorts(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_SHORTS, ARRAY_OF_NULLABLE_SHORTS);
        if (fieldKind == ARRAY_OF_NULLABLE_SHORTS) {
            Short[] array = (Short[]) objects.get(fieldName);
            short[] result = new short[array.length];
            for (int i = 0; i < array.length; i++) {
                if (array[i] == null) {
                    throw exceptionForUnexpectedNullValueInArray(fieldName, "Shorts");
                }
                result[i] = array[i];
            }
            return result;
        }
        return (short[]) objects.get(fieldName);
    }

    @Override
    @Nullable
    public String[] getArrayOfStrings(@Nonnull String fieldName) {
        return get(fieldName, ARRAY_OF_STRINGS);
    }

    @Override
    @Nullable
    public BigDecimal[] getArrayOfDecimals(@Nonnull String fieldName) {
        return get(fieldName, ARRAY_OF_DECIMALS);
    }

    @Override
    @Nullable
    public LocalTime[] getArrayOfTimes(@Nonnull String fieldName) {
        return get(fieldName, ARRAY_OF_TIMES);
    }

    @Override
    @Nullable
    public LocalDate[] getArrayOfDates(@Nonnull String fieldName) {
        return get(fieldName, ARRAY_OF_DATES);
    }

    @Override
    @Nullable
    public LocalDateTime[] getArrayOfTimestamps(@Nonnull String fieldName) {
        return get(fieldName, ARRAY_OF_TIMESTAMPS);
    }

    @Override
    @Nullable
    public OffsetDateTime[] getArrayOfTimestampWithTimezones(@Nonnull String fieldName) {
        return get(fieldName, ARRAY_OF_TIMESTAMP_WITH_TIMEZONES);
    }

    @Nullable
    @Override
    public GenericRecord[] getArrayOfGenericRecords(@Nonnull String fieldName) {
        return get(fieldName, ARRAY_OF_COMPACTS);
    }

    @Nullable
    @Override
    public Boolean getNullableBoolean(@Nonnull String fieldName) {
        return get(fieldName, BOOLEAN, NULLABLE_BOOLEAN);
    }

    @Nullable
    @Override
    public Byte getNullableByte(@Nonnull String fieldName) {
        return get(fieldName, BYTE, NULLABLE_BYTE);
    }

    @Nullable
    @Override
    public Double getNullableDouble(@Nonnull String fieldName) {
        return get(fieldName, DOUBLE, NULLABLE_DOUBLE);
    }

    @Nullable
    @Override
    public Float getNullableFloat(@Nonnull String fieldName) {
        return get(fieldName, FLOAT, NULLABLE_FLOAT);
    }

    @Nullable
    @Override
    public Integer getNullableInt(@Nonnull String fieldName) {
        return get(fieldName, INT, NULLABLE_INT);
    }

    @Nullable
    @Override
    public Long getNullableLong(@Nonnull String fieldName) {
        return get(fieldName, LONG, NULLABLE_LONG);
    }

    @Nullable
    @Override
    public Short getNullableShort(@Nonnull String fieldName) {
        return get(fieldName, SHORT, NULLABLE_SHORT);
    }

    @Nullable
    @Override
    public Boolean[] getArrayOfNullableBooleans(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_BOOLEANS, ARRAY_OF_NULLABLE_BOOLEANS);
        if (fieldKind == ARRAY_OF_BOOLEANS) {
            boolean[] array = (boolean[]) objects.get(fieldName);
            Boolean[] result = new Boolean[array.length];
            Arrays.setAll(result, i -> array[i]);
            return result;
        }
        return (Boolean[]) objects.get(fieldName);
    }

    @Nullable
    @Override
    public Byte[] getArrayOfNullableBytes(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_BYTES, ARRAY_OF_NULLABLE_BYTES);
        if (fieldKind == ARRAY_OF_BYTES) {
            byte[] array = (byte[]) objects.get(fieldName);
            Byte[] result = new Byte[array.length];
            Arrays.setAll(result, i -> array[i]);
            return result;
        }
        return (Byte[]) objects.get(fieldName);
    }

    @Nullable
    @Override
    public Double[] getArrayOfNullableDoubles(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_DOUBLES, ARRAY_OF_NULLABLE_DOUBLES);
        if (fieldKind == ARRAY_OF_DOUBLES) {
            double[] array = (double[]) objects.get(fieldName);
            Double[] result = new Double[array.length];
            Arrays.setAll(result, i -> array[i]);
            return result;
        }
        return (Double[]) objects.get(fieldName);
    }

    @Nullable
    @Override
    public Float[] getArrayOfNullableFloats(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_FLOATS, ARRAY_OF_NULLABLE_FLOATS);
        if (fieldKind == ARRAY_OF_FLOATS) {
            float[] array = (float[]) objects.get(fieldName);
            Float[] result = new Float[array.length];
            Arrays.setAll(result, i -> array[i]);
            return result;
        }
        return (Float[]) objects.get(fieldName);
    }

    @Nullable
    @Override
    public Integer[] getArrayOfNullableInts(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_INTS, ARRAY_OF_NULLABLE_INTS);
        if (fieldKind == ARRAY_OF_INTS) {
            int[] array = (int[]) objects.get(fieldName);
            Integer[] result = new Integer[array.length];
            Arrays.setAll(result, i -> array[i]);
            return result;
        }
        return (Integer[]) objects.get(fieldName);
    }

    @Nullable
    @Override
    public Long[] getArrayOfNullableLongs(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_LONGS, ARRAY_OF_NULLABLE_LONGS);
        if (fieldKind == ARRAY_OF_LONGS) {
            long[] array = (long[]) objects.get(fieldName);
            Long[] result = new Long[array.length];
            Arrays.setAll(result, i -> array[i]);
            return result;
        }
        return (Long[]) objects.get(fieldName);
    }

    @Nullable
    @Override
    public Short[] getArrayOfNullableShorts(@Nonnull String fieldName) {
        FieldKind fieldKind = check(fieldName, ARRAY_OF_SHORTS, ARRAY_OF_NULLABLE_SHORTS);
        if (fieldKind == ARRAY_OF_SHORTS) {
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

}
