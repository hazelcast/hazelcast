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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.json.JsonEscape;
import com.hazelcast.internal.serialization.impl.compact.DefaultCompactWriter;
import com.hazelcast.nio.serialization.AbstractGenericRecord;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.function.BiConsumer;

/**
 * Should always be consistent with {@link com.hazelcast.nio.serialization.FieldType}
 */
@SuppressWarnings("checkstyle:AnonInnerLength")
public enum FieldOperations {

    PORTABLE(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return ((InternalGenericRecord) genericRecord).getObject(fieldName);
        }

        @Override
        public GenericRecord readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getGenericRecord(fieldName);
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeGenericRecord(fieldName, genericRecord.getGenericRecord(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            stringBuilder.append(record.getGenericRecord(fieldName));
        }
    }),
    BYTE(new FieldTypeBasedOperations() {
        @Override
        public Byte readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getByte(fieldName);
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeByte(fieldName, genericRecord.getByte(fieldName));
        }

        @Override
        public int typeSizeInBytes() {
            return Byte.BYTES;
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            stringBuilder.append(record.getByte(fieldName));
        }
    }),
    BOOLEAN(new FieldTypeBasedOperations() {
        @Override
        public Boolean readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getBoolean(fieldName);
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeBoolean(fieldName, genericRecord.getBoolean(fieldName));
        }

        @Override
        public int typeSizeInBytes() {
            //Boolean is actually 1 bit. To make it look like smaller than Byte we use 0.
            return 0;
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            stringBuilder.append(record.getBoolean(fieldName));
        }
    }),
    CHAR(new FieldTypeBasedOperations() {
        @Override
        public Character readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getChar(fieldName);
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeChar(fieldName, genericRecord.getChar(fieldName));
        }

        @Override
        public int typeSizeInBytes() {
            return Character.BYTES;
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            JsonEscape.writeEscaped(stringBuilder, record.getChar(fieldName));
        }
    }),
    SHORT(new FieldTypeBasedOperations() {
        @Override
        public Short readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getShort(fieldName);
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeShort(fieldName, genericRecord.getShort(fieldName));
        }

        @Override
        public int typeSizeInBytes() {
            return Short.BYTES;
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            stringBuilder.append(record.getShort(fieldName));
        }
    }),
    INT(new FieldTypeBasedOperations() {
        @Override
        public Integer readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getInt(fieldName);
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeInt(fieldName, genericRecord.getInt(fieldName));
        }

        @Override
        public int typeSizeInBytes() {
            return Integer.BYTES;
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            stringBuilder.append(record.getInt(fieldName));
        }
    }),
    LONG(new FieldTypeBasedOperations() {
        @Override
        public Long readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getLong(fieldName);
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeLong(fieldName, genericRecord.getLong(fieldName));
        }

        @Override
        public int typeSizeInBytes() {
            return Long.BYTES;
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            stringBuilder.append(record.getLong(fieldName));
        }
    }),
    FLOAT(new FieldTypeBasedOperations() {
        @Override
        public Float readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getFloat(fieldName);
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeFloat(fieldName, genericRecord.getFloat(fieldName));
        }

        @Override
        public int typeSizeInBytes() {
            return Float.BYTES;
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            stringBuilder.append(record.getFloat(fieldName));
        }
    }),
    DOUBLE(new FieldTypeBasedOperations() {
        @Override
        public Double readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getDouble(fieldName);
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeDouble(fieldName, genericRecord.getDouble(fieldName));
        }

        @Override
        public int typeSizeInBytes() {
            return Double.BYTES;
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            stringBuilder.append(record.getDouble(fieldName));
        }
    }),
    UTF(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getString(fieldName);
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeString(fieldName, genericRecord.getString(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            String value = record.getString(fieldName);
            if (value == null) {
                stringBuilder.append("null");
                return;
            }
            JsonEscape.writeEscaped(stringBuilder, value);
        }
    }),

    PORTABLE_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return ((InternalGenericRecord) genericRecord).getObjectArray(fieldName, Object.class);
        }

        @Override
        public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getGenericRecordArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
            return record.getObjectFromArray(fieldName, index);
        }

        @Override
        public int hashCode(GenericRecord record, String fieldName) {
            return Arrays.hashCode(record.getGenericRecordArray(fieldName));
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeGenericRecordArray(fieldName, record.getGenericRecordArray(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            GenericRecord[] objects = record.getGenericRecordArray(fieldName);
            stringBuilder.append(Arrays.toString(objects));
        }
    }),
    BYTE_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getByteArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
            return record.getByteFromArray(fieldName, index);
        }

        @Override
        public int hashCode(GenericRecord record, String fieldName) {
            return Arrays.hashCode(record.getByteArray(fieldName));
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeByteArray(fieldName, record.getByteArray(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            stringBuilder.append(Arrays.toString(record.getByteArray(fieldName)));
        }
    }),
    BOOLEAN_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getBooleanArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
            return record.getBooleanFromArray(fieldName, index);
        }

        @Override
        public int hashCode(GenericRecord record, String fieldName) {
            return Arrays.hashCode(record.getBooleanArray(fieldName));
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeBooleanArray(fieldName, record.getBooleanArray(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            stringBuilder.append(Arrays.toString(record.getBooleanArray(fieldName)));
        }
    }),
    CHAR_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getCharArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
            return record.getCharFromArray(fieldName, index);
        }

        @Override
        public int hashCode(GenericRecord record, String fieldName) {
            return Arrays.hashCode(record.getCharArray(fieldName));
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeCharArray(fieldName, record.getCharArray(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            char[] objects = record.getCharArray(fieldName);
            if (objects == null) {
                stringBuilder.append("null");
                return;
            }
            stringBuilder.append("[");
            int objectsSize = objects.length;
            int j = 0;
            for (char object : objects) {
                j++;
                JsonEscape.writeEscaped(stringBuilder, object);
                if (j != objectsSize) {
                    stringBuilder.append(", ");
                }
            }
            stringBuilder.append("]");
        }

    }),
    SHORT_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getShortArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
            return record.getShortFromArray(fieldName, index);
        }

        @Override
        public int hashCode(GenericRecord record, String fieldName) {
            return Arrays.hashCode(record.getShortArray(fieldName));
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeShortArray(fieldName, record.getShortArray(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            stringBuilder.append(Arrays.toString(record.getShortArray(fieldName)));
        }
    }),
    INT_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getIntArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
            return record.getIntFromArray(fieldName, index);
        }

        @Override
        public int hashCode(GenericRecord record, String fieldName) {
            return Arrays.hashCode(record.getIntArray(fieldName));
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeIntArray(fieldName, record.getIntArray(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            stringBuilder.append(Arrays.toString(record.getIntArray(fieldName)));
        }
    }),
    LONG_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getLongArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
            return record.getLongFromArray(fieldName, index);
        }

        @Override
        public int hashCode(GenericRecord record, String fieldName) {
            return Arrays.hashCode(record.getLongArray(fieldName));
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeLongArray(fieldName, record.getLongArray(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            stringBuilder.append(Arrays.toString(record.getLongArray(fieldName)));
        }
    }),
    FLOAT_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getFloatArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
            return record.getFloatFromArray(fieldName, index);
        }

        @Override
        public int hashCode(GenericRecord record, String fieldName) {
            return Arrays.hashCode(record.getFloatArray(fieldName));
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeFloatArray(fieldName, record.getFloatArray(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            stringBuilder.append(Arrays.toString(record.getFloatArray(fieldName)));
        }
    }),
    DOUBLE_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getDoubleArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
            return record.getDoubleFromArray(fieldName, index);
        }

        @Override
        public int hashCode(GenericRecord record, String fieldName) {
            return Arrays.hashCode(record.getDoubleArray(fieldName));
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeDoubleArray(fieldName, record.getDoubleArray(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            stringBuilder.append(Arrays.toString(record.getDoubleArray(fieldName)));
        }
    }),
    UTF_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getStringArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
            return record.getStringFromArray(fieldName, index);
        }

        @Override
        public int hashCode(GenericRecord record, String fieldName) {
            return Arrays.hashCode(record.getStringArray(fieldName));
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeStringArray(fieldName, record.getStringArray(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            String[] objects = record.getStringArray(fieldName);
            writeArrayJsonFormatted(stringBuilder, objects, JsonEscape::writeEscaped);
        }
    }),
    DECIMAL(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getDecimal(fieldName);
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeDecimal(fieldName, genericRecord.getDecimal(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            stringBuilder.append(record.getDecimal(fieldName));
        }
    }),
    DECIMAL_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getDecimalArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
            return record.getDecimalFromArray(fieldName, index);
        }

        @Override
        public int hashCode(GenericRecord record, String fieldName) {
            return Arrays.hashCode(record.getDecimalArray(fieldName));
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeDecimalArray(fieldName, record.getDecimalArray(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            BigDecimal[] objects = record.getDecimalArray(fieldName);
            writeArrayJsonFormatted(stringBuilder, objects, StringBuilder::append);
        }
    }),
    LOCAL_TIME(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getTime(fieldName);
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeTime(fieldName, genericRecord.getTime(fieldName));
        }

        @Override
        public int typeSizeInBytes() {
            return Byte.BYTES * 3 + Integer.BYTES;
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            LocalTime value = record.getTime(fieldName);
            if (value == null) {
                stringBuilder.append("null");
                return;
            }
            stringBuilder.append('"').append(value).append('"');
        }
    }),
    LOCAL_TIME_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getTimeArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
            return record.getTimeFromArray(fieldName, index);
        }

        @Override
        public int hashCode(GenericRecord record, String fieldName) {
            return Arrays.hashCode(record.getTimeArray(fieldName));
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeTimeArray(fieldName, record.getTimeArray(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            Object[] objects = record.getTimeArray(fieldName);
            writeArrayJsonFormatted(stringBuilder, objects,
                    (builder, o) -> builder.append("\"").append(o).append("\""));
        }
    }),
    LOCAL_DATE(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getDate(fieldName);
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeDate(fieldName, genericRecord.getDate(fieldName));
        }

        @Override
        public int typeSizeInBytes() {
            return Integer.BYTES + Byte.BYTES * 2;
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            LocalDate value = record.getDate(fieldName);
            if (value == null) {
                stringBuilder.append("null");
                return;
            }
            stringBuilder.append('"').append(value).append('"');
        }
    }),
    LOCAL_DATE_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getDateArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
            return record.getDateFromArray(fieldName, index);
        }

        @Override
        public int hashCode(GenericRecord record, String fieldName) {
            return Arrays.hashCode(record.getDateArray(fieldName));
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeDateArray(fieldName, record.getDateArray(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            Object[] objects = record.getDateArray(fieldName);
            writeArrayJsonFormatted(stringBuilder, objects,
                    (builder, o) -> builder.append("\"").append(o).append("\""));

        }
    }),
    LOCAL_DATE_TIME(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getTimestamp(fieldName);
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeTimestamp(fieldName, genericRecord.getTimestamp(fieldName));
        }

        @Override
        public int typeSizeInBytes() {
            return LOCAL_DATE.operations.typeSizeInBytes() + LOCAL_TIME.operations.typeSizeInBytes();
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            LocalDateTime value = record.getTimestamp(fieldName);
            if (value == null) {
                stringBuilder.append("null");
                return;
            }
            stringBuilder.append('"').append(value).append('"');
        }
    }),
    LOCAL_DATE_TIME_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getTimestampArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
            return record.getTimestampFromArray(fieldName, index);
        }

        @Override
        public int hashCode(GenericRecord record, String fieldName) {
            return Arrays.hashCode(record.getTimestampArray(fieldName));
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeTimestampArray(fieldName, record.getTimestampArray(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            Object[] objects = record.getTimestampArray(fieldName);
            writeArrayJsonFormatted(stringBuilder, objects,
                    (builder, o) -> builder.append("\"").append(o).append("\""));
        }
    }),
    OFFSET_DATE_TIME(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getTimestampWithTimezone(fieldName);
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeTimestampWithTimezone(fieldName, genericRecord.getTimestampWithTimezone(fieldName));
        }

        @Override
        public int typeSizeInBytes() {
            return LOCAL_DATE_TIME.operations.typeSizeInBytes() + Integer.BYTES;
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            OffsetDateTime value = record.getTimestampWithTimezone(fieldName);
            if (value == null) {
                stringBuilder.append("null");
                return;
            }
            stringBuilder.append('"').append(value).append('"');
        }
    }),
    OFFSET_DATE_TIME_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getTimestampWithTimezoneArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
            return record.getTimestampWithTimezoneFromArray(fieldName, index);
        }

        @Override
        public int hashCode(GenericRecord record, String fieldName) {
            return Arrays.hashCode(record.getTimestampWithTimezoneArray(fieldName));
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeTimestampWithTimezoneArray(fieldName, record.getTimestampWithTimezoneArray(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            Object[] objects = record.getTimestampWithTimezoneArray(fieldName);
            writeArrayJsonFormatted(stringBuilder, objects,
                    (builder, o) -> builder.append("\"").append(o).append("\""));
        }
    }),
    COMPOSED(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return ((InternalGenericRecord) genericRecord).getObject(fieldName);
        }

        @Override
        public GenericRecord readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getGenericRecord(fieldName);
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeGenericRecord(fieldName, genericRecord.getGenericRecord(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            stringBuilder.append(record.getGenericRecord(fieldName));
        }
    }),
    COMPOSED_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return ((InternalGenericRecord) genericRecord).getObjectArray(fieldName, Object.class);
        }

        @Override
        public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getGenericRecordArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
            return record.getObjectFromArray(fieldName, index);
        }

        @Override
        public int hashCode(GenericRecord record, String fieldName) {
            return Arrays.hashCode(record.getGenericRecordArray(fieldName));
        }

        @Override
        public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeGenericRecordArray(fieldName, record.getGenericRecordArray(fieldName));
        }

        @Override
        public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
            GenericRecord[] objects = record.getGenericRecordArray(fieldName);
            writeArrayJsonFormatted(stringBuilder, objects, StringBuilder::append);
        }
    });

    private static final FieldOperations[] ALL = FieldOperations.values();

    private final FieldTypeBasedOperations operations;

    FieldOperations(FieldTypeBasedOperations operations) {
        this.operations = operations;
    }

    public static FieldTypeBasedOperations fieldOperations(FieldType fieldType) {
        FieldOperations fieldOperations = ALL[fieldType.getId()];
        return fieldOperations.operations;
    }

    /**
     * Writes arrays as json formatted.
     * Array itself and its items are allowed to be null.
     */
    private static <T> void writeArrayJsonFormatted(StringBuilder stringBuilder, T[] objects,
                                                    BiConsumer<StringBuilder, T> itemWriter) {
        if (objects == null) {
            stringBuilder.append("null");
            return;
        }
        stringBuilder.append("[");
        int objectSize = objects.length;
        int j = 0;
        for (T object : objects) {
            j++;
            if (object == null) {
                stringBuilder.append("null");
            } else {
                itemWriter.accept(stringBuilder, object);
            }
            if (j != objectSize) {
                stringBuilder.append(", ");
            }
        }
        stringBuilder.append("]");
    }
}
