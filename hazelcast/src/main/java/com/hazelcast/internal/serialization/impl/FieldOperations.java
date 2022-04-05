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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.json.JsonEscape;
import com.hazelcast.internal.serialization.impl.compact.DefaultCompactWriter;
import com.hazelcast.nio.serialization.AbstractGenericRecord;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.GenericRecord;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.function.BiConsumer;

/**
 * Should always be consistent with {@link FieldKind}
 */
@SuppressWarnings({"checkstyle:AnonInnerLength", "checkstyle:ExecutableStatementCount"})
public final class FieldOperations {

    private static final FieldKindBasedOperations[] ALL = new FieldKindBasedOperations[FieldKind.values().length];

    static {
        ALL[FieldKind.BOOLEAN.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getBoolean(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeBoolean(fieldName, genericRecord.getBoolean(fieldName));
            }

            @Override
            public int kindSizeInBytes() {
                //Boolean is actually 1 bit. To make it look like smaller than Byte we use 0.
                return 0;
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(record.getBoolean(fieldName));
            }
        };
        ALL[FieldKind.ARRAY_OF_BOOLEAN.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfBoolean(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getBooleanFromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfBoolean(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfBoolean(fieldName, record.getArrayOfBoolean(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(Arrays.toString(record.getArrayOfBoolean(fieldName)));
            }
        };
        ALL[FieldKind.INT8.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getInt8(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeInt8(fieldName, genericRecord.getInt8(fieldName));
            }

            @Override
            public int kindSizeInBytes() {
                return Byte.BYTES;
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(record.getInt8(fieldName));
            }
        };
        ALL[FieldKind.ARRAY_OF_INT8.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfInt8(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getInt8FromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfInt8(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfInt8(fieldName, record.getArrayOfInt8(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(Arrays.toString(record.getArrayOfInt8(fieldName)));
            }
        };
        ALL[FieldKind.CHAR.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getChar(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                throw new UnsupportedOperationException("Compact format does not support writing a char field");
            }

            @Override
            public int kindSizeInBytes() {
                return Character.BYTES;
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                JsonEscape.writeEscaped(stringBuilder, record.getChar(fieldName));
            }
        };
        ALL[FieldKind.ARRAY_OF_CHAR.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfChar(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getCharFromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfChar(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                throw new UnsupportedOperationException("Compact format does not support writing an array of chars field");
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                char[] objects = record.getArrayOfChar(fieldName);
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

        };
        ALL[FieldKind.INT16.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getInt16(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeInt16(fieldName, genericRecord.getInt16(fieldName));
            }

            @Override
            public int kindSizeInBytes() {
                return Short.BYTES;
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(record.getInt16(fieldName));
            }
        };
        ALL[FieldKind.ARRAY_OF_INT16.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfInt16(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getInt16FromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfInt16(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfInt16(fieldName, record.getArrayOfInt16(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(Arrays.toString(record.getArrayOfInt16(fieldName)));
            }
        };
        ALL[FieldKind.INT32.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getInt32(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeInt32(fieldName, genericRecord.getInt32(fieldName));
            }

            @Override
            public int kindSizeInBytes() {
                return Integer.BYTES;
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(record.getInt32(fieldName));
            }
        };
        ALL[FieldKind.ARRAY_OF_INT32.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfInt32(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getInt32FromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfInt32(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfInt32(fieldName, record.getArrayOfInt32(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(Arrays.toString(record.getArrayOfInt32(fieldName)));
            }
        };
        ALL[FieldKind.INT64.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getInt64(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeInt64(fieldName, genericRecord.getInt64(fieldName));
            }

            @Override
            public int kindSizeInBytes() {
                return Long.BYTES;
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(record.getInt64(fieldName));
            }
        };
        ALL[FieldKind.ARRAY_OF_INT64.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfInt64(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getInt64FromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfInt64(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfInt64(fieldName, record.getArrayOfInt64(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(Arrays.toString(record.getArrayOfInt64(fieldName)));
            }
        };
        ALL[FieldKind.FLOAT32.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getFloat32(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeFloat32(fieldName, genericRecord.getFloat32(fieldName));
            }

            @Override
            public int kindSizeInBytes() {
                return Float.BYTES;
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(record.getFloat32(fieldName));
            }
        };
        ALL[FieldKind.ARRAY_OF_FLOAT32.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfFloat32(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getFloat32FromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfFloat32(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfFloat32(fieldName, record.getArrayOfFloat32(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(Arrays.toString(record.getArrayOfFloat32(fieldName)));
            }
        };
        ALL[FieldKind.FLOAT64.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getFloat64(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeFloat64(fieldName, genericRecord.getFloat64(fieldName));
            }

            @Override
            public int kindSizeInBytes() {
                return Double.BYTES;
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(record.getFloat64(fieldName));
            }
        };
        ALL[FieldKind.ARRAY_OF_FLOAT64.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfFloat64(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getFloat64FromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfFloat64(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfFloat64(fieldName, record.getArrayOfFloat64(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(Arrays.toString(record.getArrayOfFloat64(fieldName)));
            }
        };
        ALL[FieldKind.STRING.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
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
        };
        ALL[FieldKind.ARRAY_OF_STRING.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfString(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getStringFromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfString(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfString(fieldName, record.getArrayOfString(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                String[] objects = record.getArrayOfString(fieldName);
                writeArrayJsonFormatted(stringBuilder, objects, JsonEscape::writeEscaped);
            }
        };
        ALL[FieldKind.DECIMAL.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
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
        };
        ALL[FieldKind.ARRAY_OF_DECIMAL.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfDecimal(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getDecimalFromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfDecimal(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfDecimal(fieldName, record.getArrayOfDecimal(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                BigDecimal[] objects = record.getArrayOfDecimal(fieldName);
                writeArrayJsonFormatted(stringBuilder, objects, StringBuilder::append);
            }
        };
        ALL[FieldKind.TIME.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getTime(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeTime(fieldName, genericRecord.getTime(fieldName));
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
        };
        ALL[FieldKind.ARRAY_OF_TIME.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfTime(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getTimeFromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfTime(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfTime(fieldName, record.getArrayOfTime(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                Object[] objects = record.getArrayOfTime(fieldName);
                writeArrayJsonFormatted(stringBuilder, objects,
                        (builder, o) -> builder.append("\"").append(o).append("\""));
            }
        };
        ALL[FieldKind.DATE.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getDate(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeDate(fieldName, genericRecord.getDate(fieldName));
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
        };
        ALL[FieldKind.ARRAY_OF_DATE.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfDate(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getDateFromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfDate(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfDate(fieldName, record.getArrayOfDate(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                Object[] objects = record.getArrayOfDate(fieldName);
                writeArrayJsonFormatted(stringBuilder, objects,
                        (builder, o) -> builder.append("\"").append(o).append("\""));

            }
        };
        ALL[FieldKind.TIMESTAMP.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getTimestamp(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeTimestamp(fieldName, genericRecord.getTimestamp(fieldName));
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
        };
        ALL[FieldKind.ARRAY_OF_TIMESTAMP.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfTimestamp(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getTimestampFromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfTimestamp(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfTimestamp(fieldName, record.getArrayOfTimestamp(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                Object[] objects = record.getArrayOfTimestamp(fieldName);
                writeArrayJsonFormatted(stringBuilder, objects,
                        (builder, o) -> builder.append("\"").append(o).append("\""));
            }
        };
        ALL[FieldKind.TIMESTAMP_WITH_TIMEZONE.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getTimestampWithTimezone(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeTimestampWithTimezone(fieldName, genericRecord.getTimestampWithTimezone(fieldName));
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
        };
        ALL[FieldKind.ARRAY_OF_TIMESTAMP_WITH_TIMEZONE.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfTimestampWithTimezone(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getTimestampWithTimezoneFromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfTimestampWithTimezone(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfTimestampWithTimezone(fieldName, record.getArrayOfTimestampWithTimezone(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                Object[] objects = record.getArrayOfTimestampWithTimezone(fieldName);
                writeArrayJsonFormatted(stringBuilder, objects,
                        (builder, o) -> builder.append("\"").append(o).append("\""));
            }
        };
        ALL[FieldKind.COMPACT.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readAsLeafObjectOnQuery(InternalGenericRecord genericRecord, String fieldName) {
                return genericRecord.getObject(fieldName);
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
        };
        ALL[FieldKind.ARRAY_OF_COMPACT.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readAsLeafObjectOnQuery(InternalGenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfObject(fieldName, Object.class);
            }

            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfGenericRecord(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getObjectFromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfGenericRecord(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfGenericRecord(fieldName, record.getArrayOfGenericRecord(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                GenericRecord[] objects = record.getArrayOfGenericRecord(fieldName);
                writeArrayJsonFormatted(stringBuilder, objects, StringBuilder::append);
            }
        };
        ALL[FieldKind.PORTABLE.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readAsLeafObjectOnQuery(InternalGenericRecord genericRecord, String fieldName) {
                return genericRecord.getObject(fieldName);
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
        };
        ALL[FieldKind.ARRAY_OF_PORTABLE.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readAsLeafObjectOnQuery(InternalGenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfObject(fieldName, Object.class);
            }

            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfGenericRecord(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getObjectFromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfGenericRecord(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfGenericRecord(fieldName, record.getArrayOfGenericRecord(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                GenericRecord[] objects = record.getArrayOfGenericRecord(fieldName);
                stringBuilder.append(Arrays.toString(objects));
            }
        };
        ALL[FieldKind.NULLABLE_BOOLEAN.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getNullableBoolean(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeNullableBoolean(fieldName, genericRecord.getNullableBoolean(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(record.getNullableBoolean(fieldName));
            }
        };
        ALL[FieldKind.ARRAY_OF_NULLABLE_BOOLEAN.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfNullableBoolean(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getNullableBooleanFromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfNullableBoolean(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfNullableBoolean(fieldName, record.getArrayOfNullableBoolean(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                writeArrayJsonFormatted(stringBuilder, record.readAny(fieldName), StringBuilder::append);
            }

        };
        ALL[FieldKind.NULLABLE_INT8.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getNullableInt8(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeNullableInt8(fieldName, genericRecord.getNullableInt8(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(record.getNullableInt8(fieldName));
            }
        };
        ALL[FieldKind.ARRAY_OF_NULLABLE_INT8.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfNullableInt8(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getNullableInt8FromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfNullableInt8(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfNullableInt8(fieldName, record.getArrayOfNullableInt8(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                writeArrayJsonFormatted(stringBuilder, record.readAny(fieldName), StringBuilder::append);
            }
        };
        ALL[FieldKind.NULLABLE_INT16.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getNullableInt16(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeNullableInt16(fieldName, genericRecord.getNullableInt16(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(record.getNullableInt16(fieldName));
            }
        };
        ALL[FieldKind.ARRAY_OF_NULLABLE_INT16.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfNullableInt16(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getNullableInt16FromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfNullableInt16(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfNullableInt16(fieldName, record.getArrayOfNullableInt16(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                writeArrayJsonFormatted(stringBuilder, record.readAny(fieldName), StringBuilder::append);
            }
        };
        ALL[FieldKind.NULLABLE_INT32.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getNullableInt32(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeNullableInt32(fieldName, genericRecord.getNullableInt32(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(record.getNullableInt32(fieldName));
            }
        };
        ALL[FieldKind.ARRAY_OF_NULLABLE_INT32.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfNullableInt32(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getNullableInt32FromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfNullableInt32(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfNullableInt32(fieldName, record.getArrayOfNullableInt32(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                writeArrayJsonFormatted(stringBuilder, record.readAny(fieldName), StringBuilder::append);
            }
        };
        ALL[FieldKind.NULLABLE_INT64.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getNullableInt64(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeNullableInt64(fieldName, genericRecord.getNullableInt64(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(record.getNullableInt64(fieldName));
            }
        };
        ALL[FieldKind.ARRAY_OF_NULLABLE_INT64.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfNullableInt64(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getNullableInt64FromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfNullableInt64(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfNullableInt64(fieldName, record.getArrayOfNullableInt64(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                writeArrayJsonFormatted(stringBuilder, record.readAny(fieldName), StringBuilder::append);
            }
        };
        ALL[FieldKind.NULLABLE_FLOAT32.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getNullableFloat32(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeNullableFloat32(fieldName, genericRecord.getNullableFloat32(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(record.getNullableFloat32(fieldName));
            }
        };
        ALL[FieldKind.ARRAY_OF_NULLABLE_FLOAT32.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfNullableFloat32(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getNullableFloat32FromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfNullableFloat32(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfNullableFloat32(fieldName, record.getArrayOfNullableFloat32(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                writeArrayJsonFormatted(stringBuilder, record.readAny(fieldName), StringBuilder::append);
            }
        };
        ALL[FieldKind.NULLABLE_FLOAT64.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getNullableFloat64(fieldName);
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
                writer.writeNullableFloat64(fieldName, genericRecord.getNullableFloat64(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                stringBuilder.append(record.getNullableFloat64(fieldName));
            }
        };
        ALL[FieldKind.ARRAY_OF_NULLABLE_FLOAT64.getId()] = new FieldKindBasedOperations() {
            @Override
            public Object readGenericRecordOrPrimitive(GenericRecord genericRecord, String fieldName) {
                return genericRecord.getArrayOfNullableFloat64(fieldName);
            }

            @Override
            public Object readIndexed(InternalGenericRecord record, String fieldName, int index) {
                return record.getNullableFloat64FromArray(fieldName, index);
            }

            @Override
            public int hashCode(GenericRecord record, String fieldName) {
                return Arrays.hashCode(record.getArrayOfNullableFloat64(fieldName));
            }

            @Override
            public void writeFieldFromRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
                writer.writeArrayOfNullableFloat64(fieldName, record.getArrayOfNullableFloat64(fieldName));
            }

            @Override
            public void writeJsonFormattedField(StringBuilder stringBuilder, AbstractGenericRecord record, String fieldName) {
                writeArrayJsonFormatted(stringBuilder, record.readAny(fieldName), StringBuilder::append);
            }
        };
    }

    private FieldOperations() {

    }

    public static FieldKindBasedOperations fieldOperations(FieldKind fieldKind) {
        return ALL[fieldKind.getId()];
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

    public static boolean isArrayKind(FieldKind fieldKind) {
        return fieldKind.getId() % 2 != 0;
    }

    public static FieldKind getSingleKind(FieldKind fieldKind) {
        assert isArrayKind(fieldKind);
        return FieldKind.values()[fieldKind.getId() - 1];
    }
}
