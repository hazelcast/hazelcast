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

import com.hazelcast.internal.serialization.impl.compact.DefaultCompactWriter;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.Portable;

import java.util.Arrays;

/**
 * Should always be consistent with {@link com.hazelcast.nio.serialization.FieldType}
 */
public enum FieldOperations {

    PORTABLE(new SingleFieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return ((InternalGenericRecord) genericRecord).getObject(fieldName);
        }

        @Override
        public GenericRecord readInSerializedForm(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getGenericRecord(fieldName);
        }

        @Override
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeGenericRecord(fieldName, genericRecord.getGenericRecord(fieldName));
        }
    }),
    BYTE(new SingleFieldTypeBasedOperations() {
        @Override
        public Byte readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getByte(fieldName);
        }

        @Override
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeByte(fieldName, genericRecord.getByte(fieldName));
        }
    }),
    BOOLEAN(new SingleFieldTypeBasedOperations() {
        @Override
        public Boolean readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getBoolean(fieldName);
        }

        @Override
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeBoolean(fieldName, genericRecord.getBoolean(fieldName));
        }
    }),
    CHAR(new SingleFieldTypeBasedOperations() {
        @Override
        public Character readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getChar(fieldName);
        }

        @Override
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeChar(fieldName, genericRecord.getChar(fieldName));
        }
    }),
    SHORT(new SingleFieldTypeBasedOperations() {
        @Override
        public Short readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getShort(fieldName);
        }

        @Override
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeShort(fieldName, genericRecord.getShort(fieldName));
        }
    }),
    INT(new SingleFieldTypeBasedOperations() {
        @Override
        public Integer readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getInt(fieldName);
        }

        @Override
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeInt(fieldName, genericRecord.getInt(fieldName));
        }
    }),
    LONG(new SingleFieldTypeBasedOperations() {
        @Override
        public Long readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getLong(fieldName);
        }

        @Override
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeLong(fieldName, genericRecord.getLong(fieldName));
        }
    }),
    FLOAT(new SingleFieldTypeBasedOperations() {
        @Override
        public Float readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getFloat(fieldName);
        }

        @Override
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeFloat(fieldName, genericRecord.getFloat(fieldName));
        }
    }),
    DOUBLE(new SingleFieldTypeBasedOperations() {
        @Override
        public Double readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getDouble(fieldName);
        }

        @Override
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeDouble(fieldName, genericRecord.getDouble(fieldName));
        }
    }),
    UTF(new SingleFieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getString(fieldName);
        }

        @Override
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeString(fieldName, genericRecord.getString(fieldName));
        }
    }),

    PORTABLE_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return ((InternalGenericRecord) genericRecord).getObjectArray(fieldName, Portable.class);
        }

        @Override
        public Object readInSerializedForm(GenericRecord genericRecord, String fieldName) {
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
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeGenericRecordArray(fieldName, record.getGenericRecordArray(fieldName));
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
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeByteArray(fieldName, record.getByteArray(fieldName));
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
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeBooleanArray(fieldName, record.getBooleanArray(fieldName));
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
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeCharArray(fieldName, record.getCharArray(fieldName));
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
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeShortArray(fieldName, record.getShortArray(fieldName));
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
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeIntArray(fieldName, record.getIntArray(fieldName));
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
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeLongArray(fieldName, record.getLongArray(fieldName));
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
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeFloatArray(fieldName, record.getFloatArray(fieldName));
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
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeDoubleArray(fieldName, record.getDoubleArray(fieldName));
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
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeStringArray(fieldName, record.getStringArray(fieldName));
        }
    }),
    DECIMAL(new SingleFieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getDecimal(fieldName);
        }

        @Override
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeDecimal(fieldName, genericRecord.getDecimal(fieldName));
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
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeDecimalArray(fieldName, record.getDecimalArray(fieldName));
        }
    }),
    LOCAL_TIME(new SingleFieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getTime(fieldName);
        }

        @Override
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeTime(fieldName, genericRecord.getTime(fieldName));
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
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeTimeArray(fieldName, record.getTimeArray(fieldName));
        }
    }),
    LOCAL_DATE(new SingleFieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getDate(fieldName);
        }

        @Override
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeDate(fieldName, genericRecord.getDate(fieldName));
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
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeDateArray(fieldName, record.getDateArray(fieldName));
        }
    }),
    LOCAL_DATE_TIME(new SingleFieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getTimestamp(fieldName);
        }

        @Override
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeTimestamp(fieldName, genericRecord.getTimestamp(fieldName));
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
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeTimestampArray(fieldName, record.getTimestampArray(fieldName));
        }
    }),
    OFFSET_DATE_TIME(new SingleFieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getTimestampWithTimezone(fieldName);
        }

        @Override
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeTimestampWithTimezone(fieldName, genericRecord.getTimestampWithTimezone(fieldName));
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
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeTimestampWithTimezoneArray(fieldName, record.getTimestampWithTimezoneArray(fieldName));
        }
    }),
    COMPOSED(new SingleFieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return ((InternalGenericRecord) genericRecord).getObject(fieldName);
        }

        @Override
        public GenericRecord readInSerializedForm(GenericRecord genericRecord, String fieldName) {
            return genericRecord.getGenericRecord(fieldName);
        }

        @Override
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord genericRecord, String fieldName) {
            writer.writeGenericRecord(fieldName, genericRecord.getGenericRecord(fieldName));
        }
    }),
    COMPOSED_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(GenericRecord genericRecord, String fieldName) {
            return ((InternalGenericRecord) genericRecord).getObjectArray(fieldName, Object.class);
        }

        @Override
        public Object readInSerializedForm(GenericRecord genericRecord, String fieldName) {
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
        public void readFromGenericRecordToWriter(DefaultCompactWriter writer, GenericRecord record, String fieldName) {
            writer.writeGenericRecordArray(fieldName, record.getGenericRecordArray(fieldName));
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
}
