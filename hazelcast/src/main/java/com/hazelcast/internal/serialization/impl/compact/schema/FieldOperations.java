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

package com.hazelcast.internal.serialization.impl.compact.schema;

import com.hazelcast.internal.serialization.impl.compact.DefaultCompactWriter;
import com.hazelcast.internal.serialization.impl.compact.InternalCompactRecord;
import com.hazelcast.nio.serialization.compact.CompactRecord;
import com.hazelcast.nio.serialization.compact.TypeID;

import java.util.Arrays;

/**
 * Should always be consistent with {@link com.hazelcast.nio.serialization.compact.TypeID}
 */
public enum FieldOperations {


    COMPOSED(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return ((InternalCompactRecord) compactRecord).getObject(fieldName);
        }

        @Override
        public CompactRecord readInSerializedForm(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getCompactRecord(fieldName);
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord compactRecord, String fieldName) {
            writer.writeCompactRecord(fieldName, compactRecord.getCompactRecord(fieldName));
        }
    }),
    COMPOSED_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return ((InternalCompactRecord) compactRecord).getObjectArray(fieldName, Object.class);
        }

        @Override
        public Object readInSerializedForm(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getCompactRecordArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalCompactRecord record, String fieldName, int index) {
            return record.getObjectFromArray(fieldName, index);
        }

        @Override
        public int hashCode(CompactRecord record, String fieldName) {
            return Arrays.hashCode(record.getCompactRecordArray(fieldName));
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord record, String fieldName) {
            writer.writeCompactRecordArray(fieldName, record.getCompactRecordArray(fieldName));
        }
    }),
    BOOLEAN(new FieldTypeBasedOperations() {
        @Override
        public Boolean readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getBoolean(fieldName);
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord compactRecord, String fieldName) {
            writer.writeBoolean(fieldName, compactRecord.getBoolean(fieldName));
        }
    }),
    BOOLEAN_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getBooleanArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalCompactRecord record, String fieldName, int index) {
            return record.getBooleanFromArray(fieldName, index);
        }

        @Override
        public int hashCode(CompactRecord record, String fieldName) {
            return Arrays.hashCode(record.getBooleanArray(fieldName));
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord record, String fieldName) {
            writer.writeBooleanArray(fieldName, record.getBooleanArray(fieldName));
        }
    }),
    BYTE(new FieldTypeBasedOperations() {
        @Override
        public Byte readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getByte(fieldName);
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord compactRecord, String fieldName) {
            writer.writeByte(fieldName, compactRecord.getByte(fieldName));
        }
    }),
    BYTE_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getByteArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalCompactRecord record, String fieldName, int index) {
            return record.getByteFromArray(fieldName, index);
        }

        @Override
        public int hashCode(CompactRecord record, String fieldName) {
            return Arrays.hashCode(record.getByteArray(fieldName));
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord record, String fieldName) {
            writer.writeByteArray(fieldName, record.getByteArray(fieldName));
        }
    }),
    CHAR(new FieldTypeBasedOperations() {
        @Override
        public Character readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getChar(fieldName);
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord compactRecord, String fieldName) {
            writer.writeChar(fieldName, compactRecord.getChar(fieldName));
        }
    }),
    CHAR_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getCharArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalCompactRecord record, String fieldName, int index) {
            return record.getCharFromArray(fieldName, index);
        }

        @Override
        public int hashCode(CompactRecord record, String fieldName) {
            return Arrays.hashCode(record.getCharArray(fieldName));
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord record, String fieldName) {
            writer.writeCharArray(fieldName, record.getCharArray(fieldName));
        }
    }),
    SHORT(new FieldTypeBasedOperations() {
        @Override
        public Short readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getShort(fieldName);
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord compactRecord, String fieldName) {
            writer.writeShort(fieldName, compactRecord.getShort(fieldName));
        }
    }),
    SHORT_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getShortArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalCompactRecord record, String fieldName, int index) {
            return record.getShortFromArray(fieldName, index);
        }

        @Override
        public int hashCode(CompactRecord record, String fieldName) {
            return Arrays.hashCode(record.getShortArray(fieldName));
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord record, String fieldName) {
            writer.writeShortArray(fieldName, record.getShortArray(fieldName));
        }
    }),
    INT(new FieldTypeBasedOperations() {
        @Override
        public Integer readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getInt(fieldName);
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord compactRecord, String fieldName) {
            writer.writeInt(fieldName, compactRecord.getInt(fieldName));
        }
    }),
    INT_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getIntArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalCompactRecord record, String fieldName, int index) {
            return record.getIntFromArray(fieldName, index);
        }

        @Override
        public int hashCode(CompactRecord record, String fieldName) {
            return Arrays.hashCode(record.getIntArray(fieldName));
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord record, String fieldName) {
            writer.writeIntArray(fieldName, record.getIntArray(fieldName));
        }
    }),
    FLOAT(new FieldTypeBasedOperations() {
        @Override
        public Float readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getFloat(fieldName);
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord compactRecord, String fieldName) {
            writer.writeFloat(fieldName, compactRecord.getFloat(fieldName));
        }
    }),
    FLOAT_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getFloatArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalCompactRecord record, String fieldName, int index) {
            return record.getFloatFromArray(fieldName, index);
        }

        @Override
        public int hashCode(CompactRecord record, String fieldName) {
            return Arrays.hashCode(record.getFloatArray(fieldName));
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord record, String fieldName) {
            writer.writeFloatArray(fieldName, record.getFloatArray(fieldName));
        }
    }),
    LONG(new FieldTypeBasedOperations() {
        @Override
        public Long readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getLong(fieldName);
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord compactRecord, String fieldName) {
            writer.writeLong(fieldName, compactRecord.getLong(fieldName));
        }
    }),
    LONG_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getLongArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalCompactRecord record, String fieldName, int index) {
            return record.getLongFromArray(fieldName, index);
        }

        @Override
        public int hashCode(CompactRecord record, String fieldName) {
            return Arrays.hashCode(record.getLongArray(fieldName));
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord record, String fieldName) {
            writer.writeLongArray(fieldName, record.getLongArray(fieldName));
        }
    }),
    DOUBLE(new FieldTypeBasedOperations() {
        @Override
        public Double readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getDouble(fieldName);
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord compactRecord, String fieldName) {
            writer.writeDouble(fieldName, compactRecord.getDouble(fieldName));
        }
    }),
    DOUBLE_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getDoubleArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalCompactRecord record, String fieldName, int index) {
            return record.getDoubleFromArray(fieldName, index);
        }

        @Override
        public int hashCode(CompactRecord record, String fieldName) {
            return Arrays.hashCode(record.getDoubleArray(fieldName));
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord record, String fieldName) {
            writer.writeDoubleArray(fieldName, record.getDoubleArray(fieldName));
        }
    }),
    STRING(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getString(fieldName);
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord compactRecord, String fieldName) {
            writer.writeString(fieldName, compactRecord.getString(fieldName));
        }
    }),
    STRING_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getStringArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalCompactRecord record, String fieldName, int index) {
            return record.getStringFromArray(fieldName, index);
        }

        @Override
        public int hashCode(CompactRecord record, String fieldName) {
            return Arrays.hashCode(record.getStringArray(fieldName));
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord record, String fieldName) {
            writer.writeStringArray(fieldName, record.getStringArray(fieldName));
        }
    }),
    DECIMAL(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getDecimal(fieldName);
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord compactRecord, String fieldName) {
            writer.writeDecimal(fieldName, compactRecord.getDecimal(fieldName));
        }
    }),
    DECIMAL_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getDecimalArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalCompactRecord record, String fieldName, int index) {
            return record.getDecimalFromArray(fieldName, index);
        }

        @Override
        public int hashCode(CompactRecord record, String fieldName) {
            return Arrays.hashCode(record.getDecimalArray(fieldName));
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord record, String fieldName) {
            writer.writeDecimalArray(fieldName, record.getDecimalArray(fieldName));
        }
    }),
    LOCAL_TIME(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getTime(fieldName);
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord compactRecord, String fieldName) {
            writer.writeTime(fieldName, compactRecord.getTime(fieldName));
        }
    }),
    LOCAL_TIME_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getTimeArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalCompactRecord record, String fieldName, int index) {
            return record.getTimeFromArray(fieldName, index);
        }

        @Override
        public int hashCode(CompactRecord record, String fieldName) {
            return Arrays.hashCode(record.getTimeArray(fieldName));
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord record, String fieldName) {
            writer.writeTimeArray(fieldName, record.getTimeArray(fieldName));
        }
    }),
    LOCAL_DATE(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getDate(fieldName);
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord compactRecord, String fieldName) {
            writer.writeDate(fieldName, compactRecord.getDate(fieldName));
        }
    }),
    LOCAL_DATE_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getDateArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalCompactRecord record, String fieldName, int index) {
            return record.getDateFromArray(fieldName, index);
        }

        @Override
        public int hashCode(CompactRecord record, String fieldName) {
            return Arrays.hashCode(record.getDateArray(fieldName));
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord record, String fieldName) {
            writer.writeDateArray(fieldName, record.getDateArray(fieldName));
        }
    }),
    LOCAL_DATE_TIME(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getTimestamp(fieldName);
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord compactRecord, String fieldName) {
            writer.writeTimestamp(fieldName, compactRecord.getTimestamp(fieldName));
        }
    }),
    LOCAL_DATE_TIME_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getTimestampArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalCompactRecord record, String fieldName, int index) {
            return record.getTimestampFromArray(fieldName, index);
        }

        @Override
        public int hashCode(CompactRecord record, String fieldName) {
            return Arrays.hashCode(record.getTimestampArray(fieldName));
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord record, String fieldName) {
            writer.writeTimestampArray(fieldName, record.getTimestampArray(fieldName));
        }
    }),
    OFFSET_DATE_TIME(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getTimestampWithTimezone(fieldName);
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord compactRecord, String fieldName) {
            writer.writeTimestampWithTimezone(fieldName, compactRecord.getTimestampWithTimezone(fieldName));
        }
    }),
    OFFSET_DATE_TIME_ARRAY(new FieldTypeBasedOperations() {
        @Override
        public Object readObject(CompactRecord compactRecord, String fieldName) {
            return compactRecord.getTimestampWithTimezoneArray(fieldName);
        }

        @Override
        public Object readIndexed(InternalCompactRecord record, String fieldName, int index) {
            return record.getTimestampWithTimezoneFromArray(fieldName, index);
        }

        @Override
        public int hashCode(CompactRecord record, String fieldName) {
            return Arrays.hashCode(record.getTimestampWithTimezoneArray(fieldName));
        }

        @Override
        public void readFromCompactRecordToWriter(DefaultCompactWriter writer, CompactRecord record, String fieldName) {
            writer.writeTimestampWithTimezoneArray(fieldName, record.getTimestampWithTimezoneArray(fieldName));
        }
    });

    private static final FieldOperations[] ALL = FieldOperations.values();

    private final FieldTypeBasedOperations operations;

    FieldOperations(FieldTypeBasedOperations operations) {
        this.operations = operations;
    }

    public static FieldTypeBasedOperations fieldOperations(TypeID fieldType) {
        FieldOperations fieldOperations = ALL[fieldType.getId()];
        return fieldOperations.operations;
    }
}
