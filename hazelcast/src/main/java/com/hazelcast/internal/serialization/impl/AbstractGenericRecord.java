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

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

/**
 * Implementation of GenericRecord interface to give common equals and hashCode implementations.
 */
@SuppressWarnings({"checkstyle:returncount", "checkstyle:cyclomaticcomplexity"})
public abstract class AbstractGenericRecord implements GenericRecord {

    /**
     * @return a unique identifier for each class.
     * Note that identifiers should be different for for the different version of the same class as well.
     */
    protected abstract ClassDefinition getClassDefinition();

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractGenericRecord)) {
            return false;
        }
        AbstractGenericRecord that = (AbstractGenericRecord) o;
        if (!that.getClassDefinition().equals(getClassDefinition())) {
            return false;
        }
        Set<String> thatFieldNames = that.getFieldNames();
        Set<String> thisFieldNames = getFieldNames();
        if (!Objects.equals(thatFieldNames, thisFieldNames)) {
            return false;
        }
        for (String fieldName : thatFieldNames) {
            FieldType thatFieldType = that.getFieldType(fieldName);
            FieldType thisFieldType = getFieldType(fieldName);
            if (!thatFieldType.equals(thisFieldType)) {
                return false;
            }
            if (thatFieldType.isArrayType()) {
                if (!Objects.deepEquals(readAny(that, fieldName, thatFieldType), readAny(this, fieldName, thisFieldType))) {
                    return false;
                }
            } else {
                if (!Objects.equals(readAny(that, fieldName, thatFieldType), readAny(this, fieldName, thisFieldType))) {
                    return false;
                }
            }
        }
        return true;
    }

    public int hashCode() {
        int result = 0;
        Set<String> fieldNames = getFieldNames();
        for (String fieldName : fieldNames) {
            FieldType fieldType = getFieldType(fieldName);
            if (fieldType.isArrayType()) {
                result = 31 * result + arrayHashCode(this, fieldName, fieldType);
            } else {
                result = 31 * result + Objects.hashCode(readAny(this, fieldName, fieldType));
            }
        }
        return result;
    }

    private static int arrayHashCode(GenericRecord record, String path, FieldType type) {
        switch (type) {
            case BYTE_ARRAY:
                return Arrays.hashCode(record.getByteArray(path));
            case SHORT_ARRAY:
                return Arrays.hashCode(record.getShortArray(path));
            case INT_ARRAY:
                return Arrays.hashCode(record.getIntArray(path));
            case LONG_ARRAY:
                return Arrays.hashCode(record.getLongArray(path));
            case FLOAT_ARRAY:
                return Arrays.hashCode(record.getFloatArray(path));
            case DOUBLE_ARRAY:
                return Arrays.hashCode(record.getDoubleArray(path));
            case BOOLEAN_ARRAY:
                return Arrays.hashCode(record.getBooleanArray(path));
            case CHAR_ARRAY:
                return Arrays.hashCode(record.getCharArray(path));
            case UTF_ARRAY:
                return Arrays.hashCode(record.getStringArray(path));
            case PORTABLE_ARRAY:
                return Arrays.hashCode(record.getGenericRecordArray(path));
            case DECIMAL_ARRAY:
                return Arrays.hashCode(record.getDecimalArray(path));
            case TIME_ARRAY:
                return Arrays.hashCode(record.getTimeArray(path));
            case DATE_ARRAY:
                return Arrays.hashCode(record.getDateArray(path));
            case TIMESTAMP_ARRAY:
                return Arrays.hashCode(record.getTimestampArray(path));
            case TIMESTAMP_WITH_TIMEZONE_ARRAY:
                return Arrays.hashCode(record.getTimestampWithTimezoneArray(path));
            default:
                throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    @SuppressWarnings("checkstyle:MethodLength")
    private static Object readAny(GenericRecord record, String path, FieldType type) {
        switch (type) {
            case BYTE:
                return record.getByte(path);
            case BYTE_ARRAY:
                return record.getByteArray(path);
            case SHORT:
                return record.getShort(path);
            case SHORT_ARRAY:
                return record.getShortArray(path);
            case INT:
                return record.getInt(path);
            case INT_ARRAY:
                return record.getIntArray(path);
            case LONG:
                return record.getLong(path);
            case LONG_ARRAY:
                return record.getLongArray(path);
            case FLOAT:
                return record.getFloat(path);
            case FLOAT_ARRAY:
                return record.getFloatArray(path);
            case DOUBLE:
                return record.getDouble(path);
            case DOUBLE_ARRAY:
                return record.getDoubleArray(path);
            case BOOLEAN:
                return record.getBoolean(path);
            case BOOLEAN_ARRAY:
                return record.getBooleanArray(path);
            case CHAR:
                return record.getChar(path);
            case CHAR_ARRAY:
                return record.getCharArray(path);
            case UTF:
                return record.getString(path);
            case UTF_ARRAY:
                return record.getStringArray(path);
            case PORTABLE:
                return record.getGenericRecord(path);
            case PORTABLE_ARRAY:
                return record.getGenericRecordArray(path);
            case DECIMAL:
                return record.getDecimal(path);
            case DECIMAL_ARRAY:
                return record.getDecimalArray(path);
            case TIME:
                return record.getTime(path);
            case TIME_ARRAY:
                return record.getTimeArray(path);
            case DATE:
                return record.getDate(path);
            case DATE_ARRAY:
                return record.getDateArray(path);
            case TIMESTAMP:
                return record.getTimestamp(path);
            case TIMESTAMP_ARRAY:
                return record.getTimestampArray(path);
            case TIMESTAMP_WITH_TIMEZONE:
                return record.getTimestampWithTimezone(path);
            case TIMESTAMP_WITH_TIMEZONE_ARRAY:
                return record.getTimestampWithTimezoneArray(path);
            default:
                throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getClassDefinition().toString());
        str.append("\n{");
        Set<String> thisFieldNames = getFieldNames();
        for (String fieldName : thisFieldNames) {
            FieldType fieldType = getFieldType(fieldName);
            Object field = readAny(this, fieldName, fieldType);
            str.append("\"").append(fieldName).append("\" : ");
            if (fieldType.isArrayType()) {
                switch (fieldType) {
                    case BYTE_ARRAY:
                        str.append(Arrays.toString((byte[]) field));
                        break;
                    case SHORT_ARRAY:
                        str.append(Arrays.toString((short[]) field));
                        break;
                    case INT_ARRAY:
                        str.append(Arrays.toString((int[]) field));
                        break;
                    case LONG_ARRAY:
                        str.append(Arrays.toString((long[]) field));
                        break;
                    case FLOAT_ARRAY:
                        str.append(Arrays.toString((float[]) field));
                        break;
                    case DOUBLE_ARRAY:
                        str.append(Arrays.toString((double[]) field));
                        break;
                    case BOOLEAN_ARRAY:
                        str.append(Arrays.toString((boolean[]) field));
                        break;
                    case CHAR_ARRAY:
                        str.append(Arrays.toString((char[]) field));
                        break;
                    case UTF_ARRAY:
                    case DECIMAL_ARRAY:
                    case TIME_ARRAY:
                    case DATE_ARRAY:
                    case TIMESTAMP_ARRAY:
                    case TIMESTAMP_WITH_TIMEZONE_ARRAY:
                    case PORTABLE_ARRAY:
                        str.append(Arrays.toString((Object[]) field));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + fieldType);
                }
            } else {
                str.append(field.toString());
            }
            str.append(", \n");
        }
        str.append("}");
        return str.toString();
    }

}
