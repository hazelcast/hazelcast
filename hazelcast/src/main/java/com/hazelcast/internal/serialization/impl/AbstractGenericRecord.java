/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
    protected abstract Object getClassIdentifier();

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractGenericRecord)) {
            return false;
        }
        AbstractGenericRecord that = (AbstractGenericRecord) o;
        if (!that.getClassIdentifier().equals(getClassIdentifier())) {
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
                return Arrays.hashCode(record.readByteArray(path));
            case SHORT_ARRAY:
                return Arrays.hashCode(record.readShortArray(path));
            case INT_ARRAY:
                return Arrays.hashCode(record.readIntArray(path));
            case LONG_ARRAY:
                return Arrays.hashCode(record.readLongArray(path));
            case FLOAT_ARRAY:
                return Arrays.hashCode(record.readFloatArray(path));
            case DOUBLE_ARRAY:
                return Arrays.hashCode(record.readDoubleArray(path));
            case BOOLEAN_ARRAY:
                return Arrays.hashCode(record.readBooleanArray(path));
            case CHAR_ARRAY:
                return Arrays.hashCode(record.readCharArray(path));
            case UTF_ARRAY:
                return Arrays.hashCode(record.readUTFArray(path));
            case PORTABLE_ARRAY:
                return Arrays.hashCode(record.readGenericRecordArray(path));
            default:
                throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    private static Object readAny(GenericRecord record, String path, FieldType type) {
        switch (type) {
            case BYTE:
                return record.readByte(path);
            case BYTE_ARRAY:
                return record.readByteArray(path);
            case SHORT:
                return record.readShort(path);
            case SHORT_ARRAY:
                return record.readShortArray(path);
            case INT:
                return record.readInt(path);
            case INT_ARRAY:
                return record.readIntArray(path);
            case LONG:
                return record.readLong(path);
            case LONG_ARRAY:
                return record.readLongArray(path);
            case FLOAT:
                return record.readFloat(path);
            case FLOAT_ARRAY:
                return record.readFloatArray(path);
            case DOUBLE:
                return record.readDouble(path);
            case DOUBLE_ARRAY:
                return record.readDoubleArray(path);
            case BOOLEAN:
                return record.readBoolean(path);
            case BOOLEAN_ARRAY:
                return record.readBooleanArray(path);
            case CHAR:
                return record.readChar(path);
            case CHAR_ARRAY:
                return record.readCharArray(path);
            case UTF:
                return record.readUTF(path);
            case UTF_ARRAY:
                return record.readUTFArray(path);
            case PORTABLE:
                return record.readGenericRecord(path);
            case PORTABLE_ARRAY:
                return record.readGenericRecordArray(path);
            default:
                throw new IllegalArgumentException("Unsupported type " + type);
        }
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getClassIdentifier().toString());
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
