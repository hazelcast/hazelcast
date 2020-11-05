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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.portable.PortableGenericRecordBuilder;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Arrays;

import static com.hazelcast.jet.sql.impl.inject.UpsertInjector.FAILING_TOP_LEVEL_INJECTOR;

@NotThreadSafe
class PortableUpsertTarget implements UpsertTarget {

    private final ClassDefinition classDefinition;

    private final Object[] values;

    PortableUpsertTarget(
            InternalSerializationService serializationService,
            int factoryId, int classId, int classVersion
    ) {
        this.classDefinition = lookupClassDefinition(serializationService, factoryId, classId, classVersion);

        this.values = new Object[classDefinition.getFieldCount()];
    }

    private static ClassDefinition lookupClassDefinition(
            InternalSerializationService serializationService,
            int factoryId,
            int classId,
            int classVersion
    ) {
        ClassDefinition classDefinition = serializationService
                .getPortableContext()
                .lookupClassDefinition(factoryId, classId, classVersion);
        if (classDefinition == null) {
            throw QueryException.error(
                    "Unable to find class definition for factoryId: " + factoryId
                    + ", classId: " + classId + ", classVersion: " + classVersion
            );
        }
        return classDefinition;
    }

    @Override
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        if (path == null) {
            return FAILING_TOP_LEVEL_INJECTOR;
        }

        int fieldIndex = classDefinition.hasField(path) ? classDefinition.getField(path).getIndex() : -1;
        return value -> {
            if (fieldIndex == -1 && value != null) {
                throw QueryException.error("Unable to inject a non-null value to \"" + path + "\"");
            }

            if (fieldIndex > -1) {
                values[fieldIndex] = value;
            }
        };
    }

    @Override
    public void init() {
        Arrays.fill(values, null);
    }

    @Override
    public Object conclude() {
        GenericRecord record = toRecord(classDefinition, values);
        Arrays.fill(values, null);
        return record;
    }

    private static GenericRecord toRecord(ClassDefinition classDefinition, Object[] values) {
        PortableGenericRecordBuilder portable = new PortableGenericRecordBuilder(classDefinition);
        for (int i = 0; i < classDefinition.getFieldCount(); i++) {
            FieldDefinition fieldDefinition = classDefinition.getField(i);
            String name = fieldDefinition.getName();
            FieldType type = fieldDefinition.getType();

            Object value = values[i];
            try {
                switch (type) {
                    case BOOLEAN:
                        portable.writeBoolean(name, value != null && (boolean) value);
                        break;
                    case BYTE:
                        portable.writeByte(name, value == null ? (byte) 0 : (byte) value);
                        break;
                    case SHORT:
                        portable.writeShort(name, value == null ? (short) 0 : (short) value);
                        break;
                    case CHAR:
                        portable.writeChar(name, value == null ? (char) 0 : (char) value);
                        break;
                    case INT:
                        portable.writeInt(name, value == null ? 0 : (int) value);
                        break;
                    case LONG:
                        portable.writeLong(name, value == null ? 0L : (long) value);
                        break;
                    case FLOAT:
                        portable.writeFloat(name, value == null ? 0F : (float) value);
                        break;
                    case DOUBLE:
                        portable.writeDouble(name, value == null ? 0D : (double) value);
                        break;
                    case UTF:
                        portable.writeUTF(name, (String) QueryDataType.VARCHAR.convert(value));
                        break;
                    case PORTABLE:
                        portable.writeGenericRecord(name, (GenericRecord) value);
                        break;
                    case BOOLEAN_ARRAY:
                        portable.writeBooleanArray(name, (boolean[]) value);
                        break;
                    case BYTE_ARRAY:
                        portable.writeByteArray(name, (byte[]) value);
                        break;
                    case SHORT_ARRAY:
                        portable.writeShortArray(name, (short[]) value);
                        break;
                    case CHAR_ARRAY:
                        portable.writeCharArray(name, (char[]) value);
                        break;
                    case INT_ARRAY:
                        portable.writeIntArray(name, (int[]) value);
                        break;
                    case LONG_ARRAY:
                        portable.writeLongArray(name, (long[]) value);
                        break;
                    case FLOAT_ARRAY:
                        portable.writeFloatArray(name, (float[]) value);
                        break;
                    case DOUBLE_ARRAY:
                        portable.writeDoubleArray(name, (double[]) value);
                        break;
                    case UTF_ARRAY:
                        portable.writeUTFArray(name, (String[]) value);
                        break;
                    case PORTABLE_ARRAY:
                        portable.writeGenericRecordArray(name, (GenericRecord[]) value);
                        break;
                    default:
                        throw QueryException.error("Unsupported type: " + type);
                }
            } catch (Exception e) {
                throw QueryException.error("Cannot set value " +
                                           (value == null ? "null" : " of type " + value.getClass().getName())
                                           + " to field \"" + name + "\" of type " + type + ": " + e.getMessage(), e);
            }
        }
        return portable.build();
    }
}
