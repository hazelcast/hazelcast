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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import javax.annotation.Nonnull;

public class PortableGenericRecord implements GenericRecord {

    private final ClassDefinition classDefinition;
    private final Object[] objects;

    public PortableGenericRecord(ClassDefinition classDefinition, Object[] objects) {
        this.classDefinition = classDefinition;
        this.objects = objects;
    }

    public ClassDefinition getClassDefinition() {
        return classDefinition;
    }

    @Override
    public GenericRecordBuilder createGenericRecordBuilder() {
        return GenericRecordBuilder.portable(classDefinition);
    }

    @Override
    public GenericRecord[] readGenericRecordArray(String fieldName) {
        return read(fieldName, FieldType.PORTABLE_ARRAY);
    }

    @Override
    public GenericRecord readGenericRecord(String fieldName) {
        return read(fieldName, FieldType.PORTABLE);
    }

    @Override
    public boolean hasField(String fieldName) {
        return classDefinition.hasField(fieldName);
    }

    @Override
    public FieldType getFieldType(String fieldName) {
        return classDefinition.getFieldType(fieldName);
    }

    @Override
    public int readInt(String fieldName) {
        return read(fieldName, FieldType.INT);
    }

    @Override
    public long readLong(String fieldName) {
        return read(fieldName, FieldType.LONG);
    }

    @Override
    public String readUTF(String fieldName) {
        return read(fieldName, FieldType.UTF);
    }

    @Override
    public boolean readBoolean(String fieldName) {
        return read(fieldName, FieldType.BOOLEAN);
    }

    @Override
    public byte readByte(String fieldName) {
        return read(fieldName, FieldType.BYTE);
    }

    @Override
    public char readChar(String fieldName) {
        return read(fieldName, FieldType.CHAR);
    }

    @Override
    public double readDouble(String fieldName) {
        return read(fieldName, FieldType.DOUBLE);
    }

    @Override
    public float readFloat(String fieldName) {
        return read(fieldName, FieldType.FLOAT);
    }

    @Override
    public short readShort(String fieldName) {
        return read(fieldName, FieldType.SHORT);
    }

    @Override
    public byte[] readByteArray(String fieldName) {
        return read(fieldName, FieldType.BYTE_ARRAY);
    }

    @Override
    public boolean[] readBooleanArray(String fieldName) {
        return read(fieldName, FieldType.BOOLEAN_ARRAY);
    }

    @Override
    public char[] readCharArray(String fieldName) {
        return read(fieldName, FieldType.CHAR_ARRAY);
    }

    @Override
    public int[] readIntArray(String fieldName) {
        return read(fieldName, FieldType.INT_ARRAY);
    }

    @Override
    public long[] readLongArray(String fieldName) {
        return read(fieldName, FieldType.LONG_ARRAY);
    }

    @Override
    public double[] readDoubleArray(String fieldName) {
        return read(fieldName, FieldType.DOUBLE_ARRAY);
    }

    @Override
    public float[] readFloatArray(String fieldName) {
        return read(fieldName, FieldType.FLOAT_ARRAY);
    }

    @Override
    public short[] readShortArray(String fieldName) {
        return read(fieldName, FieldType.SHORT_ARRAY);
    }

    @Override
    public String[] readUTFArray(String fieldName) {
        return read(fieldName, FieldType.UTF_ARRAY);
    }

    private <T> T read(String fieldName, FieldType fieldType) {
        FieldDefinition fd = check(fieldName, fieldType);
        return (T) objects[fd.getIndex()];
    }

    @Nonnull
    private FieldDefinition check(String fieldName, FieldType fieldType) {
        FieldDefinition fd = classDefinition.getField(fieldName);
        if (fd == null) {
            throw new HazelcastSerializationException("Invalid field name: '" + fieldName
                    + "' for ClassDefinition {id: " + classDefinition.getClassId() + ", version: " + classDefinition.getVersion() + "}");
        }
        if (!fd.getType().equals(fieldType)) {
            throw new HazelcastSerializationException("Invalid field type: '" + fieldName
                    + "' for ClassDefinition {id: " + classDefinition.getClassId() + ", version: " + classDefinition.getVersion() + "}"
                    + ", expected : " + fd.getType() + ", given : " + fieldType);
        }
        return fd;
    }

}
