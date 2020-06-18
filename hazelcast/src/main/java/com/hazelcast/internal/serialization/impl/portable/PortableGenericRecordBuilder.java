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

public class PortableGenericRecordBuilder implements GenericRecordBuilder {

    private final ClassDefinition classDefinition;
    private final Object[] objects;

    public PortableGenericRecordBuilder(ClassDefinition classDefinition) {
        this.classDefinition = classDefinition;
        this.objects = new Object[classDefinition.getFieldCount()];
    }

    public ClassDefinition getClassDefinition() {
        return classDefinition;
    }


    @Override
    public GenericRecord build() {
        return new PortableGenericRecord(classDefinition, objects);
    }

    @Override
    public GenericRecordBuilder writeInt(String fieldName, int value) {
        return write(fieldName, value, FieldType.INT);
    }

    @Override
    public GenericRecordBuilder writeLong(String fieldName, long value) {
        return write(fieldName, value, FieldType.LONG);
    }

    @Override
    public GenericRecordBuilder writeUTF(String fieldName, String value) {
        return write(fieldName, value, FieldType.UTF);
    }

    @Override
    public GenericRecordBuilder writeBoolean(String fieldName, boolean value) {
        return write(fieldName, value, FieldType.BOOLEAN);
    }

    @Override
    public GenericRecordBuilder writeByte(String fieldName, byte value) {
        return write(fieldName, value, FieldType.BYTE);
    }

    @Override
    public GenericRecordBuilder writeChar(String fieldName, char value) {
        return write(fieldName, value, FieldType.CHAR);
    }

    @Override
    public GenericRecordBuilder writeDouble(String fieldName, double value) {
        return write(fieldName, value, FieldType.DOUBLE);
    }

    @Override
    public GenericRecordBuilder writeFloat(String fieldName, float value) {
        return write(fieldName, value, FieldType.FLOAT);
    }

    @Override
    public GenericRecordBuilder writeShort(String fieldName, short value) {
        return write(fieldName, value, FieldType.SHORT);
    }

    @Override
    public GenericRecordBuilder writeGenericRecord(String fieldName, GenericRecord value) {
        return write(fieldName, value, FieldType.PORTABLE);
    }

    @Override
    public GenericRecordBuilder writeGenericRecordArray(String fieldName, GenericRecord[] value) {
        return write(fieldName, value, FieldType.PORTABLE_ARRAY);
    }

    @Override
    public GenericRecordBuilder writeByteArray(String fieldName, byte[] value) {
        return write(fieldName, value, FieldType.BYTE_ARRAY);
    }

    @Override
    public GenericRecordBuilder writeBooleanArray(String fieldName, boolean[] value) {
        return write(fieldName, value, FieldType.BOOLEAN_ARRAY);
    }

    @Override
    public GenericRecordBuilder writeCharArray(String fieldName, char[] value) {
        return write(fieldName, value, FieldType.CHAR_ARRAY);
    }

    @Override
    public GenericRecordBuilder writeIntArray(String fieldName, int[] value) {
        return write(fieldName, value, FieldType.INT_ARRAY);
    }

    @Override
    public GenericRecordBuilder writeLongArray(String fieldName, long[] value) {
        return write(fieldName, value, FieldType.LONG_ARRAY);
    }

    @Override
    public GenericRecordBuilder writeDoubleArray(String fieldName, double[] value) {
        return write(fieldName, value, FieldType.DOUBLE_ARRAY);
    }

    @Override
    public GenericRecordBuilder writeFloatArray(String fieldName, float[] value) {
        return write(fieldName, value, FieldType.FLOAT_ARRAY);
    }

    @Override
    public GenericRecordBuilder writeShortArray(String fieldName, short[] value) {
        return write(fieldName, value, FieldType.SHORT_ARRAY);
    }

    @Override
    public GenericRecordBuilder writeUTFArray(String fieldName, String[] value) {
        return write(fieldName, value, FieldType.UTF_ARRAY);
    }

    private GenericRecordBuilder write(String fieldName, Object value, FieldType fieldType) {
        FieldDefinition fd = check(fieldName, fieldType);
        objects[fd.getIndex()] = value;
        return this;
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
