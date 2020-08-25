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
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class PortableGenericRecordBuilder implements GenericRecord.Builder {

    private final ClassDefinition classDefinition;
    private final Object[] objects;
    private final boolean[] isOverWritten;
    private final boolean isClone;

    public PortableGenericRecordBuilder(ClassDefinition classDefinition) {
        this.classDefinition = classDefinition;
        this.objects = new Object[classDefinition.getFieldCount()];
        this.isClone = false;
        this.isOverWritten = null;
    }

    PortableGenericRecordBuilder(ClassDefinition classDefinition, Object[] objects) {
        this.classDefinition = classDefinition;
        this.objects = objects;
        this.isClone = true;
        this.isOverWritten = new boolean[objects.length];
    }

    @Override
    public GenericRecord build() {
        return new PortableGenericRecord(classDefinition, objects);
    }

    @Override
    public GenericRecord.Builder writeInt(String fieldName, int value) {
        return write(fieldName, value, FieldType.INT);
    }

    @Override
    public GenericRecord.Builder writeLong(String fieldName, long value) {
        return write(fieldName, value, FieldType.LONG);
    }

    @Override
    public GenericRecord.Builder writeUTF(String fieldName, String value) {
        return write(fieldName, value, FieldType.UTF);
    }

    @Override
    public GenericRecord.Builder writeBoolean(String fieldName, boolean value) {
        return write(fieldName, value, FieldType.BOOLEAN);
    }

    @Override
    public GenericRecord.Builder writeByte(String fieldName, byte value) {
        return write(fieldName, value, FieldType.BYTE);
    }

    @Override
    public GenericRecord.Builder writeChar(String fieldName, char value) {
        return write(fieldName, value, FieldType.CHAR);
    }

    @Override
    public GenericRecord.Builder writeDouble(String fieldName, double value) {
        return write(fieldName, value, FieldType.DOUBLE);
    }

    @Override
    public GenericRecord.Builder writeFloat(String fieldName, float value) {
        return write(fieldName, value, FieldType.FLOAT);
    }

    @Override
    public GenericRecord.Builder writeShort(String fieldName, short value) {
        return write(fieldName, value, FieldType.SHORT);
    }

    @Override
    public GenericRecord.Builder writeGenericRecord(String fieldName, @Nullable GenericRecord value) {
        return write(fieldName, value, FieldType.PORTABLE);
    }

    @Override
    public GenericRecord.Builder writeGenericRecordArray(String fieldName, GenericRecord[] value) {
        return write(fieldName, value, FieldType.PORTABLE_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeByteArray(String fieldName, byte[] value) {
        return write(fieldName, value, FieldType.BYTE_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeBooleanArray(String fieldName, boolean[] value) {
        return write(fieldName, value, FieldType.BOOLEAN_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeCharArray(String fieldName, char[] value) {
        return write(fieldName, value, FieldType.CHAR_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeIntArray(String fieldName, int[] value) {
        return write(fieldName, value, FieldType.INT_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeLongArray(String fieldName, long[] value) {
        return write(fieldName, value, FieldType.LONG_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeDoubleArray(String fieldName, double[] value) {
        return write(fieldName, value, FieldType.DOUBLE_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeFloatArray(String fieldName, float[] value) {
        return write(fieldName, value, FieldType.FLOAT_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeShortArray(String fieldName, short[] value) {
        return write(fieldName, value, FieldType.SHORT_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeUTFArray(String fieldName, String[] value) {
        return write(fieldName, value, FieldType.UTF_ARRAY);
    }

    private GenericRecord.Builder write(String fieldName, Object value, FieldType fieldType) {
        FieldDefinition fd = check(fieldName, fieldType);
        int index = fd.getIndex();
        if (objects[index] != null) {
            if (!isClone) {
                throw new HazelcastSerializationException("It is illegal to the overwrite the field");
            } else if (isOverWritten[index]) {
                throw new HazelcastSerializationException("Field can only overwritten once with `cloneWithBuilder`");
            }
        }
        objects[index] = value;
        if (isClone) {
            isOverWritten[index] = true;
        }
        return this;
    }

    @Nonnull
    private FieldDefinition check(String fieldName, FieldType fieldType) {
        FieldDefinition fd = classDefinition.getField(fieldName);
        if (fd == null) {
            throw new HazelcastSerializationException("Invalid field name: '" + fieldName
                    + "' for ClassDefinition {id: " + classDefinition.getClassId()
                    + ", version: " + classDefinition.getVersion() + "}");
        }
        if (!fd.getType().equals(fieldType)) {
            throw new HazelcastSerializationException("Invalid field type: '" + fieldName
                    + "' for ClassDefinition {id: " + classDefinition.getClassId() + ", version: "
                    + classDefinition.getVersion() + "}" + ", expected : " + fd.getType() + ", given : " + fieldType);
        }
        return fd;
    }

}
