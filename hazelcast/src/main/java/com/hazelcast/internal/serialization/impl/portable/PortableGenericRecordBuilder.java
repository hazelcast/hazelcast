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
    private final boolean[] isWritten;
    private final boolean isClone;

    public PortableGenericRecordBuilder(ClassDefinition classDefinition) {
        this.classDefinition = classDefinition;
        this.objects = new Object[classDefinition.getFieldCount()];
        this.isClone = false;
        this.isWritten = new boolean[objects.length];
    }

    PortableGenericRecordBuilder(ClassDefinition classDefinition, Object[] objects) {
        this.classDefinition = classDefinition;
        this.objects = objects;
        this.isClone = true;
        this.isWritten = new boolean[objects.length];
    }

    /**
     * @return newly created GenericRecord
     * @throws HazelcastSerializationException if a field is not written when building with builder from
     *                                         {@link GenericRecord.Builder#portable(ClassDefinition)} and
     *                                         {@link GenericRecord#newBuilder()}
     */
    @Nonnull
    @Override
    public GenericRecord build() {
        if (!isClone) {
            for (int i = 0; i < isWritten.length; i++) {
                if (!isWritten[i]) {
                    throw new HazelcastSerializationException("All fields must be written when building"
                            + " a GenericRecord for portable, unwritten field :" + classDefinition.getField(i));
                }
            }
        }
        return new PortableGenericRecord(classDefinition, objects);
    }

    @Override
    public GenericRecord.Builder writeInt(@Nonnull String fieldName, int value) {
        return write(fieldName, value, FieldType.INT);
    }

    @Override
    public GenericRecord.Builder writeLong(@Nonnull String fieldName, long value) {
        return write(fieldName, value, FieldType.LONG);
    }

    @Override
    public GenericRecord.Builder writeUTF(@Nonnull String fieldName, String value) {
        return write(fieldName, value, FieldType.UTF);
    }

    @Override
    public GenericRecord.Builder writeBoolean(@Nonnull String fieldName, boolean value) {
        return write(fieldName, value, FieldType.BOOLEAN);
    }

    @Override
    public GenericRecord.Builder writeByte(@Nonnull String fieldName, byte value) {
        return write(fieldName, value, FieldType.BYTE);
    }

    @Override
    public GenericRecord.Builder writeChar(@Nonnull String fieldName, char value) {
        return write(fieldName, value, FieldType.CHAR);
    }

    @Override
    public GenericRecord.Builder writeDouble(@Nonnull String fieldName, double value) {
        return write(fieldName, value, FieldType.DOUBLE);
    }

    @Override
    public GenericRecord.Builder writeFloat(@Nonnull String fieldName, float value) {
        return write(fieldName, value, FieldType.FLOAT);
    }

    @Override
    public GenericRecord.Builder writeShort(@Nonnull String fieldName, short value) {
        return write(fieldName, value, FieldType.SHORT);
    }

    @Override
    public GenericRecord.Builder writeGenericRecord(@Nonnull String fieldName, @Nullable GenericRecord value) {
        return write(fieldName, value, FieldType.PORTABLE);
    }

    @Override
    public GenericRecord.Builder writeGenericRecordArray(@Nonnull String fieldName, GenericRecord[] value) {
        return write(fieldName, value, FieldType.PORTABLE_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeByteArray(@Nonnull String fieldName, byte[] value) {
        return write(fieldName, value, FieldType.BYTE_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeBooleanArray(@Nonnull String fieldName, boolean[] value) {
        return write(fieldName, value, FieldType.BOOLEAN_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeCharArray(@Nonnull String fieldName, char[] value) {
        return write(fieldName, value, FieldType.CHAR_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeIntArray(@Nonnull String fieldName, int[] value) {
        return write(fieldName, value, FieldType.INT_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeLongArray(@Nonnull String fieldName, long[] value) {
        return write(fieldName, value, FieldType.LONG_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeDoubleArray(@Nonnull String fieldName, double[] value) {
        return write(fieldName, value, FieldType.DOUBLE_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeFloatArray(@Nonnull String fieldName, float[] value) {
        return write(fieldName, value, FieldType.FLOAT_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeShortArray(@Nonnull String fieldName, short[] value) {
        return write(fieldName, value, FieldType.SHORT_ARRAY);
    }

    @Override
    public GenericRecord.Builder writeUTFArray(@Nonnull String fieldName, String[] value) {
        return write(fieldName, value, FieldType.UTF_ARRAY);
    }

    private GenericRecord.Builder write(@Nonnull String fieldName, Object value, FieldType fieldType) {
        FieldDefinition fd = check(fieldName, fieldType);
        int index = fd.getIndex();
        if (isWritten[index]) {
            if (!isClone) {
                throw new HazelcastSerializationException("It is illegal to the overwrite the field");
            } else {
                throw new HazelcastSerializationException("Field can only overwritten once with `cloneWithBuilder`");
            }
        }
        objects[index] = value;
        isWritten[index] = true;
        return this;
    }

    @Nonnull
    private FieldDefinition check(@Nonnull String fieldName, FieldType fieldType) {
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
