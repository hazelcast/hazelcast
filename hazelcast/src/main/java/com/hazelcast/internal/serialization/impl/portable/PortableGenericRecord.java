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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.util.Arrays;

public class PortableGenericRecord implements GenericRecord {

    private final ClassDefinition classDefinition;
    private final Object[] objects;

    @SuppressFBWarnings({"EI_EXPOSE_REP2"})
    public PortableGenericRecord(ClassDefinition classDefinition, Object[] objects) {
        this.classDefinition = classDefinition;
        this.objects = objects;
    }

    public ClassDefinition getClassDefinition() {
        return classDefinition;
    }

    @Nonnull
    @Override
    public Builder newBuilder() {
        return Builder.portable(classDefinition);
    }

    @Nonnull
    @Override
    public Builder cloneWithBuilder() {
        return new PortableGenericRecordBuilder(classDefinition, Arrays.copyOf(objects, objects.length));
    }

    @Override
    public GenericRecord[] readGenericRecordArray(@Nonnull String fieldName) {
        return read(fieldName, FieldType.PORTABLE_ARRAY);
    }

    @Override
    public GenericRecord readGenericRecord(@Nonnull String fieldName) {
        return read(fieldName, FieldType.PORTABLE);
    }

    @Override
    public boolean hasField(@Nonnull String fieldName) {
        return classDefinition.hasField(fieldName);
    }

    @Override
    @Nonnull
    public FieldType getFieldType(@Nonnull String fieldName) {
        return classDefinition.getFieldType(fieldName);
    }

    @Override
    public boolean readBoolean(@Nonnull String fieldName) {
        return read(fieldName, FieldType.BOOLEAN);
    }

    @Override
    public byte readByte(@Nonnull String fieldName) {
        return read(fieldName, FieldType.BYTE);
    }

    @Override
    public char readChar(@Nonnull String fieldName) {
        return read(fieldName, FieldType.CHAR);
    }

    @Override
    public double readDouble(@Nonnull String fieldName) {
        return read(fieldName, FieldType.DOUBLE);
    }

    @Override
    public float readFloat(@Nonnull String fieldName) {
        return read(fieldName, FieldType.FLOAT);
    }

    @Override
    public int readInt(@Nonnull String fieldName) {
        return read(fieldName, FieldType.INT);
    }

    @Override
    public long readLong(@Nonnull String fieldName) {
        return read(fieldName, FieldType.LONG);
    }

    @Override
    public short readShort(@Nonnull String fieldName) {
        return read(fieldName, FieldType.SHORT);
    }

    @Override
    public String readUTF(@Nonnull String fieldName) {
        return read(fieldName, FieldType.UTF);
    }

    @Override
    public boolean[] readBooleanArray(@Nonnull String fieldName) {
        return read(fieldName, FieldType.BOOLEAN_ARRAY);
    }

    @Override
    public byte[] readByteArray(@Nonnull String fieldName) {
        return read(fieldName, FieldType.BYTE_ARRAY);
    }

    @Override
    public char[] readCharArray(@Nonnull String fieldName) {
        return read(fieldName, FieldType.CHAR_ARRAY);
    }

    @Override
    public double[] readDoubleArray(@Nonnull String fieldName) {
        return read(fieldName, FieldType.DOUBLE_ARRAY);
    }

    @Override
    public float[] readFloatArray(@Nonnull String fieldName) {
        return read(fieldName, FieldType.FLOAT_ARRAY);
    }

    @Override
    public int[] readIntArray(@Nonnull String fieldName) {
        return read(fieldName, FieldType.INT_ARRAY);
    }

    @Override
    public long[] readLongArray(@Nonnull String fieldName) {
        return read(fieldName, FieldType.LONG_ARRAY);
    }

    @Override
    public short[] readShortArray(@Nonnull String fieldName) {
        return read(fieldName, FieldType.SHORT_ARRAY);
    }

    @Override
    public String[] readUTFArray(@Nonnull String fieldName) {
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
                    + "' for ClassDefinition {id: " + classDefinition.getClassId() + ", version: "
                    + classDefinition.getVersion() + "}");
        }
        if (!fd.getType().equals(fieldType)) {
            throw new HazelcastSerializationException("Invalid field type: '" + fieldName
                    + "' for ClassDefinition {id: " + classDefinition.getClassId() + ", version: "
                    + classDefinition.getVersion() + "}" + ", expected : " + fd.getType() + ", given : " + fieldType);
        }
        return fd;
    }

    @Override
    public String toString() {
        return "PortableGenericRecord{classDefinition=" + classDefinition
                + ", objects=" + Arrays.toString(objects)
                + '}';
    }
}
