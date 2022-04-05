/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableWriter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;

final class ClassDefinitionWriter implements PortableWriter {

    private final PortableContext context;
    private final ClassDefinitionBuilder builder;

    ClassDefinitionWriter(PortableContext context, int factoryId, int classId, int version) {
        this.context = context;
        builder = new ClassDefinitionBuilder(factoryId, classId, version);
    }

    ClassDefinitionWriter(PortableContext context, ClassDefinitionBuilder builder) {
        this.context = context;
        this.builder = builder;
    }

    @Override
    public void writeInt(@Nonnull String fieldName, int value) {
        builder.addIntField(fieldName);
    }

    @Override
    public void writeLong(@Nonnull String fieldName, long value) {
        builder.addLongField(fieldName);
    }

    @Override
    public void writeUTF(@Nonnull String fieldName, @Nullable String str) {
        writeString(fieldName, str);
    }

    @Override
    public void writeString(@Nonnull String fieldName, @Nullable String value) {
        builder.addStringField(fieldName);
    }

    @Override
    public void writeBoolean(@Nonnull String fieldName, boolean value) {
        builder.addBooleanField(fieldName);
    }

    @Override
    public void writeByte(@Nonnull String fieldName, byte value) {
        builder.addByteField(fieldName);
    }

    @Override
    public void writeChar(@Nonnull String fieldName, int value) {
        builder.addCharField(fieldName);
    }

    @Override
    public void writeDouble(@Nonnull String fieldName, double value) {
        builder.addDoubleField(fieldName);
    }

    @Override
    public void writeFloat(@Nonnull String fieldName, float value) {
        builder.addFloatField(fieldName);
    }

    @Override
    public void writeShort(@Nonnull String fieldName, short value) {
        builder.addShortField(fieldName);
    }

    @Override
    public void writeByteArray(@Nonnull String fieldName, byte[] bytes) {
        builder.addByteArrayField(fieldName);
    }

    @Override
    public void writeBooleanArray(@Nonnull String fieldName, boolean[] booleans) {
        builder.addBooleanArrayField(fieldName);
    }

    @Override
    public void writeCharArray(@Nonnull String fieldName, char[] chars) {
        builder.addCharArrayField(fieldName);
    }

    @Override
    public void writeIntArray(@Nonnull String fieldName, int[] ints) {
        builder.addIntArrayField(fieldName);
    }

    @Override
    public void writeLongArray(@Nonnull String fieldName, long[] longs) {
        builder.addLongArrayField(fieldName);
    }

    @Override
    public void writeDoubleArray(@Nonnull String fieldName, double[] values) {
        builder.addDoubleArrayField(fieldName);
    }

    @Override
    public void writeFloatArray(@Nonnull String fieldName, float[] values) {
        builder.addFloatArrayField(fieldName);
    }

    @Override
    public void writeShortArray(@Nonnull String fieldName, short[] values) {
        builder.addShortArrayField(fieldName);
    }

    @Override
    public void writeUTFArray(@Nonnull String fieldName, @Nullable String[] values) {
        writeStringArray(fieldName, values);
    }

    @Override
    public void writeStringArray(@Nonnull String fieldName, @Nullable String[] values) {
        builder.addStringArrayField(fieldName);
    }

    @Override
    public void writePortable(@Nonnull String fieldName, @Nullable Portable portable) throws IOException {
        if (portable == null) {
            throw new HazelcastSerializationException("Cannot write null portable without explicitly "
                    + "registering class definition!");
        }
        int version = SerializationUtil.getPortableVersion(portable, context.getVersion());
        ClassDefinition nestedClassDef = createNestedClassDef(portable,
                new ClassDefinitionBuilder(portable.getFactoryId(), portable.getClassId(), version));
        builder.addPortableField(fieldName, nestedClassDef);
    }

    @Override
    public void writeNullPortable(@Nonnull String fieldName, int factoryId, int classId) {
        final ClassDefinition nestedClassDef = context.lookupClassDefinition(factoryId, classId, context.getVersion());
        if (nestedClassDef == null) {
            throw new HazelcastSerializationException("Cannot write null portable without explicitly "
                    + "registering class definition!");
        }
        builder.addPortableField(fieldName, nestedClassDef);
    }

    @Override
    public void writeDecimal(@Nonnull String fieldName, @Nullable BigDecimal value) {
        builder.addDecimalField(fieldName);
    }

    @Override
    public void writeTime(@Nonnull String fieldName, @Nullable LocalTime value) {
        builder.addTimeField(fieldName);
    }

    @Override
    public void writeDate(@Nonnull String fieldName, @Nullable LocalDate value) {
        builder.addDateField(fieldName);
    }

    @Override
    public void writeTimestamp(@Nonnull String fieldName, @Nullable LocalDateTime value) {
        builder.addTimestampField(fieldName);
    }

    @Override
    public void writeTimestampWithTimezone(@Nonnull String fieldName, @Nullable OffsetDateTime value) {
        builder.addTimestampWithTimezoneField(fieldName);
    }

    @Override
    public void writePortableArray(@Nonnull String fieldName, Portable[] portables) throws IOException {
        if (portables == null || portables.length == 0) {
            throw new HazelcastSerializationException("Cannot write null portable array without explicitly "
                    + "registering class definition!");
        }
        Portable p = portables[0];
        int classId = p.getClassId();
        for (int i = 1; i < portables.length; i++) {
            if (portables[i].getClassId() != classId) {
                throw new IllegalArgumentException("Detected different class-ids in portable array!");
            }
        }
        int version = SerializationUtil.getPortableVersion(p, context.getVersion());
        ClassDefinition nestedClassDef = createNestedClassDef(p,
                new ClassDefinitionBuilder(p.getFactoryId(), classId, version));
        builder.addPortableArrayField(fieldName, nestedClassDef);
    }

    @Override
    public void writeDecimalArray(@Nonnull String fieldName, @Nullable BigDecimal[] values) {
        builder.addDecimalArrayField(fieldName);
    }

    @Override
    public void writeTimeArray(@Nonnull String fieldName, @Nullable LocalTime[] values) {
        builder.addTimeArrayField(fieldName);
    }

    @Override
    public void writeDateArray(@Nonnull String fieldName, @Nullable LocalDate[] values) {
        builder.addDateArrayField(fieldName);
    }

    @Override
    public void writeTimestampArray(@Nonnull String fieldName, @Nullable LocalDateTime[] values) {
        builder.addTimestampArrayField(fieldName);
    }

    @Override
    public void writeTimestampWithTimezoneArray(@Nonnull String fieldName, @Nullable OffsetDateTime[] values) {
        builder.addTimestampWithTimezoneArrayField(fieldName);
    }

    @Override
    @Nonnull
    public ObjectDataOutput getRawDataOutput() {
        return new EmptyObjectDataOutput();
    }

    private ClassDefinition createNestedClassDef(Portable portable, ClassDefinitionBuilder nestedBuilder)
            throws IOException {
        ClassDefinitionWriter writer = new ClassDefinitionWriter(context, nestedBuilder);
        portable.writePortable(writer);
        return context.registerClassDefinition(nestedBuilder.build());
    }

    ClassDefinition registerAndGet() {
        final ClassDefinition cd = builder.build();
        return context.registerClassDefinition(cd);
    }
}
