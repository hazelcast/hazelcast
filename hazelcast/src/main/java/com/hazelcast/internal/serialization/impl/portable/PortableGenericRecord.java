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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.nio.serialization.AbstractGenericRecord;
import com.hazelcast.nio.serialization.ClassDefinition;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class PortableGenericRecord extends AbstractGenericRecord implements InternalGenericRecord {

    /**
     * Returns the schema associated with this GenericRecord.
     */
    public abstract ClassDefinition getClassDefinition();

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("{\"Portable\": ");
        writeFieldsToStringBuilder(stringBuilder);
        stringBuilder.append('}');
        return stringBuilder.toString();
    }

    @Nullable
    @Override
    public Boolean getNullableBoolean(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Byte getNullableByte(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Double getNullableDouble(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Float getNullableFloat(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Integer getNullableInt(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Long getNullableLong(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Short getNullableShort(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Boolean[] getArrayOfNullableBooleans(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Byte[] getArrayOfNullableBytes(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Double[] getArrayOfNullableDoubles(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Float[] getArrayOfNullableFloats(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Integer[] getArrayOfNullableInts(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Long[] getArrayOfNullableLongs(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Short[] getArrayOfNullableShorts(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Byte getNullableByteFromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Boolean getNullableBooleanFromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Short getNullableShortFromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Integer getNullableIntFromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Long getNullableLongFromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Float getNullableFloatFromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Double getNullableDoubleFromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }
}
