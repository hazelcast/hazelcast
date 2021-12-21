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
    public Byte getNullableInt8(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Double getNullableFloat64(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Float getNullableFloat32(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Integer getNullableInt32(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Long getNullableInt64(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Short getNullableInt16(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Boolean[] getArrayOfNullableBooleans(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Byte[] getArrayOfNullableInt8s(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Double[] getArrayOfNullableFloat64s(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Float[] getArrayOfNullableFloat32s(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Integer[] getArrayOfNullableInt32s(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Long[] getArrayOfNullableInt64s(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Short[] getArrayOfNullableInt16s(@Nonnull String fieldName) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Byte getNullableInt8FromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Boolean getNullableBooleanFromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Short getNullableInt16FromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Integer getNullableInt32FromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Long getNullableInt64FromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Float getNullableFloat32FromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Double getNullableFloat64FromArray(@Nonnull String fieldName, int index) {
        throw new UnsupportedOperationException();
    }
}
