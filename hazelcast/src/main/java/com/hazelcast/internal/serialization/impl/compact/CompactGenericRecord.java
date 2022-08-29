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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.internal.json.JsonEscape;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.serialization.impl.AbstractGenericRecord;
import com.hazelcast.nio.serialization.FieldKind;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An extension of the {@link AbstractGenericRecord} that requires the
 * implementors to provide a {@link Schema} that represents the
 * Compact serialized objects.
 * <p>
 * See the javadoc of {@link InternalGenericRecord} for GenericRecord class hierarchy.
 */
public abstract class CompactGenericRecord extends AbstractGenericRecord {

    /**
     * Returns the schema associated with this GenericRecord.
     */
    public abstract Schema getSchema();

    /**
     * WARNING: This method should only be used if `fd` is known to be a valid field descriptor in this generic record
     * and its kind is {@link FieldKind#COMPACT}.
     */
    @Nullable
    public abstract <T> T getObject(@Nonnull FieldDescriptor fd);

    @Nonnull
    public abstract FieldDescriptor getFieldDescriptor(@Nonnull String fieldName);

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append('{');
        JsonEscape.writeEscaped(stringBuilder, getSchema().getTypeName());
        stringBuilder.append(": ");
        writeFieldsToStringBuilder(stringBuilder);
        stringBuilder.append('}');
        return stringBuilder.toString();
    }
}
