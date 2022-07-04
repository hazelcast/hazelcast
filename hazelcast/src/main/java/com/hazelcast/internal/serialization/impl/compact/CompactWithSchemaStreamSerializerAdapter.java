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

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.impl.SerializerAdapter;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;

import java.io.IOException;
import java.util.Objects;

import static com.hazelcast.internal.serialization.impl.SerializationConstants.TYPE_COMPACT_WITH_SCHEMA;

/**
 * SerializerAdapter for Compact format where the schema is included in the data.
 */
public class CompactWithSchemaStreamSerializerAdapter implements SerializerAdapter {

    private final CompactStreamSerializer serializer;

    public CompactWithSchemaStreamSerializerAdapter(CompactStreamSerializer compactStreamSerializer) {
        this.serializer = compactStreamSerializer;
    }

    @Override
    public void write(ObjectDataOutput out, Object object) throws IOException {
        serializer.write((BufferObjectDataOutput) out, object, true);
    }

    @Override
    public Object read(ObjectDataInput in) throws IOException {
        return serializer.read((BufferObjectDataInput) in, true);
    }

    @Override
    public int getTypeId() {
        return TYPE_COMPACT_WITH_SCHEMA;
    }

    @Override
    public void destroy() {
        serializer.destroy();
    }

    @Override
    public Serializer getImpl() {
        return serializer;
    }

    @Override
    public String toString() {
        return "CompactWithSchemaStreamSerializerAdapter{serializer=" + serializer + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CompactWithSchemaStreamSerializerAdapter that = (CompactWithSchemaStreamSerializerAdapter) o;

        return Objects.equals(serializer, that.serializer);
    }

    @Override
    public int hashCode() {
        return serializer != null ? serializer.hashCode() : 0;
    }
}
