/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class CompactUpsertTargetDescriptor implements UpsertTargetDescriptor {
    private String typeName;
    private Map<String, Schema> schemas;

    @SuppressWarnings("unused")
    private CompactUpsertTargetDescriptor() { }

    public CompactUpsertTargetDescriptor(@Nonnull String typeName, @Nonnull Map<String, Schema> schemas) {
        this.typeName = typeName;
        this.schemas = schemas;
    }

    @Override
    public UpsertTarget create(InternalSerializationService serializationService) {
        return new CompactUpsertTarget(typeName, schemas, serializationService);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(typeName);
        SerializationUtil.writeMap(schemas, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        typeName = in.readString();
        schemas = SerializationUtil.readMap(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactUpsertTargetDescriptor that = (CompactUpsertTargetDescriptor) o;
        return typeName.equals(that.typeName)
                && schemas.equals(that.schemas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, schemas);
    }

    @Override
    public String toString() {
        return "CompactUpsertTargetDescriptor{"
                + "typeName=" + typeName
                + ", schemas=" + schemas
                + '}';
    }
}
