/*
 * Copyright 2021 Hazelcast Inc.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Objects;

public final class AvroUpsertTargetDescriptor implements UpsertTargetDescriptor {

    private String schema;

    @SuppressWarnings("unused")
    private AvroUpsertTargetDescriptor() {
    }

    public AvroUpsertTargetDescriptor(String schema) {
        this.schema = schema;
    }

    @Override
    public UpsertTarget create(InternalSerializationService serializationService) {
        return new AvroUpsertTarget(schema);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(schema);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        schema = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AvroUpsertTargetDescriptor that = (AvroUpsertTargetDescriptor) o;
        return Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema);
    }
}
