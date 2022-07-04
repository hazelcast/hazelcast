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
import java.util.Map;
import java.util.Objects;

public class PojoUpsertTargetDescriptor implements UpsertTargetDescriptor {

    private String className;
    private Map<String, String> typeNamesByPaths;

    @SuppressWarnings("unused")
    private PojoUpsertTargetDescriptor() {
    }

    public PojoUpsertTargetDescriptor(String className, Map<String, String> typeNamesByPaths) {
        this.className = className;
        this.typeNamesByPaths = typeNamesByPaths;
    }

    @Override
    public UpsertTarget create(InternalSerializationService serializationService) {
        return new PojoUpsertTarget(className, typeNamesByPaths);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(className);
        out.writeObject(typeNamesByPaths);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        className = in.readString();
        typeNamesByPaths = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PojoUpsertTargetDescriptor that = (PojoUpsertTargetDescriptor) o;
        return Objects.equals(className, that.className) && Objects.equals(typeNamesByPaths, that.typeNamesByPaths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(className, typeNamesByPaths);
    }
}
