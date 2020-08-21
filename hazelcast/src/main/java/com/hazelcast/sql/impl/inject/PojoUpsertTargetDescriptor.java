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

package com.hazelcast.sql.impl.inject;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class PojoUpsertTargetDescriptor implements UpsertTargetDescriptor, IdentifiedDataSerializable {

    private String className;
    private Map<String, String> typeNamesByPaths;

    @SuppressWarnings("unused")
    public PojoUpsertTargetDescriptor() {
    }

    public PojoUpsertTargetDescriptor(String className, Map<String, String> typeNamesByPaths) {
        this.className = className;
        this.typeNamesByPaths = typeNamesByPaths;
    }

    public String getClassName() {
        return className;
    }

    public Map<String, String> getTypeNamesByPaths() {
        return typeNamesByPaths;
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.TARGET_DESCRIPTOR_POJO;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(className);
        out.writeObject(typeNamesByPaths);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        className = in.readUTF();
        typeNamesByPaths = in.readObject();
    }

    @Override
    public String toString() {
        return "PojoUpsertTargetDescriptor{"
                + "className='" + className + '\''
                + ", typeNamesByPaths=" + typeNamesByPaths
                + '}';
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
        return Objects.equals(className, that.className)
                && Objects.equals(typeNamesByPaths, that.typeNamesByPaths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(className, typeNamesByPaths);
    }
}
