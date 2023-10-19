/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.PortableId;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.serialization.SerializableByConvention.Reason.PUBLIC_API;

@SerializableByConvention(PUBLIC_API)
public class ClassDefinitionImpl implements ClassDefinition, DataSerializable {
    private PortableId portableId;
    private Map<String, FieldDefinition> fieldDefinitionsMap;

    @SuppressWarnings("unused")
    private ClassDefinitionImpl() { }

    public ClassDefinitionImpl(PortableId portableId) {
        this.portableId = portableId;
        fieldDefinitionsMap = new LinkedHashMap<>();
    }

    public void addFieldDef(FieldDefinitionImpl fd) {
        fieldDefinitionsMap.put(fd.getName(), fd);
    }

    @Override
    public FieldDefinition getField(String name) {
        return fieldDefinitionsMap.get(name);
    }

    @Override
    public FieldDefinition getField(int fieldIndex) {
        if (fieldIndex < 0 || fieldIndex >= fieldDefinitionsMap.size()) {
            throw new IndexOutOfBoundsException("Index: " + fieldIndex + ", Size: " + fieldDefinitionsMap.size());
        }
        for (FieldDefinition fieldDefinition : fieldDefinitionsMap.values()) {
            if (fieldIndex == fieldDefinition.getIndex()) {
                return fieldDefinition;
            }
        }
        throw new IndexOutOfBoundsException("Index: " + fieldIndex + ", Size: " + fieldDefinitionsMap.size());
    }

    @Override
    public boolean hasField(String fieldName) {
        return fieldDefinitionsMap.containsKey(fieldName);
    }

    @Override
    public Set<String> getFieldNames() {
        return Collections.unmodifiableSet(fieldDefinitionsMap.keySet());
    }

    @Override
    public FieldType getFieldType(String fieldName) {
        final FieldDefinition fd = getField(fieldName);
        if (fd != null) {
            return fd.getType();
        }
        throw new IllegalArgumentException("Unknown field: " + fieldName);
    }

    @Override
    public int getFieldClassId(String fieldName) {
        final FieldDefinition fd = getField(fieldName);
        if (fd != null) {
            return fd.getClassId();
        }
        throw new IllegalArgumentException("Unknown field: " + fieldName);
    }

    @Override
    public int getFieldCount() {
        return fieldDefinitionsMap.size();
    }

    @Override
    public final int getFactoryId() {
        return portableId.getFactoryId();
    }

    @Override
    public final int getClassId() {
        return portableId.getClassId();
    }

    @Override
    public final int getVersion() {
        return portableId.getVersion();
    }

    @Override
    public PortableId getPortableId() {
        return portableId;
    }

    void setVersionIfNotSet(int version) {
        portableId.setVersionIfNotSet(version);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        portableId.writeData(out);
        out.writeObject(fieldDefinitionsMap);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        portableId = new PortableId();
        portableId.readData(in);
        fieldDefinitionsMap = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClassDefinitionImpl that = (ClassDefinitionImpl) o;
        return portableId.equals(that.portableId)
                && fieldDefinitionsMap.equals(that.fieldDefinitionsMap);
    }

    @Override
    public int hashCode() {
        return portableId.hashCode();
    }

    @Override
    public String toString() {
        return "ClassDefinition{"
                + "factoryId=" + portableId.getFactoryId()
                + ", classId=" + portableId.getClassId()
                + ", version=" + portableId.getVersion()
                + ", fieldDefinitions=" + fieldDefinitionsMap.values()
                + '}';
    }
}
