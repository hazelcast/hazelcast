/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.*;

/**
 * @mdogan 12/26/12
 */
public class ClassDefinitionImpl implements DataSerializable, ClassDefinition {
    int classId;
    int version;
    private List<FieldDefinition> fieldDefinitions = new ArrayList<FieldDefinition>();
    private Map<String, FieldDefinition> fieldDefinitionsMap = new HashMap<String,
            FieldDefinition>();
    private Set<ClassDefinition> nestedClassDefinitions = new HashSet<ClassDefinition>();

    private byte[] binary;

    public void add(FieldDefinitionImpl fd) {
        fieldDefinitions.add(fd);
        fieldDefinitionsMap.put(fd.fieldName, fd);
    }

    public void add(ClassDefinitionImpl cd) {
        nestedClassDefinitions.add(cd);
    }

    public FieldDefinition get(String name) {
        return fieldDefinitionsMap.get(name);
    }

    public FieldDefinition get(int fieldIndex) {
        return fieldDefinitions.get(fieldIndex);
    }

    public Set<ClassDefinition> getNestedClassDefinitions() {
        return nestedClassDefinitions;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(classId);
        out.writeInt(version);
        out.writeInt(fieldDefinitions.size());
        for (FieldDefinition fieldDefinition : fieldDefinitions) {
            fieldDefinition.writeData(out);
        }
        out.writeInt(nestedClassDefinitions.size());
        for (ClassDefinition classDefinition : nestedClassDefinitions) {
            classDefinition.writeData(out);
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        classId = in.readInt();
        version = in.readInt();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            FieldDefinitionImpl fieldDefinition = new FieldDefinitionImpl();
            fieldDefinition.readData(in);
            add(fieldDefinition);
        }
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            ClassDefinitionImpl classDefinition = new ClassDefinitionImpl();
            classDefinition.readData(in);
            add(classDefinition);
        }
    }

    public int getFieldCount() {
        return fieldDefinitions.size();
    }

    public byte[] getBinary() {
        return binary;
    }

    public int getClassId() {
        return classId;
    }

    public int getVersion() {
        return version;
    }

    void setBinary(byte[] binary) {
        this.binary = binary;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClassDefinitionImpl that = (ClassDefinitionImpl) o;

        if (classId != that.classId) return false;
        if (version != that.version) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = classId;
        result = 31 * result + version;
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ClassDefinition");
        sb.append("{classId=").append(classId);
        sb.append(", version=").append(version);
        sb.append(", fieldDefinitions=").append(fieldDefinitions);
        sb.append('}');
        return sb.toString();
    }
}
