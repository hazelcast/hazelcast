/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ClassDefinitionImpl extends BinaryClassDefinition implements ClassDefinition {

    private final List<FieldDefinition> fieldDefinitions = new ArrayList<FieldDefinition>();
    private final Map<String, FieldDefinition> fieldDefinitionsMap = new HashMap<String,
            FieldDefinition>();
    private final Set<ClassDefinition> nestedClassDefinitions = new HashSet<ClassDefinition>();

    public ClassDefinitionImpl() {
    }

    public ClassDefinitionImpl(int factoryId, int classId, int version) {
        this.factoryId = factoryId;
        this.classId = classId;
        this.version = version;
    }

    public void addFieldDef(FieldDefinition fd) {
        fieldDefinitions.add(fd);
        fieldDefinitionsMap.put(fd.getName(), fd);
    }

    public void addClassDef(ClassDefinition cd) {
        nestedClassDefinitions.add(cd);
    }

    public FieldDefinition getField(String name) {
        return fieldDefinitionsMap.get(name);
    }

    public FieldDefinition getField(int fieldIndex) {
        return fieldDefinitions.get(fieldIndex);
    }

    public Set<ClassDefinition> getNestedClassDefinitions() {
        return nestedClassDefinitions;
    }

    public boolean hasField(String fieldName) {
        return fieldDefinitionsMap.containsKey(fieldName);
    }

    public Set<String> getFieldNames() {
        return new HashSet<String>(fieldDefinitionsMap.keySet());
    }

    public FieldType getFieldType(String fieldName) {
        final FieldDefinition fd = getField(fieldName);
        if (fd != null) {
            return fd.getType();
        }
        throw new IllegalArgumentException("Unknown field: " + fieldName);
    }

    public int getFieldClassId(String fieldName) {
        final FieldDefinition fd = getField(fieldName);
        if (fd != null) {
            return fd.getClassId();
        }
        throw new IllegalArgumentException("Unknown field: " + fieldName);
    }

    public int getFieldVersion(String fieldName) {
        final FieldDefinition fd = getField(fieldName);
        if (fd != null) {
            return fd.getVersion();
        }
        throw new IllegalArgumentException("Unknown field: " + fieldName);
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(factoryId);
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
        factoryId = in.readInt();
        classId = in.readInt();
        version = in.readInt();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            FieldDefinitionImpl fieldDefinition = new FieldDefinitionImpl();
            fieldDefinition.readData(in);
            addFieldDef(fieldDefinition);
        }
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            ClassDefinitionImpl classDefinition = new ClassDefinitionImpl();
            classDefinition.readData(in);
            addClassDef(classDefinition);
        }
    }

    @Override
    public int getFieldCount() {
        return fieldDefinitions.size();
    }

    void setVersionIfNotSet(int version) {
        if (getVersion() < 0) {
            this.version = version;
            for (FieldDefinition fd : fieldDefinitions) {
                ((FieldDefinitionImpl) fd).setVersionIfNotSet(version);
            }
        }
    }

    //CHECKSTYLE:OFF
    //Generated equals method has too high NPath Complexity
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClassDefinitionImpl that = (ClassDefinitionImpl) o;

        if (classId != that.classId) {
            return false;
        }
        if (version != that.version) {
            return false;
        }
        if (getFieldCount() != that.getFieldCount()) {
            return false;
        }
        for (FieldDefinition fd : fieldDefinitions) {
            FieldDefinition fd2 = that.getField(fd.getName());
            if (fd2 == null) {
                return false;
            }
            if (!fd.equals(fd2)) {
                return false;
            }
        }

        return true;
    }
    //CHECKSTYLE:ON

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
        sb.append("{factoryId=").append(factoryId);
        sb.append(", classId=").append(classId);
        sb.append(", version=").append(version);
        sb.append(", fieldDefinitions=").append(fieldDefinitions);
        sb.append('}');
        return sb.toString();
    }
}
