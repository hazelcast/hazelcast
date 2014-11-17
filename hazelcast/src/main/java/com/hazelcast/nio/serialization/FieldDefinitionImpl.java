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

class FieldDefinitionImpl implements FieldDefinition {

    int index;
    String fieldName;
    FieldType type;
    int classId;
    int factoryId;

    FieldDefinitionImpl() {
    }

    FieldDefinitionImpl(int index, String fieldName, FieldType type) {
        this(index, fieldName, type, 0, 0);
    }

    FieldDefinitionImpl(int index, String fieldName, FieldType type, int factoryId, int classId) {
        this.classId = classId;
        this.type = type;
        this.fieldName = fieldName;
        this.index = index;
        this.factoryId = factoryId;
    }

    public FieldType getType() {
        return type;
    }

    public String getName() {
        return fieldName;
    }

    public int getIndex() {
        return index;
    }

    public int getFactoryId() {
        return factoryId;
    }

    public int getClassId() {
        return classId;
    }

    boolean isPortable() {
        return type == FieldType.PORTABLE || type == FieldType.PORTABLE_ARRAY;
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

        FieldDefinitionImpl that = (FieldDefinitionImpl) o;

        if (classId != that.classId) {
            return false;
        }
        if (factoryId != that.factoryId) {
            return false;
        }

        if (fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null) {
            return false;
        }
        if (type != that.type) {
            return false;
        }

        return true;
    }
    //CHECKSTYLE:ON

    @Override
    public int hashCode() {
        int result = fieldName != null ? fieldName.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + classId;
        result = 31 * result + factoryId;
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FieldDefinitionImpl{");
        sb.append("index=").append(index);
        sb.append(", fieldName='").append(fieldName).append('\'');
        sb.append(", type=").append(type);
        sb.append(", classId=").append(classId);
        sb.append(", factoryId=").append(factoryId);
        sb.append('}');
        return sb.toString();
    }
}
