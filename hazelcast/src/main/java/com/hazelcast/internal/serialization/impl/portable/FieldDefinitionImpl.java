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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;

public class FieldDefinitionImpl implements FieldDefinition {

    private final int index;
    private final String fieldName;
    private final FieldType type;
    private final int classId;
    private final int factoryId;
    private final int version;

    public FieldDefinitionImpl(int index, String fieldName, FieldType type, int version) {
        this(index, fieldName, type, 0, 0, version);
    }

    public FieldDefinitionImpl(int index, String fieldName, FieldType type, int factoryId, int classId, int version) {
        this.classId = classId;
        this.type = type;
        this.fieldName = fieldName;
        this.index = index;
        this.factoryId = factoryId;
        this.version = version;
    }

    @Override
    public FieldType getType() {
        return type;
    }

    @Override
    public String getName() {
        return fieldName;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public int getFactoryId() {
        return factoryId;
    }

    @Override
    public int getClassId() {
        return classId;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FieldDefinitionImpl that = (FieldDefinitionImpl) o;
        if (index != that.index) {
            return false;
        }
        if (classId != that.classId) {
            return false;
        }
        if (factoryId != that.factoryId) {
            return false;
        }
        if (version != that.version) {
            return false;
        }
        if (fieldName != null ? !fieldName.equals(that.fieldName) : that.fieldName != null) {
            return false;
        }
        return type == that.type;
    }

    @Override
    public int hashCode() {
        int result = index;
        result = 31 * result + (fieldName != null ? fieldName.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + classId;
        result = 31 * result + factoryId;
        result = 31 * result + version;
        return result;
    }

    @Override
    public String toString() {
        return "FieldDefinitionImpl{"
                + "index=" + index
                + ", fieldName='" + fieldName + '\''
                + ", type=" + type
                + ", classId=" + classId
                + ", factoryId=" + factoryId
                + ", version=" + version
                + '}';
    }
}
