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

package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.internal.serialization.SerializableByConvention;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;

import java.io.IOException;

import static com.hazelcast.internal.serialization.SerializableByConvention.Reason.PUBLIC_API;

@SerializableByConvention(PUBLIC_API)
public class FieldDefinitionImpl implements FieldDefinition, DataSerializable {

    private int index;
    private String fieldName;
    private FieldType type;
    private int factoryId;
    private int classId;
    private int version;

    @SuppressWarnings("unused")
    private FieldDefinitionImpl() {
    }

    public FieldDefinitionImpl(int index, String fieldName, FieldType type, int version) {
        this(index, fieldName, type, 0, 0, version);
    }

    public FieldDefinitionImpl(int index, String fieldName, FieldType type, int factoryId, int classId, int version) {
        this.type = type;
        this.fieldName = fieldName;
        this.index = index;
        this.factoryId = factoryId;
        this.classId = classId;
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
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(index);
        out.writeString(fieldName);
        out.writeByte(type.getId());
        out.writeInt(factoryId);
        out.writeInt(classId);
        out.writeInt(version);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        index = in.readInt();
        fieldName = in.readString();
        type = FieldType.get(in.readByte());
        factoryId = in.readInt();
        classId = in.readInt();
        version = in.readInt();
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
        if (factoryId != that.factoryId) {
            return false;
        }
        if (classId != that.classId) {
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
        result = 31 * result + factoryId;
        result = 31 * result + classId;
        result = 31 * result + version;
        return result;
    }

    @Override
    public String toString() {
        return "FieldDefinitionImpl{"
                + "index=" + index
                + ", fieldName='" + fieldName + '\''
                + ", type=" + type
                + ", factoryId=" + factoryId
                + ", classId=" + classId
                + ", version=" + version
                + '}';
    }
}
