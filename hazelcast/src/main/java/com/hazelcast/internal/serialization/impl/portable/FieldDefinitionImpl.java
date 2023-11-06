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
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.PortableId;

import java.io.IOException;
import java.util.Objects;

import static com.hazelcast.internal.serialization.SerializableByConvention.Reason.PUBLIC_API;

@SerializableByConvention(PUBLIC_API)
public class FieldDefinitionImpl implements FieldDefinition, DataSerializable {
    private int index;
    private String fieldName;
    private FieldType type;
    private PortableId portableId;

    @SuppressWarnings("unused")
    private FieldDefinitionImpl() { }

    public FieldDefinitionImpl(int index, String fieldName, FieldType type, int version) {
        this(index, fieldName, type, 0, 0, version);
    }

    public FieldDefinitionImpl(int index, String fieldName, FieldType type, int factoryId, int classId, int version) {
        this(index, fieldName, type, new PortableId(factoryId, classId, version));
    }

    public FieldDefinitionImpl(int index, String fieldName, FieldType type, PortableId portableId) {
        this.index = index;
        this.fieldName = fieldName;
        this.type = type;
        this.portableId = portableId;
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
        return portableId.getFactoryId();
    }

    @Override
    public int getClassId() {
        return portableId.getClassId();
    }

    @Override
    public int getVersion() {
        return portableId.getVersion();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(index);
        out.writeString(fieldName);
        out.writeByte(type.getId());
        portableId.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        index = in.readInt();
        fieldName = in.readString();
        type = FieldType.get(in.readByte());
        portableId = new PortableId();
        portableId.readData(in);
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
        return index == that.index
                && Objects.equals(fieldName, that.fieldName)
                && type == that.type
                && portableId.equals(that.portableId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, fieldName, type, portableId);
    }

    @Override
    public String toString() {
        return "FieldDefinitionImpl{"
                + "index=" + index
                + ", fieldName='" + fieldName + '\''
                + ", type=" + type
                + ", factoryId=" + portableId.getFactoryId()
                + ", classId=" + portableId.getClassId()
                + ", version=" + portableId.getVersion()
                + '}';
    }
}
