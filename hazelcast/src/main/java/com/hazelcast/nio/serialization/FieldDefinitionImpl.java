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

/**
 * @mdogan 12/26/12
 */
class FieldDefinitionImpl implements DataSerializable, FieldDefinition {

    int index;
    String fieldName;
    byte dataType;
    int classId = -1;

    FieldDefinitionImpl() {
    }

    FieldDefinitionImpl(int index, String fieldName, byte dataType) {
        this.index = index;
        this.fieldName = fieldName;
        this.dataType = dataType;
    }

    FieldDefinitionImpl(int index, String fieldName, byte dataType, int classId) {
        this.classId = classId;
        this.dataType = dataType;
        this.fieldName = fieldName;
        this.index = index;
    }

    public byte getDataType() {
        return dataType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public int getIndex() {
        return index;
    }

    public int getClassId() {
        return classId;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(index);
        out.writeUTF(fieldName);
        out.writeByte(dataType);
        out.writeInt(classId);
    }

    public void readData(ObjectDataInput in) throws IOException {
        index = in.readInt();
        fieldName = in.readUTF();
        dataType = in.readByte();
        classId = in.readInt();
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("FieldDefinition");
        sb.append("{dataType=").append(dataType);
        sb.append(", index=").append(index);
        sb.append(", fieldName='").append(fieldName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
