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

package com.hazelcast.config;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class FunctionArgument implements IdentifiedDataSerializable {
    private String fieldName;
    private String typeName;
    private Boolean isArray;
    private String[] values;

    public FunctionArgument() { }

    public FunctionArgument(
            final String fieldName,
            final String typeName,
            final Boolean isArray,
            final String[] values
    ) {
        this.fieldName = fieldName;
        this.typeName = typeName;
        this.isArray = isArray;
        this.values = values;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(final String fieldName) {
        this.fieldName = fieldName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(final String typeName) {
        this.typeName = typeName;
    }

    public Boolean isArray() {
        return isArray;
    }

    public void setArray(final Boolean array) {
        isArray = array;
    }

    public String[] getValues() {
        return values;
    }

    public void setValues(final String[] values) {
        this.values = values;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.FUNCTION_ARGUMENT;
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeObject(typeName);
        out.writeBoolean(isArray);
        out.writeInt(values.length);
        for (final Object value : values) {
            out.writeObject(value);
        }
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        this.fieldName = in.readString();
        this.typeName = in.readObject();
        this.isArray = in.readBoolean();
        final int size = in.readInt();
        this.values = new String[size];
        for (int i = 0; i < size; i++) {
            this.values[i] = in.readString();
        }
    }
}
