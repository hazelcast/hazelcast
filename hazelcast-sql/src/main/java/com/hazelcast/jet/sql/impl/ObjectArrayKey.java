/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.sql.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.row.JetSqlRow;

import java.io.IOException;
import java.util.Arrays;

/**
 * A wrapper for {@code Data[]} supporting equals/hashCode and hz-serialization.
 */
public final class ObjectArrayKey implements DataSerializable {

    private Data[] keyFields;

    @SuppressWarnings("unused")
    private ObjectArrayKey() {
    }

    private ObjectArrayKey(Data[] keyFields) {
        this.keyFields = keyFields;
    }

    public boolean containsNull() {
        for (Object o : keyFields) {
            if (o == null) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(keyFields.length);
        for (Data field : keyFields) {
            IOUtil.writeData(out, field);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        keyFields = new Data[size];
        for (int i = 0; i < size; i++) {
            keyFields[i] = IOUtil.readData(in);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ObjectArrayKey that = (ObjectArrayKey) o;
        return Arrays.equals(keyFields, that.keyFields);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(keyFields);
    }

    public static ObjectArrayKey project(JetSqlRow row, int[] indices) {
        Data[] key = new Data[indices.length];
        for (int i = 0; i < indices.length; i++) {
            key[i] = row.getSerialized(indices[i]);
        }
        return new ObjectArrayKey(key);
    }

    /**
     * Return a function that maps an input {@link JetSqlRow} to an {@link
     * ObjectArrayKey}, extracting the fields given in {@code indices}.
     *
     * @param indices the indices of keys
     * @return the projection function
     */
    public static FunctionEx<JetSqlRow, ObjectArrayKey> projectFn(int[] indices) {
        return row -> project(row, indices);
    }
}
