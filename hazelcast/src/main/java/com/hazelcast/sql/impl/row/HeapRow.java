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

package com.hazelcast.sql.impl.row;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QuerySerializationHook;

import java.io.IOException;
import java.util.Arrays;

/**
 * Row with values stored on heap.
 */
public class HeapRow implements Row, IdentifiedDataSerializable {
    /** Row values. */
    private Object[] values;

    public HeapRow() {
        // No-op.
    }

    public HeapRow(int length) {
        this.values = new Object[length];
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getColumn(int idx) {
        return (T) values[idx];
    }

    @Override
    public int getColumnCount() {
        return values.length;
    }

    public void set(int idx, Object val) {
        values[idx] = val;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HeapRow heapRow = (HeapRow) o;

        return Arrays.equals(values, heapRow.values);
    }

    @Override
    public int getFactoryId() {
        return QuerySerializationHook.F_ID;
    }

    @Override
    public int getClassId() {
        return QuerySerializationHook.ROW_HEAP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(values.length);

        // TODO: Serializing wrapped objects will be slow and space-inefficient. How to mitigate it?
        // TODO: Handle serialization errors.
        for (Object value : values) {
            out.writeObject(value);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();

        values = new Object[len];

        // TODO: Handle deserialization errors: cancel query!
        for (int i = 0; i < len; i++) {
            values[i] = in.readObject();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + Arrays.toString(values) + '}';
    }
}
