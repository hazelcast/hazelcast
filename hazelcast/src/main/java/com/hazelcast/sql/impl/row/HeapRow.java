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

package com.hazelcast.sql.impl.row;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public HeapRow(Object[] values) {
        assert values != null;

        this.values = values;
    }

    public static HeapRow of(Object... values) {
        assert values != null;

        return new HeapRow(values);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(int index) {
        return (T) values[index];
    }

    @Override
    public int getColumnCount() {
        return values.length;
    }

    public void set(int index, Object val) {
        values[index] = val;
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.ROW_HEAP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(values.length);

        for (Object value : values) {
            out.writeObject(value);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();

        values = new Object[len];

        for (int i = 0; i < len; i++) {
            values[i] = in.readObject();
        }
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
}
