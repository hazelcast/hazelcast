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
import com.hazelcast.sql.impl.SqlDataSerializerHook;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Batch where rows are organized in a list.
 */
public class ListRowBatch implements RowBatch, IdentifiedDataSerializable {
    /** Rows. */
    private List<Row> rows;

    public ListRowBatch() {
        // No-op.
    }

    public ListRowBatch(List<Row> rows) {
        this.rows = rows;
    }

    @Override
    public Row getRow(int index) {
        assert index >= 0 && index < rows.size() : index;

        return rows.get(index);
    }

    @Override
    public int getRowCount() {
        return rows.size();
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.ROW_BATCH_LIST;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(rows.size());

        for (Row row : rows) {
            out.writeObject(row);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();

        rows = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            rows.add(in.readObject());
        }
    }
}
