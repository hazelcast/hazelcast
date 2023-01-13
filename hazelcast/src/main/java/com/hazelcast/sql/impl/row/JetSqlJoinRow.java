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

package com.hazelcast.sql.impl.row;

import com.hazelcast.jet.core.JetDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Special row for handling nested loops outer joins aggregation
 */
public class JetSqlJoinRow extends JetSqlRow {

    private int processorIndex;
    private long rowId;
    private boolean matched;

    // for deserialization
    public JetSqlJoinRow() { }

    public JetSqlJoinRow(JetSqlRow row, int processorIndex, long rowId, boolean matched) {
        super(row.getSerializationService(), row.getValues());
        this.processorIndex = processorIndex;
        this.rowId = rowId;
        this.matched = matched;
    }

    public int getProcessorIndex() {
        return processorIndex;
    }

    public long getRowId() {
        return rowId;
    }

    public boolean isMatched() {
        return matched;
    }

    @Override
    public boolean equals(Object o) {
        JetSqlJoinRow jetSqlRow = (JetSqlJoinRow) o;
        return super.equals(o)
                && processorIndex == jetSqlRow.processorIndex
                && rowId == jetSqlRow.rowId
                && matched == jetSqlRow.matched;
    }

    @Override
    public int hashCode() {
        // This is a dummy value that will not break the contract, but the object is not supposed
        // to be used as a hash map key. Ideally we would throw an UOE here, but we use the instances
        // as a map key in tests
        return 0;
    }

    @Override
    public String toString() {
        return "[row=" + super.toString()
                + ", processorIndex=" + processorIndex
                + ", rowId=" + rowId
                + ", matched=" + matched
                + "]";
    }

    @Override
    public int getFactoryId() {
        return JetDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetDataSerializerHook.JET_SQL_JOIN_ROW;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(processorIndex);
        out.writeLong(rowId);
        out.writeBoolean(matched);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        processorIndex = in.readInt();
        rowId = in.readLong();
        matched = in.readBoolean();
    }

}
