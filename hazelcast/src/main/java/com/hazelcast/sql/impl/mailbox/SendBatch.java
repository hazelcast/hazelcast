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

package com.hazelcast.sql.impl.mailbox;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.row.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class SendBatch implements DataSerializable {
    /** Rows being transferred. */
    private List<Row> rows;

    /** Remaining memory on the receiver side before it is blocked. */
    private long remainingMemory;

    /** Left batch marker. */
    private boolean last;

    /** ID of sending member. */
    private transient UUID senderId;

    public SendBatch() {
        // No-op.
    }

    public SendBatch(List<Row> rows, long remainingMemory, boolean last) {
        this.rows = rows;
        this.remainingMemory = remainingMemory;
        this.last = last;
    }

    public List<Row> getRows() {
        return rows;
    }

    public long getRemainingMemory() {
        return remainingMemory;
    }

    public boolean isLast() {
        return last;
    }

    public UUID getSenderId() {
        return senderId;
    }

    public void setSenderId(UUID senderId) {
        this.senderId = senderId;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(rows.size());

        for (Row row : rows) {
            out.writeObject(row);
        }

        out.writeBoolean(last);
        out.writeLong(remainingMemory);
    }

    @SuppressWarnings("Duplicates")
    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int rowCnt = in.readInt();

        if (rowCnt == 0) {
            rows = Collections.emptyList();
        } else {
            rows = new ArrayList<>(rowCnt);

            for (int i = 0; i < rowCnt; i++) {
                rows.add(in.readObject());
            }
        }

        last = in.readBoolean();
        remainingMemory = in.readLong();
    }

    @Override
    public String toString() {
        return "SendBatch{senderId=" + senderId + ", last=" + last + ", remainingMemory=" + remainingMemory
                   + ", rowCount=" + rows.size() + ", rows=" + rows + '}';
    }
}
