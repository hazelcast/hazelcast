/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.operation.coordinator;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.row.RowBatch;

import java.io.IOException;
import java.util.UUID;

/**
 * Batch operation.
 */
public class QueryBatchExchangeOperation extends QueryAbstractExchangeOperation {

    private RowBatch batch;
    private long ordinal;
    private boolean last;
    private long remainingMemory;

    public QueryBatchExchangeOperation() {
        // No-op.
    }

    public QueryBatchExchangeOperation(
        QueryId queryId,
        int edgeId,
        UUID targetMemberId,
        RowBatch batch,
        long ordinal,
        boolean last,
        long remainingMemory
    ) {
        super(queryId, edgeId, targetMemberId);

        assert batch != null;
        assert remainingMemory >= 0L;

        this.batch = batch;
        this.ordinal = ordinal;
        this.last = last;
        this.remainingMemory = remainingMemory;
    }

    public RowBatch getBatch() {
        return batch;
    }

    public long getOrdinal() {
        return ordinal;
    }

    public boolean isLast() {
        return last;
    }

    public long getRemainingMemory() {
        return remainingMemory;
    }

    @Override
    public boolean isInbound() {
        return true;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.OPERATION_BATCH;
    }

    @Override
    protected void writeInternal2(ObjectDataOutput out) throws IOException {
        out.writeObject(batch);
        out.writeLong(ordinal);
        out.writeBoolean(last);
        out.writeLong(remainingMemory);
    }

    @Override
    protected void readInternal2(ObjectDataInput in) throws IOException {
        batch = in.readObject();
        ordinal = in.readLong();
        last = in.readBoolean();
        remainingMemory = in.readLong();
    }
}
