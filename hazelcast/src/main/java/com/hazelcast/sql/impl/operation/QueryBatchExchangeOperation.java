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

package com.hazelcast.sql.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QuerySerializationHook;
import com.hazelcast.sql.impl.row.RowBatch;

import java.io.IOException;

/**
 * Batch operation.
 */
public class QueryBatchExchangeOperation extends QueryAbstractExchangeOperation {
    private RowBatch batch;
    private boolean last;
    private long remainingMemory;

    public QueryBatchExchangeOperation() {
        // No-op.
    }

    public QueryBatchExchangeOperation(QueryId queryId, int edgeId, RowBatch batch, boolean last, long remainingMemory) {
        super(queryId, edgeId);

        this.batch = batch;
        this.last = last;
        this.remainingMemory = remainingMemory;
    }

    public RowBatch getBatch() {
        return batch;
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
        return QuerySerializationHook.OPERATION_BATCH;
    }

    @Override
    protected void writeInternal2(ObjectDataOutput out) throws IOException {
        out.writeObject(batch);
        out.writeBoolean(last);
        out.writeLong(remainingMemory);
    }

    @Override
    protected void readInternal2(ObjectDataInput in) throws IOException {
        batch = in.readObject();
        last = in.readBoolean();
        remainingMemory = in.readLong();
    }
}
