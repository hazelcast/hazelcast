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

package com.hazelcast.sql.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlDataSerializerHook;

import java.io.IOException;
import java.util.UUID;

/**
 * Flow control operation which allows for remote sender to proceed with sending.
 */
public class QueryFlowControlExchangeOperation extends QueryAbstractExchangeOperation {

    private long ordinal;
    private long remainingMemory;

    public QueryFlowControlExchangeOperation() {
        // No-op.
    }

    public QueryFlowControlExchangeOperation(
        QueryId queryId,
        int edgeId,
        UUID targetMemberId,
        long ordinal,
        long remainingMemory
    ) {
        super(queryId, edgeId, targetMemberId);

        assert remainingMemory >= 0L;

        this.ordinal = ordinal;
        this.remainingMemory = remainingMemory;
    }

    public long getOrdinal() {
        return ordinal;
    }

    public long getRemainingMemory() {
        return remainingMemory;
    }

    @Override
    public boolean isInbound() {
        return false;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.QUERY_OPERATION_FLOW_CONTROL;
    }

    @Override
    protected void writeInternal2(ObjectDataOutput out) throws IOException {
        out.writeLong(ordinal);
        out.writeLong(remainingMemory);
    }

    @Override
    protected void readInternal2(ObjectDataInput in) throws IOException {
        ordinal = in.readLong();
        remainingMemory = in.readLong();
    }
}
