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

import java.io.IOException;

/**
 * Flow control operation which allows for remote sender to proceed with sending.
 */
public class QueryFlowControlOperation extends QueryDataExchangeOperation {
    /** Amount of memory available on the receiver. */
    private long remainingMemory;

    public QueryFlowControlOperation() {
        // No-op.
    }

    public QueryFlowControlOperation(long epochWatermark, QueryId queryId, int edgeId, long remainingMemory) {
        super(epochWatermark, queryId, edgeId);

        this.remainingMemory = remainingMemory;
    }

    public long getRemainingMemory() {
        return remainingMemory;
    }

    @Override
    public boolean isInbound() {
        return false;
    }

    @Override
    protected void writeInternal2(ObjectDataOutput out) throws IOException {
        out.writeLong(remainingMemory);
    }

    @Override
    protected void readInternal2(ObjectDataInput in) throws IOException {
        remainingMemory = in.readLong();
    }
}
