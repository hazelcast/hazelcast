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
import com.hazelcast.sql.impl.mailbox.SendBatch;

import java.io.IOException;

/**
 * Batch operation.
 */
public class QueryBatchOperation extends QueryIdAwareOperation {
    private int edgeId;
    private SendBatch batch;

    public QueryBatchOperation() {
        // No-op.
    }

    public QueryBatchOperation(
        long epochWatermark,
        QueryId queryId,
        int edgeId,
        SendBatch batch
    ) {
        super(epochWatermark, queryId);

        this.edgeId = edgeId;
        this.batch = batch;
    }

    public int getEdgeId() {
        return edgeId;
    }

    public SendBatch getBatch() {
        return batch;
    }

    @Override
    protected void writeInternal1(ObjectDataOutput out) throws IOException {
        out.writeInt(edgeId);

        batch.writeData(out);
    }

    @Override
    protected void readInternal1(ObjectDataInput in) throws IOException {
        edgeId = in.readInt();

        batch = new SendBatch();
        batch.readData(in);
    }
}
