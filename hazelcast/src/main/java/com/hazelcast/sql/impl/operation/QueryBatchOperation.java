/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.mailbox.SendBatch;
import com.hazelcast.sql.impl.worker.data.BatchDataTask;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.SqlServiceImpl;

import java.io.IOException;

/**
 * Execution batch.
 */
public class QueryBatchOperation extends QueryAbstractOperation {
    private QueryId queryId;
    private int edgeId;
    private String sourceMemberId;
    private int sourceStripe;
    private int sourceThread;
    private int targetStripe;
    private int targetThread;
    private SendBatch batch;

    public QueryBatchOperation() {
        // No-op.
    }

    public QueryBatchOperation(
        QueryId queryId,
        int edgeId,
        String sourceMemberId,
        int sourceStripe,
        int sourceThread,
        int targetStripe,
        int targetThread,
        SendBatch batch
    ) {
        this.queryId = queryId;
        this.edgeId = edgeId;
        this.sourceMemberId = sourceMemberId;
        this.sourceStripe = sourceStripe;
        this.sourceThread = sourceThread;
        this.targetStripe = targetStripe;
        this.targetThread = targetThread;
        this.batch = batch;
    }

    @Override
    public void run() throws Exception {
        // TODO: Avoid "getService" call, use NodeEngine instead.
        SqlServiceImpl service = getService();

        service.onQueryBatchRequest(new BatchDataTask(queryId, edgeId, sourceMemberId, sourceStripe, sourceThread,
            targetStripe, targetThread, batch));
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        queryId.writeData(out);

        out.writeInt(edgeId);
        out.writeUTF(sourceMemberId);
        out.writeInt(sourceStripe);
        out.writeInt(sourceThread);
        out.writeInt(targetStripe);
        out.writeInt(targetThread);

        batch.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        queryId = new QueryId();
        queryId.readData(in);

        edgeId = in.readInt();
        sourceMemberId = in.readUTF();
        sourceStripe = in.readInt();
        sourceThread = in.readInt();
        targetStripe = in.readInt();
        targetThread = in.readInt();

        batch = new SendBatch();
        batch.readData(in);
    }
}
