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

import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.mailbox.SendBatch;

import java.io.IOException;
import java.util.UUID;

/**
 * Batch operation.
 */
public class QueryBatchOperation extends QueryOperation {
    private QueryId queryId;
    private int edgeId;
    private UUID sourceMemberId;
    private int sourceDeploymentOffset;
    private int targetDeploymentOffset;
    private SendBatch batch;

    public QueryBatchOperation() {
        // No-op.
    }

    public QueryBatchOperation(
        QueryId queryId,
        int edgeId,
        UUID sourceMemberId,
        int sourceDeploymentOffset,
        int targetDeploymentOffset,
        SendBatch batch
    ) {
        this.queryId = queryId;
        this.edgeId = edgeId;
        this.sourceMemberId = sourceMemberId;
        this.sourceDeploymentOffset = sourceDeploymentOffset;
        this.targetDeploymentOffset = targetDeploymentOffset;
        this.batch = batch;
    }

    public QueryId getQueryId() {
        return queryId;
    }

    public int getEdgeId() {
        return edgeId;
    }

    public UUID getSourceMemberId() {
        return sourceMemberId;
    }

    public int getSourceDeploymentOffset() {
        return sourceDeploymentOffset;
    }

    public int getTargetDeploymentOffset() {
        return targetDeploymentOffset;
    }

    public SendBatch getBatch() {
        return batch;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        queryId.writeData(out);

        out.writeInt(edgeId);
        UUIDSerializationUtil.writeUUID(out, sourceMemberId);
        out.writeInt(sourceDeploymentOffset);
        out.writeInt(targetDeploymentOffset);

        batch.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        queryId = new QueryId();
        queryId.readData(in);

        edgeId = in.readInt();
        sourceMemberId = UUIDSerializationUtil.readUUID(in);
        sourceDeploymentOffset = in.readInt();
        targetDeploymentOffset = in.readInt();

        batch = new SendBatch();
        batch.readData(in);
    }
}
