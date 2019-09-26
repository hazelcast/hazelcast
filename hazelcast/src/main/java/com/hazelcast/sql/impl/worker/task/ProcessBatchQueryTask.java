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

package com.hazelcast.sql.impl.worker.task;

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.mailbox.SendBatch;

import java.util.UUID;

/**
 * Task to process incoming batch. Batch may be either mapped or unmapped. In the first case we submit it directly
 * to the data pool. Otherwise we pass it through the control pool to perform mapping when local context is ready.
 */
public class ProcessBatchQueryTask implements QueryTask {
    /** Query ID. */
    private final QueryId queryId;

    /** Edge. */
    private final int edgeId;

    /** Source member which sent this batch. */
    private final UUID sourceMemberId;

    /** Source deployment ID. */
    private final int sourceDeploymentId;

    /** Target deployment ID. */
    private final int targetDeploymentId;

    /** Data. */
    private final SendBatch batch;

    public ProcessBatchQueryTask(
        QueryId queryId,
        int edgeId,
        UUID sourceMemberId,
        int sourceDeploymentId,
        int targetDeploymentId,
        SendBatch batch
    ) {
        this.queryId = queryId;
        this.edgeId = edgeId;
        this.sourceMemberId = sourceMemberId;
        this.sourceDeploymentId = sourceDeploymentId;
        this.targetDeploymentId = targetDeploymentId;
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

    public int getSourceDeploymentId() {
        return sourceDeploymentId;
    }

    public int getTargetDeploymentId() {
        return targetDeploymentId;
    }

    public SendBatch getBatch() {
        return batch;
    }
}
