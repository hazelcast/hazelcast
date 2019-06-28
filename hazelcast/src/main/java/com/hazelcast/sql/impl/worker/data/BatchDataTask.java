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

package com.hazelcast.sql.impl.worker.data;

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.mailbox.SendBatch;
import com.hazelcast.sql.impl.worker.control.ControlTask;

/**
 * Task to process incoming batch. Batch may be either mapped or unmapped. In the first case we submit it directly
 * to the data pool. Otherwise we pass it through the control pool to perform mapping when local context is ready.
 */
public class BatchDataTask implements ControlTask, DataTask {
    /** Query ID. */
    private final QueryId queryId;

    /** Edge. */
    private final int edgeId;

    /** Source member which sent this batch. */
    private final String sourceMemberId;

    /** Source member stripe. */
    private final int sourceStripe;

    /** Source member thread. */
    private final int sourceThread;

    /** Target stripe. */
    private final int targetStripe;

    /** Target thread. May be unknown on early execution stages. */
    private int targetThread;

    /** Data. */
    private final SendBatch batch;

    public BatchDataTask(QueryId queryId, int edgeId, String sourceMemberId, int sourceStripe, int sourceThread,
        int targetStripe, int targetThread, SendBatch batch) {
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
    public QueryId getQueryId() {
        return queryId;
    }

    @Override
    public int getThread() {
        return getTargetThread();
    }

    public int getEdgeId() {
        return edgeId;
    }

    public String getSourceMemberId() {
        return sourceMemberId;
    }

    public int getSourceStripe() {
        return sourceStripe;
    }

    public int getSourceThread() {
        return sourceThread;
    }

    public int getTargetStripe() {
        return targetStripe;
    }

    public int getTargetThread() {
        return targetThread;
    }

    public boolean isMapped() {
        return getTargetThread() != DataWorker.UNMAPPED_STRIPE;
    }

    public void setTargetThread(int targetThread) {
        this.targetThread = targetThread;
    }

    public SendBatch getBatch() {
        return batch;
    }
}