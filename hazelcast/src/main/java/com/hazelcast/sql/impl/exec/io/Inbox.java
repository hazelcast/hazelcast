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

package com.hazelcast.sql.impl.exec.io;

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.exec.io.flowcontrol.FlowControl;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;

import java.util.ArrayDeque;
import java.util.UUID;

/**
 * Normal inbox which merges all incoming batches into a single stream.
 */
public class Inbox extends AbstractInbox {
    /** Queue of batches from all remote stripes. */
    private final ArrayDeque<InboundBatch> batches = new ArrayDeque<>();

    public Inbox(
        QueryOperationHandler operationHandler,
        QueryId queryId,
        int edgeId,
        int rowWidth,
        UUID localMemberId,
        int remainingStreams,
        FlowControl flowControl
    ) {
        super(operationHandler, queryId, edgeId, rowWidth, localMemberId, remainingStreams, flowControl);
    }

    @Override
    protected void onBatch0(InboundBatch batch) {
        batches.addLast(batch);
    }

    public InboundBatch poll() {
        InboundBatch batch = batches.pollFirst();

        onBatchPolled(batch);

        return batch;
    }

    @Override
    public String toString() {
        return "Inbox {queryId=" + queryId + ", edgeId=" + edgeId + "}";
    }
}
