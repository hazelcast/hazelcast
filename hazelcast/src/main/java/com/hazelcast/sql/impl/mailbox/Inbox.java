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

package com.hazelcast.sql.impl.mailbox;

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.mailbox.flowcontrol.FlowControl;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;

import java.util.ArrayDeque;

/**
 * Normal inbox which merges all incoming batches into a single stream.
 */
public class Inbox extends AbstractInbox {
    /** Queue of batches from all remote stripes. */
    private final ArrayDeque<InboundBatch> batches = new ArrayDeque<>(INITIAL_QUEUE_SIZE);

    public Inbox(
        QueryId queryId,
        int edgeId,
        int rowWidth,
        QueryOperationHandler operationHandler,
        int remainingSources,
        FlowControl flowControl
    ) {
        super(queryId, edgeId, rowWidth, operationHandler, remainingSources, flowControl);
    }

    @Override
    public void onBatch0(InboundBatch batch) {
        batches.addLast(batch);
    }

    public InboundBatch poll() {
        InboundBatch batch = batches.pollFirst();

        onBatchPolled(batch);

        return batch;
    }

    @Override
    public String toString() {
        return "SingleInbox {queryId=" + queryId + ", edgeId=" + edgeId + "}";
    }
}
