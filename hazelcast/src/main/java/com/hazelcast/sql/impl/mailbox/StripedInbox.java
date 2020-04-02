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
import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;

/**
 * Inbox which puts batches from different senders into separate queues.
 */
public class StripedInbox extends AbstractInbox {
    /** Map from member ID to index. */
    private final HashMap<UUID, Integer> memberToIdxMap = new HashMap<>();

    /** Batches from members. */
    private final ArrayDeque<InboundBatch>[] queues;

    @SuppressWarnings("unchecked")
    public StripedInbox(
        QueryId queryId,
        int edgeId,
        int rowWidth,
        QueryOperationHandler operationHandler,
        Collection<UUID> senderMemberIds,
        FlowControl flowControl
    ) {
        super(queryId, edgeId, rowWidth, operationHandler, senderMemberIds.size(), flowControl);

        // Build inverse map from the member to its index.
        int memberIdx = 0;

        for (UUID senderMemberId : senderMemberIds) {
            memberToIdxMap.put(senderMemberId, memberIdx);

            memberIdx++;
        }

        // Initialize queues.
        queues = new ArrayDeque[memberIdx];

        for (int i = 0; i < memberIdx; i++) {
            queues[i] = new ArrayDeque<>(INITIAL_QUEUE_SIZE);
        }
    }

    @Override
    public void onBatch0(InboundBatch batch) {
        int idx = memberToIdxMap.get(batch.getSenderId());

        ArrayDeque<InboundBatch> queue = queues[idx];

        queue.add(batch);
    }

    public int getStripeCount() {
        return queues.length;
    }

    public InboundBatch poll(int stripe) {
        InboundBatch batch = queues[stripe].poll();

        onBatchPolled(batch);

        return batch;
    }

    @Override
    public String toString() {
        return "StripedInbox {queryId=" + queryId + ", edgeId=" + edgeId + "}";
    }
}
