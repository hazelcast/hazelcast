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

import java.util.ArrayDeque;
import java.util.UUID;

/**
 * AbstractInbox which merges batches from all the nodes into a single stream.
 */
public class SingleInbox extends AbstractInbox {
    /** Queue of batches from all remote stripes. */
    private final ArrayDeque<SendBatch> batches = new ArrayDeque<>();

    public SingleInbox(QueryId queryId, int edgeId, int remaining) {
        super(queryId, edgeId, remaining);
    }

    @Override
    public void onBatch0(UUID sourceMemberId, SendBatch batch) {
        batches.add(batch);
    }

    public SendBatch poll() {
        return batches.poll();
    }

    @Override
    public String toString() {
        return "SingleInbox {queryId=" + queryId + ", edgeId=" + getEdgeId() + "}";
    }
}
