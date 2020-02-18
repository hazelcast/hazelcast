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

import java.util.UUID;

/**
 * Abstract inbox implementation.
 */
public abstract class AbstractInbox extends AbstractMailbox {
    /** Initial size of batch queues. */
    protected static final int INITIAL_QUEUE_SIZE = 4;

    /** Number of enqueued batches. */
    protected int enqueuedBatches;

    /** Remaining remote sources. */
    private int remainingSources;

    protected AbstractInbox(QueryId queryId, int edgeId, int remainingSources) {
        super(queryId, edgeId);

        this.remainingSources = remainingSources;
    }

    /**
     * Handle batch arrival. Always invoked from the worker.
     */
    public void onBatch(UUID sourceMemberId, SendBatch batch) {
        onBatch0(sourceMemberId, batch);

        enqueuedBatches++;

        if (batch.isLast()) {
            remainingSources--;
        }
    }

    protected abstract void onBatch0(UUID sourceMemberId, SendBatch batch);

    /**
     * @return {@code True} if no more incoming batches are expected.
     */
    public boolean closed() {
        return enqueuedBatches == 0 && remainingSources == 0;
    }
}
