/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.queue;

import com.hazelcast.nio.serialization.Data;

/**
 * @ali 9/5/13
 */
public class TxQueueItem extends QueueItem {

    private transient String transactionId;

    private transient boolean pollOperation;

    public TxQueueItem() {
    }

    public TxQueueItem(QueueItem item) {
        this.itemId = item.itemId;
        this.container = item.container;
        this.data = item.data;
    }

    public TxQueueItem(QueueContainer container, long itemId, Data data) {
        super(container, itemId, data);
    }

    public String getTransactionId() {
        return transactionId;
    }

    public TxQueueItem setTransactionId(String transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    public boolean isPollOperation() {
        return pollOperation;
    }

    public TxQueueItem setPollOperation(boolean pollOperation) {
        this.pollOperation = pollOperation;
        return this;
    }
}
