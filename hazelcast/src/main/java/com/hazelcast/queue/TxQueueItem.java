/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

/**
 * Transactional Queue Item.
 */
public class TxQueueItem extends QueueItem {

    private String transactionId;

    private boolean pollOperation;

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

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeUTF(transactionId);
        out.writeBoolean(pollOperation);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        transactionId = in.readUTF();
        pollOperation = in.readBoolean();
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.TX_QUEUE_ITEM;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TxQueueItem)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        TxQueueItem item = (TxQueueItem) o;

        if (pollOperation != item.pollOperation) {
            return false;
        }
        if (!transactionId.equals(item.transactionId)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + transactionId.hashCode();
        result = 31 * result + (pollOperation ? 1 : 0);
        return result;
    }
}
