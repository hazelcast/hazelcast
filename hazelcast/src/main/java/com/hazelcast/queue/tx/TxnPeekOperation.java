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

package com.hazelcast.queue.tx;

import com.hazelcast.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.queue.QueueDataSerializerHook;
import com.hazelcast.queue.QueueOperation;

import java.io.IOException;

/**
 * Peek operation for the transactional queue.
 */
public class TxnPeekOperation extends QueueOperation {

    private long itemId;
    private String transactionId;

    public TxnPeekOperation() {

    }

    public TxnPeekOperation(String name, long timeoutMillis, long itemId, String transactionId) {
        super(name, timeoutMillis);
        this.itemId = itemId;
        this.transactionId = transactionId;
    }

    @Override
    public void run() throws Exception {
        response = getOrCreateContainer().txnPeek(itemId, transactionId);
    }

    @Override
    public void afterRun() throws Exception {
        if (response != null) {
            LocalQueueStatsImpl localQueueStatsImpl = getQueueService().getLocalQueueStatsImpl(name);
            localQueueStatsImpl.incrementOtherOperations();
        }
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.TXN_PEEK;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
        out.writeUTF(transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
        transactionId = in.readUTF();
    }
}
