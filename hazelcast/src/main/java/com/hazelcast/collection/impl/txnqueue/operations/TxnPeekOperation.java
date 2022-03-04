/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.txnqueue.operations;

import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;
import com.hazelcast.collection.impl.queue.operations.QueueOperation;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.internal.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import java.io.IOException;
import java.util.UUID;

/**
 * Peek operation for the transactional queue.
 */
public class TxnPeekOperation extends QueueOperation implements BlockingOperation, ReadonlyOperation {

    private long itemId;
    private UUID transactionId;

    public TxnPeekOperation() {
    }

    public TxnPeekOperation(String name, long timeoutMillis, long itemId, UUID transactionId) {
        super(name, timeoutMillis);
        this.itemId = itemId;
        this.transactionId = transactionId;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        response = queueContainer.txnPeek(itemId, transactionId);
    }

    @Override
    public void afterRun() throws Exception {
        if (response != null) {
            LocalQueueStatsImpl localQueueStatsImpl = getQueueService().getLocalQueueStatsImpl(name);
            localQueueStatsImpl.incrementOtherOperations();
        }
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.TXN_PEEK;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
        UUIDSerializationUtil.writeUUID(out, transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
        transactionId = UUIDSerializationUtil.readUUID(in);
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        QueueContainer queueContainer = getContainer();
        return queueContainer.getPollWaitNotifyKey();
    }


    @Override
    public boolean shouldWait() {
        final QueueContainer queueContainer = getContainer();
        return getWaitTimeout() != 0 && itemId == -1 && queueContainer.size() == 0;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }
}
