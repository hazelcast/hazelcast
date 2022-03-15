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

package com.hazelcast.collection.impl.txnqueue;

import com.hazelcast.collection.impl.collection.CollectionDataSerializerHook;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.txncollection.CollectionTransactionLogRecord;
import com.hazelcast.collection.impl.txnqueue.operations.TxnCommitOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnPrepareOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnRollbackOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.UUID;

/**
 * This class contains Transaction log for the Queue.
 */
public class QueueTransactionLogRecord extends CollectionTransactionLogRecord {

    public QueueTransactionLogRecord() {
    }

    public QueueTransactionLogRecord(UUID transactionId, String name, int partitionId) {
        super(QueueService.SERVICE_NAME, transactionId, name, partitionId);
    }

    @Override
    public Operation newPrepareOperation() {
        long[] itemIds = createItemIdArray();
        return new TxnPrepareOperation(partitionId, name, itemIds, transactionId);
    }

    @Override
    public Operation newCommitOperation() {
        return new TxnCommitOperation(partitionId, name, operationList);
    }

    @Override
    public Operation newRollbackOperation() {
        long[] itemIds = createItemIdArray();
        return new TxnRollbackOperation(partitionId, name, itemIds);
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.QUEUE_TRANSACTION_LOG_RECORD;
    }
}
