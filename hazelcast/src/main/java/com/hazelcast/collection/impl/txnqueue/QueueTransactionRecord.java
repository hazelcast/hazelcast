/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.txnqueue.operations.TxnPollOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnPrepareOperation;
import com.hazelcast.collection.impl.txnqueue.operations.TxnRollbackOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.impl.AbstractTransactionRecord;
import com.hazelcast.transaction.impl.KeyAwareTransactionRecord;

import java.io.IOException;

/**
 * This class contains Transaction log for the Queue.
 */
public class QueueTransactionRecord extends AbstractTransactionRecord implements KeyAwareTransactionRecord {

    private long itemId;
    private String name;
    private Operation op;
    private int partitionId;
    private String transactionId;

    public QueueTransactionRecord() {
    }

    public QueueTransactionRecord(String transactionId, long itemId, String name, int partitionId, Operation op) {
        this.transactionId = transactionId;
        this.itemId = itemId;
        this.name = name;
        this.partitionId = partitionId;
        this.op = op;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public Operation createPrepareOperation() {
        boolean pollOperation = op instanceof TxnPollOperation;
        return new TxnPrepareOperation(partitionId, name, itemId, pollOperation, transactionId);
    }

    @Override
    public Operation createRollbackOperation() {
        boolean pollOperation = op instanceof TxnPollOperation;
        return new TxnRollbackOperation(partitionId, name, itemId, pollOperation);
    }

    @Override
    public Operation createCommitOperation() {
        op.setPartitionId(partitionId);
        op.setServiceName(QueueService.SERVICE_NAME);
        return op;
    }

    @Override
    public Object getKey() {
        return new TransactionRecordKey(itemId, name);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(transactionId);
        out.writeLong(itemId);
        out.writeUTF(name);
        out.writeInt(partitionId);
        out.writeObject(op);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        transactionId = in.readUTF();
        itemId = in.readLong();
        name = in.readUTF();
        partitionId = in.readInt();
        op = in.readObject();
    }
}
