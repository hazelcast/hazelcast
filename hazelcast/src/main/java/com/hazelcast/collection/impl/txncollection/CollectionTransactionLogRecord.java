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

package com.hazelcast.collection.impl.txncollection;

import com.hazelcast.collection.impl.txncollection.operations.CollectionPrepareOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionRollbackOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionTxnRemoveOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.impl.TransactionLogRecord;

import java.io.IOException;

public class CollectionTransactionLogRecord implements TransactionLogRecord {

    private String transactionId;
    private long itemId;
    private String name;
    private Operation op;
    private int partitionId;
    private String serviceName;

    public CollectionTransactionLogRecord() {
    }

    public CollectionTransactionLogRecord(long itemId, String name, int partitionId, String serviceName,
                                          String transactionId, Operation op) {
        this.itemId = itemId;
        this.name = name;
        this.op = op;
        this.partitionId = partitionId;
        this.serviceName = serviceName;
        this.transactionId = transactionId;
    }

    @Override
    public Object getKey() {
        return new TransactionLogRecordKey(name, itemId, serviceName);
    }

    @Override
    public Operation newPrepareOperation() {
        boolean removeOperation = op instanceof CollectionTxnRemoveOperation;
        return new CollectionPrepareOperation(partitionId, name, serviceName, itemId, transactionId, removeOperation);
    }

    @Override
    public Operation newCommitOperation() {
        op.setServiceName(serviceName);
        op.setPartitionId(partitionId);
        return op;
    }

    @Override
    public Operation newRollbackOperation() {
        boolean removeOperation = op instanceof CollectionTxnRemoveOperation;
        return new CollectionRollbackOperation(partitionId, name, serviceName, itemId, removeOperation);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(itemId);
        out.writeUTF(name);
        out.writeInt(partitionId);
        out.writeUTF(serviceName);
        out.writeObject(op);
        out.writeUTF(transactionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        itemId = in.readLong();
        name = in.readUTF();
        partitionId = in.readInt();
        serviceName = in.readUTF();
        op = in.readObject();
        transactionId = in.readUTF();
    }

    @Override
    public String toString() {
        return "CollectionTransactionLogRecord{"
                + "transactionId='" + transactionId + '\''
                + ", itemId=" + itemId
                + ", name='" + name + '\''
                + ", op=" + op
                + ", partitionId=" + partitionId
                + ", serviceName='" + serviceName + '\''
                + '}';
    }
}
