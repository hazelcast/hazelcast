/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.collection.impl.CollectionTxnUtil;
import com.hazelcast.collection.impl.txncollection.operations.CollectionCommitOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionPrepareOperation;
import com.hazelcast.collection.impl.txncollection.operations.CollectionRollbackOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.impl.TransactionLogRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class contains Transaction log for the Collection.
 */
public class CollectionTransactionLogRecord implements TransactionLogRecord {

    protected String name;
    protected List<Operation> operationList;
    protected int partitionId;
    protected String transactionId;
    protected String serviceName;

    public CollectionTransactionLogRecord() {
    }

    public CollectionTransactionLogRecord(String serviceName, String transactionId, String name, int partitionId) {
        this.serviceName = serviceName;
        this.transactionId = transactionId;
        this.name = name;
        this.partitionId = partitionId;
        this.operationList = new ArrayList<Operation>();
    }

    @Override
    public Operation newPrepareOperation() {
        long[] itemIds = createItemIdArray();
        return new CollectionPrepareOperation(partitionId, name, serviceName, itemIds, transactionId);
    }

    @Override
    public Operation newCommitOperation() {
        return new CollectionCommitOperation(partitionId, name, serviceName, operationList);
    }

    @Override
    public Operation newRollbackOperation() {
        long[] itemIds = createItemIdArray();
        return new CollectionRollbackOperation(partitionId, name, serviceName, itemIds);
    }

    @Override
    public Object getKey() {
        return name;
    }

    public void addOperation(CollectionTxnOperation operation) {
        Iterator<Operation> iterator = operationList.iterator();
        while (iterator.hasNext()) {
            CollectionTxnOperation op = (CollectionTxnOperation) iterator.next();
            if (op.getItemId() == operation.getItemId()) {
                iterator.remove();
                break;
            }
        }
        operationList.add((Operation) operation);
    }

    public int removeOperation(long itemId) {
        Iterator<Operation> iterator = operationList.iterator();
        while (iterator.hasNext()) {
            CollectionTxnOperation op = (CollectionTxnOperation) iterator.next();
            if (op.getItemId() == itemId) {
                iterator.remove();
                break;
            }
        }
        return operationList.size();
    }

    protected long[] createItemIdArray() {
        int size = operationList.size();
        long[] itemIds = new long[size];
        for (int i = 0; i < size; i++) {
            CollectionTxnOperation operation = (CollectionTxnOperation) operationList.get(i);
            itemIds[i] = CollectionTxnUtil.getItemId(operation);
        }
        return itemIds;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(serviceName);
        out.writeUTF(transactionId);
        out.writeUTF(name);
        out.writeInt(partitionId);
        CollectionTxnUtil.write(out, operationList);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        serviceName = in.readUTF();
        transactionId = in.readUTF();
        name = in.readUTF();
        partitionId = in.readInt();
        operationList = CollectionTxnUtil.read(in);
    }


}
