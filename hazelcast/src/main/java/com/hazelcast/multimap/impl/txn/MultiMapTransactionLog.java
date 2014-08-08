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

package com.hazelcast.multimap.impl.txn;

import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.transaction.impl.KeyAwareTransactionLog;
import com.hazelcast.util.ExceptionUtil;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;

public class MultiMapTransactionLog implements KeyAwareTransactionLog {

    String name;
    final List<Operation> opList = new LinkedList<Operation>();
    Data key;
    long ttl;
    long threadId;
    long txVersion;

    public MultiMapTransactionLog() {
    }

    public MultiMapTransactionLog(Data key, String name, long ttl, long threadId, long version) {
        this.key = key;
        this.name = name;
        this.ttl = ttl;
        this.threadId = threadId;
        this.txVersion = version;
    }

    public Future prepare(NodeEngine nodeEngine) {
        TxnPrepareOperation operation = new TxnPrepareOperation(name, key, ttl, threadId);
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            final OperationService operationService = nodeEngine.getOperationService();
            return operationService.invokeOnPartition(MultiMapService.SERVICE_NAME, operation, partitionId);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Future commit(NodeEngine nodeEngine) {
        TxnCommitOperation operation = new TxnCommitOperation(name, key, threadId, txVersion, opList);
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            final OperationService operationService = nodeEngine.getOperationService();
            return operationService.invokeOnPartition(MultiMapService.SERVICE_NAME, operation, partitionId);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Future rollback(NodeEngine nodeEngine) {
        TxnRollbackOperation operation = new TxnRollbackOperation(name, key, threadId);
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            final OperationService operationService = nodeEngine.getOperationService();
            return operationService.invokeOnPartition(MultiMapService.SERVICE_NAME, operation, partitionId);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(opList.size());
        for (Operation op : opList) {
            out.writeObject(op);
        }
        key.writeData(out);
        out.writeLong(ttl);
        out.writeLong(threadId);
        out.writeLong(txVersion);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            opList.add((Operation) in.readObject());
        }
        key = new Data();
        key.readData(in);
        ttl = in.readLong();
        threadId = in.readLong();
        txVersion = in.readLong();
    }

    public Object getKey() {
        return new TransactionLogKey(name, key);
    }

    public void addOperation(Operation op) {
        if (op instanceof TxnRemoveOperation) {
            TxnRemoveOperation removeOperation = (TxnRemoveOperation) op;
            Iterator<Operation> iter = opList.iterator();
            while (iter.hasNext()) {
                Operation opp = iter.next();
                if (opp instanceof TxnPutOperation) {
                    TxnPutOperation putOperation = (TxnPutOperation) opp;
                    if (putOperation.getRecordId() == removeOperation.getRecordId()) {
                        iter.remove();
                        return;
                    }
                }
            }
        } else if (op instanceof TxnRemoveAllOperation) {
            TxnRemoveAllOperation removeAllOperation = (TxnRemoveAllOperation) op;
            Collection<Long> recordIds = removeAllOperation.getRecordIds();
            Iterator<Operation> iter = opList.iterator();
            while (iter.hasNext()) {
                Operation opp = iter.next();
                if (opp instanceof TxnPutOperation) {
                    TxnPutOperation putOperation = (TxnPutOperation) opp;
                    if (recordIds.remove(putOperation.getRecordId())) {
                        iter.remove();
                    }
                }
            }
            if (recordIds.isEmpty()) {
                return;
            }
        }
        opList.add(op);
    }

    public int size() {
        int size = 0;
        for (Operation operation : opList) {
            if (operation instanceof TxnRemoveAllOperation) {
                TxnRemoveAllOperation removeAllOperation = (TxnRemoveAllOperation) operation;
                size -= removeAllOperation.getRecordIds().size();
            } else if (operation instanceof TxnRemoveOperation) {
                size--;
            } else {
                size++;
            }
        }
        return size;
    }


}
