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

package com.hazelcast.multimap.impl.txn;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.transaction.impl.TransactionLogRecord;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class MultiMapTransactionLogRecord implements TransactionLogRecord {

    // TODO: probably better to switch to an ArrayList to reduce litter
    private final List<Operation> opList = new LinkedList<>();
    private int partitionId;
    private String name;
    private Data key;
    private long ttl;
    private long threadId;

    public MultiMapTransactionLogRecord() {
    }

    public MultiMapTransactionLogRecord(int partitionId, Data key, String name, long ttl, long threadId) {
        this.key = key;
        this.name = name;
        this.ttl = ttl;
        this.threadId = threadId;
        this.partitionId = partitionId;
    }

    @Override
    public Operation newPrepareOperation() {
        return new TxnPrepareOperation(partitionId, name, key, threadId);
    }

    @Override
    public Operation newCommitOperation() {
        return new TxnCommitOperation(partitionId, name, key, threadId, opList);
    }

    @Override
    public Operation newRollbackOperation() {
        return new TxnRollbackOperation(partitionId, name, key, threadId);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeInt(partitionId);
        out.writeInt(opList.size());
        for (Operation op : opList) {
            out.writeObject(op);
        }
        IOUtil.writeData(out, key);
        out.writeLong(ttl);
        out.writeLong(threadId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        partitionId = in.readInt();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            opList.add((Operation) in.readObject());
        }
        key = IOUtil.readData(in);
        ttl = in.readLong();
        threadId = in.readLong();
    }

    @Override
    public Object getKey() {
        return new TransactionRecordKey(name, key);
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
            Iterator<Operation> iterator = opList.iterator();
            while (iterator.hasNext()) {
                Operation opp = iterator.next();
                if (opp instanceof TxnPutOperation) {
                    TxnPutOperation putOperation = (TxnPutOperation) opp;
                    if (recordIds.remove(putOperation.getRecordId())) {
                        iterator.remove();
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

    @Override
    public String toString() {
        return "MultiMapTransactionRecord{"
                + "name='" + name + '\''
                + ", opList=" + opList
                + ", key=" + key
                + ", ttl=" + ttl
                + ", threadId=" + threadId
                + '}';
    }

    @Override
    public int getFactoryId() {
        return MultiMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.MULTIMAP_TRANSACTION_LOG_RECORD;
    }
}
