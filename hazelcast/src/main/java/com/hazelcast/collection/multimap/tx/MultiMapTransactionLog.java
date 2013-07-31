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

package com.hazelcast.collection.multimap.tx;

import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.impl.KeyAwareTransactionLog;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * @author ali 3/29/13
 */
public class MultiMapTransactionLog implements KeyAwareTransactionLog {

    CollectionProxyId proxyId;
    final List<Operation> opList = new LinkedList<Operation>();
    Data key;
    long ttl;
    int threadId;
    long txVersion;

    public MultiMapTransactionLog() {
    }

    public MultiMapTransactionLog(Data key, CollectionProxyId proxyId, long ttl, int threadId, long version) {
        this.key = key;
        this.proxyId = proxyId;
        this.ttl = ttl;
        this.threadId = threadId;
        this.txVersion = version;
    }

    public Future prepare(NodeEngine nodeEngine) {
        TxnPrepareOperation operation = new TxnPrepareOperation(proxyId, key, ttl, threadId);
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(CollectionService.SERVICE_NAME, operation, partitionId).build();
            return invocation.invoke();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Future commit(NodeEngine nodeEngine) {
        TxnCommitOperation operation = new TxnCommitOperation(proxyId, key, threadId, txVersion, opList);
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(CollectionService.SERVICE_NAME, operation, partitionId).build();
            return invocation.invoke();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Future rollback(NodeEngine nodeEngine) {
        TxnRollbackOperation operation = new TxnRollbackOperation(proxyId, key, threadId);
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Invocation invocation = nodeEngine.getOperationService()
                    .createInvocationBuilder(CollectionService.SERVICE_NAME, operation, partitionId).build();
            return invocation.invoke();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        proxyId.writeData(out);
        out.writeInt(opList.size());
        for (Operation op: opList){
            out.writeObject(op);
        }
        key.writeData(out);
        out.writeLong(ttl);
        out.writeInt(threadId);
        out.writeLong(txVersion);
    }

    public void readData(ObjectDataInput in) throws IOException {
        proxyId = new CollectionProxyId();
        proxyId.readData(in);
        int size = in.readInt();
        for (int i=0; i<size; i++){
            opList.add((Operation)in.readObject());
        }
        key = new Data();
        key.readData(in);
        ttl = in.readLong();
        threadId = in.readInt();
        txVersion = in.readLong();
    }

    public Object getKey() {
        return new TransactionLogKey(proxyId, key);
    }

    public void addOperation(Operation op){
        if (op instanceof TxnRemoveOperation){
            TxnRemoveOperation removeOperation = (TxnRemoveOperation)op;
            Iterator<Operation> iter = opList.iterator();
            while (iter.hasNext()){
                Operation opp = iter.next();
                if (opp instanceof TxnPutOperation){
                    TxnPutOperation putOperation = (TxnPutOperation)opp;
                    if(putOperation.getRecordId() == removeOperation.getRecordId()){
                        iter.remove();
                        return;
                    }
                }
            }
        }
        else if (op instanceof TxnRemoveAllOperation){
            TxnRemoveAllOperation removeAllOperation = (TxnRemoveAllOperation)op;
            Collection<Long> recordIds = removeAllOperation.getRecordIds();
            Iterator<Operation> iter = opList.iterator();
            while (iter.hasNext()){
                Operation opp = iter.next();
                if (opp instanceof TxnPutOperation){
                    TxnPutOperation putOperation = (TxnPutOperation)opp;
                    if (recordIds.remove(putOperation.getRecordId())){
                        iter.remove();
                    }
                }
            }
            if (recordIds.isEmpty()){
                return;
            }
        }
        opList.add(op);
    }

    public int size(){
        int size = 0;
        for (Operation operation : opList) {
            if (operation instanceof TxnRemoveAllOperation) {
                TxnRemoveAllOperation removeAllOperation = (TxnRemoveAllOperation)operation;
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
