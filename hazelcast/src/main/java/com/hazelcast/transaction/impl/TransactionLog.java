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

package com.hazelcast.transaction.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * The transaction log contains all {@link TransactionLogRecord} for a given transaction.
 *
 * If within a transaction 3 map.puts would be done on different keys and 1 queue.take would be done, than
 * the TransactionLog will contains 4 {@link TransactionLogRecord} instances.
 *
 * planned optimization:
 * Most transaction will be small, but an linkedlist + hashmap is created. Instead use an array and do a
 * linear search in that array. When there are too many items added, then enable the hashmap.
 */
public class TransactionLog implements Iterable<TransactionLogRecord> {

    private final List<TransactionLogRecord> recordList = new LinkedList<TransactionLogRecord>();
    private final Map<Object, TransactionLogRecord> recordMap = new HashMap<Object, TransactionLogRecord>();

    public TransactionLog() {
    }

    public TransactionLog(List<TransactionLogRecord> transactionLog) {
        //
        recordList.addAll(transactionLog);
    }

    @Override
    public Iterator<TransactionLogRecord> iterator() {
        return recordList.iterator();
    }

    public void add(TransactionLogRecord record) {
        // there should be just one tx log for the same key. so if there is older we are removing it
        if (record instanceof KeyAwareTransactionLogRecord) {
            KeyAwareTransactionLogRecord keyAwareRecord = (KeyAwareTransactionLogRecord) record;
            TransactionLogRecord removed = recordMap.remove(keyAwareRecord.getKey());
            if (removed != null) {
                recordList.remove(removed);
            }
        }

        recordList.add(record);

        if (record instanceof KeyAwareTransactionLogRecord) {
            KeyAwareTransactionLogRecord keyAwareRecord = (KeyAwareTransactionLogRecord) record;
            recordMap.put(keyAwareRecord.getKey(), keyAwareRecord);
        }
    }

    public TransactionLogRecord get(Object key) {
        return recordMap.get(key);
    }

    public List<TransactionLogRecord> getRecordList() {
        return recordList;
    }

    public void remove(Object key) {
        TransactionLogRecord removed = recordMap.remove(key);
        if (removed != null) {
            recordList.remove(removed);
        }
    }

    /**
     * Returns the number of TransactionRecords in this TransactionLog.
     *
     * @return the number of TransactionRecords.
     */
    public int size() {
        return recordList.size();
    }

    public Future prepare(NodeEngine nodeEngine, TransactionLogRecord record) {
        return invoke(nodeEngine, record.newPrepareOperation());
    }

    public Future commit(NodeEngine nodeEngine, TransactionLogRecord record) {
        return invoke(nodeEngine, record.newCommitOperation());
    }

    public Future rollback(NodeEngine nodeEngine, TransactionLogRecord record) {
        return invoke(nodeEngine, record.newRollbackOperation());
    }

    private Future invoke(NodeEngine nodeEngine, Operation op) {
        OperationService operationService = nodeEngine.getOperationService();

        return operationService.invokeOnPartition(
                op.getServiceName(),
                op,
                op.getPartitionId());
    }

    public void commitAsync(NodeEngine nodeEngine, TransactionLogRecord record, ExecutionCallback callback) {
        invokeAsync(nodeEngine, callback, record.newCommitOperation());
    }

    public void rollbackAsync(NodeEngine nodeEngine, ExecutionCallback callback, TransactionLogRecord record) {
        invokeAsync(nodeEngine, callback, record.newRollbackOperation());
    }

    private void invokeAsync(NodeEngine nodeEngine, ExecutionCallback callback, Operation op) {
        InternalOperationService operationService = (InternalOperationService) nodeEngine.getOperationService();

        operationService.asyncInvokeOnPartition(
                op.getServiceName(),
                op,
                op.getPartitionId(),
                callback);
    }
}
