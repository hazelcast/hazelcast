/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * The transaction log contains all {@link TransactionLogRecord} for a given transaction.
 *
 * If within a transaction 3 map.puts would be done on different keys and 1 queue.take would be done, than
 * the TransactionLog will contains 4 {@link TransactionLogRecord} instances.
 *
 * planned optimization:
 * Most transaction will be small, but an linkedhashmap is created. Instead use an array and do a
 * linear search in that array. When there are too many items added, then enable the hashmap.
 */
public class TransactionLog {

    private final Map<Object, TransactionLogRecord> recordMap = new LinkedHashMap<>();

    public TransactionLog() {
    }

    public TransactionLog(List<TransactionLogRecord> transactionLog) {
        for (TransactionLogRecord record : transactionLog) {
            add(record);
        }
    }

    public void add(TransactionLogRecord record) {
        Object key = record.getKey();
        if (key == null) {
            key = new Object();
        }
        recordMap.put(key, record);
    }

    public TransactionLogRecord get(Object key) {
        return recordMap.get(key);
    }

    public List<TransactionLogRecord> getRecordList() {
        return new ArrayList<>(recordMap.values());
    }

    public void remove(Object key) {
        recordMap.remove(key);
    }

    /**
     * Returns the number of TransactionRecords in this TransactionLog.
     *
     * @return the number of TransactionRecords.
     */
    public int size() {
        return recordMap.size();
    }

    public List<Future> commit(NodeEngine nodeEngine) {
        List<Future> futures = new ArrayList<Future>(size());
        for (TransactionLogRecord record : recordMap.values()) {
            Future future = invoke(nodeEngine, record, record.newCommitOperation());
            futures.add(future);
        }
        return futures;
    }

    public List<Future> prepare(NodeEngine nodeEngine) {
        List<Future> futures = new ArrayList<Future>(size());
        for (TransactionLogRecord record : recordMap.values()) {
            Future future = invoke(nodeEngine, record, record.newPrepareOperation());
            futures.add(future);
        }
        return futures;
    }

    public List<Future> rollback(NodeEngine nodeEngine) {
        List<Future> futures = new ArrayList<Future>(size());
        ListIterator<TransactionLogRecord> iterator = getRecordList().listIterator(size());
        while (iterator.hasPrevious()) {
            TransactionLogRecord record = iterator.previous();
            Future future = invoke(nodeEngine, record, record.newRollbackOperation());
            futures.add(future);
        }
        return futures;
    }

    private Future invoke(NodeEngine nodeEngine, TransactionLogRecord record, Operation op) {
        OperationService operationService = nodeEngine.getOperationService();
        if (record instanceof TargetAwareTransactionLogRecord) {
            Address target = ((TargetAwareTransactionLogRecord) record).getTarget();
            return operationService.invokeOnTarget(op.getServiceName(), op, target);
        }
        return operationService.invokeOnPartition(op.getServiceName(), op, op.getPartitionId());
    }

    public void commitAsync(NodeEngine nodeEngine, ExecutionCallback callback) {
        for (TransactionLogRecord record : recordMap.values()) {
            invokeAsync(nodeEngine, callback, record, record.newCommitOperation());
        }
    }

    public void rollbackAsync(NodeEngine nodeEngine, ExecutionCallback callback) {
        for (TransactionLogRecord record : recordMap.values()) {
            invokeAsync(nodeEngine, callback, record, record.newRollbackOperation());
        }
    }

    private void invokeAsync(NodeEngine nodeEngine, ExecutionCallback callback,
            TransactionLogRecord record, Operation op) {

        OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();

        if (record instanceof TargetAwareTransactionLogRecord) {
            Address target = ((TargetAwareTransactionLogRecord) record).getTarget();
            operationService.invokeOnTarget(op.getServiceName(), op, target);
        } else {
            operationService.asyncInvokeOnPartition(op.getServiceName(), op, op.getPartitionId(), callback);
        }
    }
}
