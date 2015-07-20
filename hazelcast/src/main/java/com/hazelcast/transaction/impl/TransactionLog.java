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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
public class TransactionLog {

    private final Map<Object, TransactionLogRecord> recordMap = new HashMap<Object, TransactionLogRecord>();

    public TransactionLog() {
    }

    public TransactionLog(Collection<TransactionLogRecord> transactionLog) {
        for (TransactionLogRecord record : transactionLog) {
            recordMap.put(record.getKey(), record);
        }
    }

    /**
     * Adds a record to this TransactionLog.
     *
     * If a previous record for the same key exist, it will be overwritten.
     *
     * @param record the record to add.
     */
    public void add(TransactionLogRecord record) {
        Object key = record.getKey();
        recordMap.put(key, record);
    }

    /**
     * Gets the TransactionLogRecord with the given key.
     *
     * @param key the key of the TransactionLogRecord
     * @return the found TransactionLogRecord or null if no record is found for that key.
     */
    public TransactionLogRecord get(Object key) {
        return recordMap.get(key);
    }

    public Collection<TransactionLogRecord> getRecords() {
        return recordMap.values();
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
            Future future = invoke(nodeEngine, record.newCommitOperation());
            futures.add(future);
        }
        return futures;
    }

    public List<Future> prepare(NodeEngine nodeEngine) {
        List<Future> futures = new ArrayList<Future>(size());
        for (TransactionLogRecord record : recordMap.values()) {
            Future future = invoke(nodeEngine, record.newPrepareOperation());
            futures.add(future);
        }
        return futures;
    }

    public List<Future> rollback(NodeEngine nodeEngine) {
        List<Future> futures = new ArrayList<Future>(size());
        for (TransactionLogRecord record : recordMap.values()) {
            Future future = invoke(nodeEngine, record.newRollbackOperation());
            futures.add(future);
        }
        return futures;
    }

    private Future invoke(NodeEngine nodeEngine, Operation op) {
        OperationService operationService = nodeEngine.getOperationService();

        return operationService.invokeOnPartition(
                op.getServiceName(),
                op,
                op.getPartitionId());
    }

    public void commitAsync(NodeEngine nodeEngine, ExecutionCallback callback) {
        for (TransactionLogRecord record : recordMap.values()) {
            invokeAsync(nodeEngine, callback, record.newCommitOperation());
        }
    }

    public void rollbackAsync(NodeEngine nodeEngine, ExecutionCallback callback) {
        for (TransactionLogRecord record : recordMap.values()) {
            invokeAsync(nodeEngine, callback, record.newRollbackOperation());
        }
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
