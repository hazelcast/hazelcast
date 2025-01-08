/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

/**
 * The transaction log contains all {@link
 * TransactionLogRecord} for a given transaction.
 * <p>
 * If within a transaction 3 map.puts would be done on different
 * keys and 1 queue.take would be done, then the TransactionLog
 * will contain 4 {@link TransactionLogRecord} instances.
 * <p>
 * Planned optimization:
 * Most transaction will be small, but an HashMap is created.
 * Instead, use an array and do a linear search in that array.
 * When there are too many items added, then enable the hashmap.
 */
public class TransactionLog {

    private final Map<Object, TransactionLogRecord> recordMap = new HashMap<>();

    public TransactionLog() {
    }

    public TransactionLog(Collection<TransactionLogRecord> transactionLog) {
        for (TransactionLogRecord transactionLogRecord : transactionLog) {
            add(transactionLogRecord);
        }
    }

    public void add(TransactionLogRecord transactionLogRecord) {
        Object key = transactionLogRecord.getKey();
        if (key == null) {
            key = new Object();
        }
        recordMap.put(key, transactionLogRecord);
    }

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

    public List<Future<Object>> commit(NodeEngine nodeEngine) {
        List<Future<Object>> futures = new ArrayList<>(size());
        for (TransactionLogRecord transactionLogRecord : recordMap.values()) {
            Future<Object> future = invoke(nodeEngine, transactionLogRecord, transactionLogRecord.newCommitOperation());
            futures.add(future);
        }
        return futures;
    }

    public void onCommitSuccess() {
        for (TransactionLogRecord transactionLogRecord : recordMap.values()) {
            transactionLogRecord.onCommitSuccess();
        }
    }

    public void onCommitFailure() {
        for (TransactionLogRecord transactionLogRecord : recordMap.values()) {
            transactionLogRecord.onCommitFailure();
        }
    }

    public List<Future<Object>> prepare(NodeEngine nodeEngine) {
        List<Future<Object>> futures = new ArrayList<>(size());
        for (TransactionLogRecord transactionLogRecord : recordMap.values()) {
            Future<Object> future = invoke(nodeEngine, transactionLogRecord, transactionLogRecord.newPrepareOperation());
            futures.add(future);
        }
        return futures;
    }

    public List<Future<Object>> rollback(NodeEngine nodeEngine) {
        List<Future<Object>> futures = new ArrayList<>(size());
        for (TransactionLogRecord transactionLogRecord : recordMap.values()) {
            Future<Object> future = invoke(nodeEngine, transactionLogRecord, transactionLogRecord.newRollbackOperation());
            futures.add(future);
        }
        return futures;
    }

    private Future<Object> invoke(NodeEngine nodeEngine, TransactionLogRecord transactionLogRecord, Operation op) {
        OperationService operationService = nodeEngine.getOperationService();
        if (transactionLogRecord instanceof TargetAwareTransactionLogRecord logRecord) {
            Address target = logRecord.getTarget();
            return operationService.invokeOnTarget(op.getServiceName(), op, target);
        }
        return operationService.invokeOnPartition(op.getServiceName(), op, op.getPartitionId());
    }

    public void commitAsync(NodeEngine nodeEngine, BiConsumer callback) {
        for (TransactionLogRecord transactionLogRecord : recordMap.values()) {
            invokeAsync(nodeEngine, callback, transactionLogRecord, transactionLogRecord.newCommitOperation());
        }
    }

    public void rollbackAsync(NodeEngine nodeEngine, BiConsumer callback) {
        for (TransactionLogRecord transactionLogRecord : recordMap.values()) {
            invokeAsync(nodeEngine, callback, transactionLogRecord, transactionLogRecord.newRollbackOperation());
        }
    }

    private void invokeAsync(NodeEngine nodeEngine, BiConsumer callback,
                             TransactionLogRecord transactionLogRecord, Operation op) {

        OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();

        if (transactionLogRecord instanceof TargetAwareTransactionLogRecord logRecord) {
            Address target = logRecord.getTarget();
            operationService.invokeOnTarget(op.getServiceName(), op, target);
        } else {
            operationService.invokeOnPartitionAsync(op.getServiceName(), op, op.getPartitionId())
                    .whenCompleteAsync(callback, ConcurrencyUtil.getDefaultAsyncExecutor());
        }
    }
}
