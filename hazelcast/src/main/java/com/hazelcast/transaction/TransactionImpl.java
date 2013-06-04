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

package com.hazelcast.transaction;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.transaction.Transaction.State.*;
import static com.hazelcast.transaction.TransactionOptions.TransactionType;

final class TransactionImpl implements Transaction {

    private static final ThreadLocal<Boolean> threadFlag = new ThreadLocal<Boolean>();

    private final TransactionManagerServiceImpl transactionManagerService;
    private final NodeEngine nodeEngine;
    private final List<TransactionLog> txLogs = new LinkedList<TransactionLog>();
    private final Map<Object, TransactionLog> txLogMap = new HashMap<Object, TransactionLog>();
    private final String txnId;
    private final long threadId = Thread.currentThread().getId();
    private final long timeoutMillis;
    private final int durability;
    private final TransactionType transactionType;
    private State state = NO_TXN;
    private long startTime = 0L;
    private Address[] backupAddresses;

    public TransactionImpl(TransactionManagerServiceImpl transactionManagerService, NodeEngine nodeEngine, TransactionOptions options) {
        this.transactionManagerService = transactionManagerService;
        this.nodeEngine = nodeEngine;
        this.txnId = UUID.randomUUID().toString();
        this.timeoutMillis = options.getTimeoutMillis();
        this.durability = options.getDurability();
        this.transactionType = options.getTransactionType();
    }

    // used by tx backups
    TransactionImpl(TransactionManagerServiceImpl transactionManagerService, NodeEngine nodeEngine,
                    String txnId, List<TransactionLog> txLogs, long timeoutMillis, long startTime) {
        this.transactionManagerService = transactionManagerService;
        this.nodeEngine = nodeEngine;
        this.txnId = txnId;
        this.timeoutMillis = timeoutMillis;
        this.startTime = startTime;
        this.durability = 0;
        this.transactionType = TransactionType.TWO_PHASE;
        this.txLogs.addAll(txLogs);
        this.state = PREPARED;
    }

    public String getTxnId() {
        return txnId;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void addTransactionLog(TransactionLog transactionLog) {
        if (state != Transaction.State.ACTIVE) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
        checkThread();
        // there should be just one tx log for the same key. so if there is older we are removing it
        if(transactionLog instanceof KeyAwareTransactionLog) {
            KeyAwareTransactionLog keyAwareTransactionLog = (KeyAwareTransactionLog) transactionLog;
            TransactionLog removed = txLogMap.remove(keyAwareTransactionLog.getKey());
            txLogs.remove(removed);
        }

        txLogs.add(transactionLog);
        if (transactionLog instanceof KeyAwareTransactionLog){
            KeyAwareTransactionLog keyAwareTransactionLog = (KeyAwareTransactionLog)transactionLog;
            txLogMap.put(keyAwareTransactionLog.getKey(), keyAwareTransactionLog);
        }
    }

    public TransactionLog getTransactionLog(Object key){
        return txLogMap.get(key);
    }

    public void removeTransactionLog(Object key){
        TransactionLog removed = txLogMap.remove(key);
        if (removed != null){
            txLogs.remove(removed);
        }
    }

    private void checkThread() {
        if (threadId != Thread.currentThread().getId()) {
            throw new IllegalStateException("Transaction cannot span multiple threads!");
        }
    }

    void begin() throws IllegalStateException {
        if (state == ACTIVE) {
            throw new IllegalStateException("Transaction is already active");
        }
        checkThread();
        if (threadFlag.get() != null) {
            throw new IllegalStateException("Nested transactions are not allowed!");
        }
        threadFlag.set(true);
        startTime = Clock.currentTimeMillis();
        backupAddresses = transactionManagerService.pickBackupAddresses(durability);
        state = ACTIVE;
    }

    void prepare() throws TransactionException {
        if (state != ACTIVE) {
            throw new TransactionNotActiveException("Transaction is not active");
        }
        checkThread();
        checkTimeout();
        try {
            final List<Future> futures = new ArrayList<Future>(txLogs.size());
                state = PREPARING;
                for (TransactionLog txLog : txLogs) {
                    futures.add(txLog.prepare(nodeEngine));
                }
                for (Future future : futures) {
                    future.get(timeoutMillis, TimeUnit.MILLISECONDS);
                }
                futures.clear();
                state = PREPARED;
            // replicate tx log
            if (durability > 0) {
                final OperationService operationService = nodeEngine.getOperationService();
                for (Address backupAddress : backupAddresses) {
                    if (nodeEngine.getClusterService().getMember(backupAddress) != null){
                        final Invocation inv = operationService.createInvocationBuilder(TransactionManagerServiceImpl.SERVICE_NAME,
                                new ReplicateTxOperation(txLogs, nodeEngine.getLocalMember().getUuid(), txnId, timeoutMillis, startTime),
                                backupAddress).build();
                        futures.add(inv.invoke());
                    }
                }
                for (Future future : futures) {
                    future.get(timeoutMillis, TimeUnit.MILLISECONDS);
                }
                futures.clear();
            }

        } catch (Throwable e) {
            if (e instanceof ExecutionException && e.getCause() instanceof TransactionException) {
                throw (TransactionException) e.getCause();
            }
            throw ExceptionUtil.rethrow(e);
        }
    }

    void commit() throws TransactionException, IllegalStateException {
        if (transactionType.equals(TransactionType.TWO_PHASE) && state != PREPARED) {
            throw new IllegalStateException("Transaction is not prepared");
        }
        if (transactionType.equals(TransactionType.LOCAL) && state != ACTIVE) {
            throw new IllegalStateException("Transaction is not active");
        }
        checkThread();
        checkTimeout();
        try {
            final List<Future> futures = new ArrayList<Future>(txLogs.size());
            state = COMMITTING;
            for (TransactionLog txLog : txLogs) {
                futures.add(txLog.commit(nodeEngine));
            }
            for (Future future : futures) {
                try {
                    future.get(5, TimeUnit.MINUTES);
                } catch (Throwable e) {
                    nodeEngine.getLogger(getClass()).log(Level.WARNING, "Error during commit!", e);
                }
            }
            state = COMMITTED;

            // purge tx backup
            purgeTxBackups();
        } catch (Throwable e) {
            state = COMMIT_FAILED;
            if (e instanceof ExecutionException && e.getCause() instanceof TransactionException) {
                throw (TransactionException) e.getCause();
            }
            throw ExceptionUtil.rethrow(e);
        } finally {
            threadFlag.set(null);
        }
    }

    private void checkTimeout() throws TransactionException {
        if (startTime + timeoutMillis < Clock.currentTimeMillis()) {
            throw new TransactionException("Transaction is timed-out!");
        }
    }

    void rollback() throws IllegalStateException {
        if (state == NO_TXN || state == ROLLED_BACK) {
            throw new IllegalStateException("Transaction is not active");
        }
        checkThread();
        state = ROLLING_BACK;
        try {
            final List<Future> futures = new ArrayList<Future>(txLogs.size());
            final OperationService operationService = nodeEngine.getOperationService();

            // rollback tx backup
            if (durability > 0 && transactionType.equals(TransactionType.TWO_PHASE)) {
                for (Address backupAddress : backupAddresses) {
                    if (nodeEngine.getClusterService().getMember(backupAddress) != null){
                        final Invocation inv = operationService.createInvocationBuilder(TransactionManagerServiceImpl.SERVICE_NAME,
                                new RollbackTxBackupOperation(txnId), backupAddress).build();
                        futures.add(inv.invoke());
                    }
                }
                for (Future future : futures) {
                    try {
                        future.get(timeoutMillis, TimeUnit.MILLISECONDS);
                    } catch (Throwable e) {
                        nodeEngine.getLogger(getClass()).log(Level.WARNING, "Error during tx rollback backup!", e);
                    }
                }
                futures.clear();
            }

            final ListIterator<TransactionLog> iter = txLogs.listIterator(txLogs.size());
            while (iter.hasPrevious()) {
                final TransactionLog txLog = iter.previous();
                futures.add(txLog.rollback(nodeEngine));
            }
            for (Future future : futures) {
                try {
                    future.get(5, TimeUnit.MINUTES);
                } catch (Throwable e) {
                    nodeEngine.getLogger(getClass()).log(Level.WARNING, "Error during rollback!", e);
                }
            }
            // purge tx backup
            purgeTxBackups();
        } catch (Throwable e) {
            throw ExceptionUtil.rethrow(e);
        } finally {
            state = ROLLED_BACK;
            threadFlag.set(null);
        }
    }

    private void purgeTxBackups() {
        if (durability > 0 && transactionType.equals(TransactionType.TWO_PHASE)) {
            final OperationService operationService = nodeEngine.getOperationService();
            for (Address backupAddress : backupAddresses) {
                if (nodeEngine.getClusterService().getMember(backupAddress) != null){
                    try {
                        final Invocation inv = operationService.createInvocationBuilder(TransactionManagerServiceImpl.SERVICE_NAME,
                                new PurgeTxBackupOperation(txnId), backupAddress).build();
                        inv.invoke();
                    } catch (Throwable e) {
                        nodeEngine.getLogger(getClass()).log(Level.WARNING, "Error during purging backups!", e);
                    }
                }
            }
        }
    }

    public State getState() {
        return state;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Transaction");
        sb.append("{txnId='").append(txnId).append('\'');
        sb.append(", state=").append(state);
        sb.append(", txType=").append(transactionType.value);
        sb.append(", timeoutMillis=").append(timeoutMillis);
        sb.append('}');
        return sb.toString();
    }
}
