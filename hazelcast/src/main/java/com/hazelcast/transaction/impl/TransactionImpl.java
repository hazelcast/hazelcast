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

package com.hazelcast.transaction.impl;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.util.UuidUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import static com.hazelcast.transaction.TransactionOptions.TransactionType;
import static com.hazelcast.transaction.impl.Transaction.State.ACTIVE;
import static com.hazelcast.transaction.impl.Transaction.State.COMMITTED;
import static com.hazelcast.transaction.impl.Transaction.State.COMMITTING;
import static com.hazelcast.transaction.impl.Transaction.State.COMMIT_FAILED;
import static com.hazelcast.transaction.impl.Transaction.State.NO_TXN;
import static com.hazelcast.transaction.impl.Transaction.State.PREPARED;
import static com.hazelcast.transaction.impl.Transaction.State.PREPARING;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLED_BACK;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLING_BACK;
import static com.hazelcast.util.FutureUtil.ExceptionHandler;
import static com.hazelcast.util.FutureUtil.logAllExceptions;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;

final class TransactionImpl implements Transaction, TransactionSupport {

    private static final ThreadLocal<Boolean> THREAD_FLAG = new ThreadLocal<Boolean>();
    private static final int ROLLBACK_TIMEOUT_MINUTES = 5;
    private static final int COMMIT_TIMEOUT_MINUTES = 5;

    private final ExceptionHandler commitExceptionHandler;
    private final ExceptionHandler rollbackExceptionHandler;
    private final ExceptionHandler rollbackTxExceptionHandler;

    private final TransactionManagerServiceImpl transactionManagerService;
    private final NodeEngine nodeEngine;
    private final List<TransactionLog> txLogs = new LinkedList<TransactionLog>();
    private final Map<Object, TransactionLog> txLogMap = new HashMap<Object, TransactionLog>();
    private final String txnId;
    private Long threadId;
    private long timeoutMillis;
    private final int durability;
    private final TransactionType transactionType;
    private final String txOwnerUuid;
    private final boolean checkThreadAccess;
    private State state = NO_TXN;
    private long startTime;
    private Address[] backupAddresses;
    private SerializableXID xid;

    public TransactionImpl(TransactionManagerServiceImpl transactionManagerService, NodeEngine nodeEngine,
                           TransactionOptions options, String txOwnerUuid) {
        this.transactionManagerService = transactionManagerService;
        this.nodeEngine = nodeEngine;
        this.txnId = UuidUtil.buildRandomUuidString();
        this.timeoutMillis = options.getTimeoutMillis();
        this.durability = options.getDurability();
        this.transactionType = options.getTransactionType();
        this.txOwnerUuid = txOwnerUuid == null ? nodeEngine.getLocalMember().getUuid() : txOwnerUuid;
        this.checkThreadAccess = txOwnerUuid == null;

        ILogger logger = nodeEngine.getLogger(getClass());
        this.commitExceptionHandler = logAllExceptions(logger, "Error during commit!", Level.WARNING);
        this.rollbackExceptionHandler = logAllExceptions(logger, "Error during rollback!", Level.WARNING);
        this.rollbackTxExceptionHandler = logAllExceptions(logger, "Error during tx rollback backup!", Level.WARNING);
    }

    // used by tx backups
    TransactionImpl(TransactionManagerServiceImpl transactionManagerService, NodeEngine nodeEngine,
                    String txnId, List<TransactionLog> txLogs, long timeoutMillis, long startTime, String txOwnerUuid) {
        this.transactionManagerService = transactionManagerService;
        this.nodeEngine = nodeEngine;
        this.txnId = txnId;
        this.timeoutMillis = timeoutMillis;
        this.startTime = startTime;
        this.durability = 0;
        this.transactionType = TransactionType.TWO_PHASE;
        this.txLogs.addAll(txLogs);
        this.state = PREPARED;
        this.txOwnerUuid = txOwnerUuid;
        this.checkThreadAccess = false;

        ILogger logger = nodeEngine.getLogger(getClass());
        this.commitExceptionHandler = logAllExceptions(logger, "Error during commit!", Level.WARNING);
        this.rollbackExceptionHandler = logAllExceptions(logger, "Error during rollback!", Level.WARNING);
        this.rollbackTxExceptionHandler = logAllExceptions(logger, "Error during tx rollback backup!", Level.WARNING);
    }

    public void setXid(SerializableXID xid) {
        this.xid = xid;
    }

    public SerializableXID getXid() {
        return xid;
    }

    @Override
    public String getTxnId() {
        return txnId;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    @Override
    public void addTransactionLog(TransactionLog transactionLog) {
        if (state != Transaction.State.ACTIVE) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
        checkThread();
        // there should be just one tx log for the same key. so if there is older we are removing it
        if (transactionLog instanceof KeyAwareTransactionLog) {
            KeyAwareTransactionLog keyAwareTransactionLog = (KeyAwareTransactionLog) transactionLog;
            TransactionLog removed = txLogMap.remove(keyAwareTransactionLog.getKey());
            if (removed != null) {
                txLogs.remove(removed);
            }
        }

        txLogs.add(transactionLog);
        if (transactionLog instanceof KeyAwareTransactionLog) {
            KeyAwareTransactionLog keyAwareTransactionLog = (KeyAwareTransactionLog) transactionLog;
            txLogMap.put(keyAwareTransactionLog.getKey(), keyAwareTransactionLog);
        }
    }

    public TransactionLog getTransactionLog(Object key) {
        return txLogMap.get(key);
    }

    public List<TransactionLog> getTxLogs() {
        return txLogs;
    }

    public void removeTransactionLog(Object key) {
        TransactionLog removed = txLogMap.remove(key);
        if (removed != null) {
            txLogs.remove(removed);
        }
    }

    private void checkThread() {
        if (checkThreadAccess && threadId != null && threadId.longValue() != Thread.currentThread().getId()) {
            throw new IllegalStateException("Transaction cannot span multiple threads!");
        }
    }

    @Override
    public void begin() throws IllegalStateException {
        if (state == ACTIVE) {
            throw new IllegalStateException("Transaction is already active");
        }
        if (THREAD_FLAG.get() != null) {
            throw new IllegalStateException("Nested transactions are not allowed!");
        }
        startTime = Clock.currentTimeMillis();
        backupAddresses = transactionManagerService.pickBackupAddresses(durability);

        if (durability > 0 && backupAddresses != null && transactionType == TransactionType.TWO_PHASE) {
            List<Future> futures = startTxBackup();
            awaitTxBackupCompletion(futures);
        }

        //init caller thread
        if (threadId == null) {
            threadId = Thread.currentThread().getId();
            setThreadFlag(Boolean.TRUE);
        }
        state = ACTIVE;
    }

    private void awaitTxBackupCompletion(List<Future> futures) {
        for (Future future : futures) {
            try {
                future.get(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (MemberLeftException e) {
                nodeEngine.getLogger(Transaction.class).warning("Member left while replicating tx begin: " + e);
            } catch (Throwable e) {
                if (e instanceof ExecutionException) {
                    e = e.getCause() != null ? e.getCause() : e;
                }
                if (e instanceof TargetNotMemberException) {
                    nodeEngine.getLogger(Transaction.class).warning("Member left while replicating tx begin: " + e);
                } else {
                    throw ExceptionUtil.rethrow(e);
                }
            }
        }
    }

    private List<Future> startTxBackup() {
        final OperationService operationService = nodeEngine.getOperationService();
        List<Future> futures = new ArrayList<Future>(backupAddresses.length);
        for (Address backupAddress : backupAddresses) {
            if (nodeEngine.getClusterService().getMember(backupAddress) != null) {
                final Future f = operationService.invokeOnTarget(TransactionManagerServiceImpl.SERVICE_NAME,
                        new BeginTxBackupOperation(txOwnerUuid, txnId, xid), backupAddress);
                futures.add(f);
            }
        }
        return futures;
    }

    private void setThreadFlag(Boolean flag) {
        if (checkThreadAccess) {
            THREAD_FLAG.set(flag);
        }
    }

    public void prepare() throws TransactionException {
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
            waitWithDeadline(futures, timeoutMillis, TimeUnit.MILLISECONDS, FutureUtil.RETHROW_EVERYTHING);
            futures.clear();
            state = PREPARED;
            if (durability > 0) {
                replicateTxnLog();
            }
        } catch (Throwable e) {
            throw ExceptionUtil.rethrow(e, TransactionException.class);
        }
    }

    private void replicateTxnLog() throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
        final List<Future> futures = new ArrayList<Future>(txLogs.size());
        final OperationService operationService = nodeEngine.getOperationService();
        for (Address backupAddress : backupAddresses) {
            if (nodeEngine.getClusterService().getMember(backupAddress) != null) {
                final Future f = operationService.invokeOnTarget(TransactionManagerServiceImpl.SERVICE_NAME,
                        new ReplicateTxOperation(txLogs, txOwnerUuid, txnId, timeoutMillis, startTime),
                        backupAddress);
                futures.add(f);
            }
        }
        waitWithDeadline(futures, timeoutMillis, TimeUnit.MILLISECONDS, FutureUtil.RETHROW_EVERYTHING);
        futures.clear();
    }

    @Override
    public void commit() throws TransactionException, IllegalStateException {
        try {
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
                waitWithDeadline(futures, COMMIT_TIMEOUT_MINUTES, TimeUnit.MINUTES, commitExceptionHandler);

                state = COMMITTED;

                // purge tx backup
                purgeTxBackups();
            } catch (Throwable e) {
                state = COMMIT_FAILED;
                throw ExceptionUtil.rethrow(e, TransactionException.class);
            }
        } finally {
            setThreadFlag(null);
        }
    }

    private void checkTimeout() throws TransactionException {
        if (startTime + timeoutMillis < Clock.currentTimeMillis()) {
            throw new TransactionException("Transaction is timed-out!");
        }
    }

    @Override
    public void rollback() throws IllegalStateException {
        try {
            if (state == NO_TXN || state == ROLLED_BACK) {
                throw new IllegalStateException("Transaction is not active");
            }
            checkThread();
            state = ROLLING_BACK;
            try {
                rollbackTxBackup();

                final List<Future> futures = new ArrayList<Future>(txLogs.size());
                final ListIterator<TransactionLog> iterator = txLogs.listIterator(txLogs.size());
                while (iterator.hasPrevious()) {
                    final TransactionLog txLog = iterator.previous();
                    futures.add(txLog.rollback(nodeEngine));
                }
                waitWithDeadline(futures, ROLLBACK_TIMEOUT_MINUTES, TimeUnit.MINUTES, rollbackExceptionHandler);
                // purge tx backup
                purgeTxBackups();
            } catch (Throwable e) {
                throw ExceptionUtil.rethrow(e);
            } finally {
                state = ROLLED_BACK;
            }
        } finally {
            setThreadFlag(null);
        }

    }

    private void rollbackTxBackup() {
        final OperationService operationService = nodeEngine.getOperationService();
        final List<Future> futures = new ArrayList<Future>(txLogs.size());
        // rollback tx backup
        if (durability > 0 && transactionType.equals(TransactionType.TWO_PHASE)) {
            for (Address backupAddress : backupAddresses) {
                if (nodeEngine.getClusterService().getMember(backupAddress) != null) {
                    final Future f = operationService.invokeOnTarget(TransactionManagerServiceImpl.SERVICE_NAME,
                            new RollbackTxBackupOperation(txnId), backupAddress);
                    futures.add(f);
                }
            }

            try {
                waitWithDeadline(futures, timeoutMillis, TimeUnit.MILLISECONDS, rollbackTxExceptionHandler);
            } catch (TimeoutException e) {
                ILogger logger = nodeEngine.getLogger(getClass());
                logger.warning("Timeout during tx rollback backup!", e);
            }
            futures.clear();
        }
    }

    public void setRollbackOnly() {
        state = ROLLING_BACK;
    }

    private void purgeTxBackups() {
        if (durability > 0 && transactionType.equals(TransactionType.TWO_PHASE)) {
            final OperationService operationService = nodeEngine.getOperationService();
            for (Address backupAddress : backupAddresses) {
                if (nodeEngine.getClusterService().getMember(backupAddress) != null) {
                    try {
                        operationService.invokeOnTarget(TransactionManagerServiceImpl.SERVICE_NAME,
                                new PurgeTxBackupOperation(txnId), backupAddress);
                    } catch (Throwable e) {
                        nodeEngine.getLogger(getClass()).warning("Error during purging backups!", e);
                    }
                }
            }
        }
    }

    public long getStartTime() {
        return startTime;
    }

    public String getOwnerUuid() {
        return txOwnerUuid;
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public boolean setTimeoutMillis(long timeoutMillis) {

        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("Timeout can not be negative!");
        }

        if (state == NO_TXN && getTimeoutMillis() != timeoutMillis) {

            if (timeoutMillis == 0) {
                this.timeoutMillis = TransactionOptions.DEFAULT_TIMEOUT_MILLIS;
            } else {
                this.timeoutMillis = timeoutMillis;
            }
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Transaction");
        sb.append("{txnId='").append(txnId).append('\'');
        sb.append(", state=").append(state);
        sb.append(", txType=").append(transactionType);
        sb.append(", timeoutMillis=").append(timeoutMillis);
        sb.append('}');
        return sb.toString();
    }
}
