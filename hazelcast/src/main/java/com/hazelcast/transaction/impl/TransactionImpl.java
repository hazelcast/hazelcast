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
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
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

final class TransactionImpl implements Transaction, TransactionSupport {

    private static final ThreadLocal<Boolean> threadFlag = new ThreadLocal<Boolean>();

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
    private long startTime = 0L;
    private Address[] backupAddresses;
    private SerializableXID xid = null;

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
    }

    public void setXid(SerializableXID xid) {
        this.xid = xid;
    }

    public SerializableXID getXid() {
        return xid;
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

    public void begin() throws IllegalStateException {
        if (state == ACTIVE) {
            throw new IllegalStateException("Transaction is already active");
        }
        if (threadFlag.get() != null) {
            throw new IllegalStateException("Nested transactions are not allowed!");
        }
        startTime = Clock.currentTimeMillis();
        backupAddresses = transactionManagerService.pickBackupAddresses(durability);

        if (durability > 0 && backupAddresses != null && transactionType == TransactionType.TWO_PHASE) {
            final OperationService operationService = nodeEngine.getOperationService();
            List<Future> futures = new ArrayList<Future>(backupAddresses.length);
            for (Address backupAddress : backupAddresses) {
                if (nodeEngine.getClusterService().getMember(backupAddress) != null) {
                    final Future f = operationService.invokeOnTarget(TransactionManagerServiceImpl.SERVICE_NAME,
                            new BeginTxBackupOperation(txOwnerUuid, txnId, xid), backupAddress);
                    futures.add(f);
                }
            }
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

        //init caller thread
        if (threadId == null) {
            threadId = Thread.currentThread().getId();
            setThreadFlag(Boolean.TRUE);
        }
        state = ACTIVE;
    }

    private void setThreadFlag(Boolean flag) {
        if (checkThreadAccess) {
            threadFlag.set(flag);
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
            for (Future future : futures) {
                future.get(timeoutMillis, TimeUnit.MILLISECONDS);
            }
            futures.clear();
            state = PREPARED;
            // replicate tx log
            if (durability > 0) {
                final OperationService operationService = nodeEngine.getOperationService();
                for (Address backupAddress : backupAddresses) {
                    if (nodeEngine.getClusterService().getMember(backupAddress) != null) {
                        final Future f = operationService.invokeOnTarget(TransactionManagerServiceImpl.SERVICE_NAME,
                                new ReplicateTxOperation(txLogs, txOwnerUuid, txnId, timeoutMillis, startTime),
                                backupAddress);
                        futures.add(f);
                    }
                }
                for (Future future : futures) {
                    future.get(timeoutMillis, TimeUnit.MILLISECONDS);
                }
                futures.clear();
            }

        } catch (Throwable e) {
            throw ExceptionUtil.rethrow(e, TransactionException.class);
        }
    }

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
                for (Future future : futures) {
                    try {
                        future.get(5, TimeUnit.MINUTES);
                    } catch (Throwable e) {
                        nodeEngine.getLogger(getClass()).warning("Error during commit!", e);
                    }
                }
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

    public void rollback() throws IllegalStateException {
        try {
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
                        if (nodeEngine.getClusterService().getMember(backupAddress) != null) {
                            final Future f = operationService.invokeOnTarget(TransactionManagerServiceImpl.SERVICE_NAME,
                                    new RollbackTxBackupOperation(txnId), backupAddress);
                            futures.add(f);
                        }
                    }
                    for (Future future : futures) {
                        try {
                            future.get(timeoutMillis, TimeUnit.MILLISECONDS);
                        } catch (Throwable e) {
                            nodeEngine.getLogger(getClass()).warning("Error during tx rollback backup!", e);
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
                        nodeEngine.getLogger(getClass()).warning("Error during rollback!", e);
                    }
                }
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

    public State getState() {
        return state;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public boolean setTimeoutMillis(long timeoutMillis) {
        if (state == NO_TXN) {
            this.timeoutMillis = timeoutMillis;
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
