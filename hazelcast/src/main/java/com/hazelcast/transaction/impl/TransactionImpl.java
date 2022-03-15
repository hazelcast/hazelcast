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

package com.hazelcast.transaction.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.util.FutureUtil.ExceptionHandler;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionTimedOutException;
import com.hazelcast.transaction.impl.operations.CreateTxBackupLogOperation;
import com.hazelcast.transaction.impl.operations.PurgeTxBackupLogOperation;
import com.hazelcast.transaction.impl.operations.ReplicateTxBackupLogOperation;
import com.hazelcast.transaction.impl.operations.RollbackTxBackupLogOperation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

import static com.hazelcast.internal.util.Clock.currentTimeMillis;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.FutureUtil.RETHROW_TRANSACTION_EXCEPTION;
import static com.hazelcast.internal.util.FutureUtil.logAllExceptions;
import static com.hazelcast.internal.util.FutureUtil.waitUntilAllRespondedWithDeadline;
import static com.hazelcast.internal.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static com.hazelcast.transaction.TransactionOptions.TransactionType;
import static com.hazelcast.transaction.TransactionOptions.TransactionType.ONE_PHASE;
import static com.hazelcast.transaction.TransactionOptions.TransactionType.TWO_PHASE;
import static com.hazelcast.transaction.impl.Transaction.State.ACTIVE;
import static com.hazelcast.transaction.impl.Transaction.State.COMMITTED;
import static com.hazelcast.transaction.impl.Transaction.State.COMMITTING;
import static com.hazelcast.transaction.impl.Transaction.State.COMMIT_FAILED;
import static com.hazelcast.transaction.impl.Transaction.State.NO_TXN;
import static com.hazelcast.transaction.impl.Transaction.State.PREPARED;
import static com.hazelcast.transaction.impl.Transaction.State.PREPARING;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLED_BACK;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLING_BACK;
import static com.hazelcast.transaction.impl.TransactionManagerServiceImpl.SERVICE_NAME;
import static java.lang.Boolean.TRUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings("checkstyle:methodcount")
public class TransactionImpl implements Transaction {

    private static final Address[] EMPTY_ADDRESSES = new Address[0];
    private static final ThreadLocal<Boolean> TRANSACTION_EXISTS = new ThreadLocal<Boolean>();

    private final ExceptionHandler rollbackExceptionHandler;
    private final ExceptionHandler rollbackTxExceptionHandler;
    private final ExceptionHandler replicationTxExceptionHandler;
    private final TransactionManagerServiceImpl transactionManagerService;
    private final NodeEngine nodeEngine;
    private final UUID txnId;
    private final int durability;
    private final TransactionType transactionType;
    private final boolean checkThreadAccess;
    private final ILogger logger;
    private final UUID txOwnerUuid;
    private final TransactionLog transactionLog;
    private Long threadId;
    private long timeoutMillis;
    private State state = NO_TXN;
    private long startTime;
    private Address[] backupAddresses = EMPTY_ADDRESSES;
    private boolean backupLogsCreated;
    private boolean originatedFromClient;

    public TransactionImpl(@Nonnull TransactionManagerServiceImpl transactionManagerService,
                           @Nonnull NodeEngine nodeEngine,
                           @Nonnull TransactionOptions options,
                           @Nullable UUID txOwnerUuid) {
        this(transactionManagerService, nodeEngine, options, txOwnerUuid, false);
    }

    public TransactionImpl(@Nonnull TransactionManagerServiceImpl transactionManagerService,
                           @Nonnull NodeEngine nodeEngine,
                           @Nonnull TransactionOptions options,
                           @Nullable UUID txOwnerUuid,
                           boolean originatedFromClient) {
        this.transactionLog = new TransactionLog();
        this.transactionManagerService = transactionManagerService;
        this.nodeEngine = nodeEngine;
        this.txnId = newUnsecureUUID();
        this.timeoutMillis = options.getTimeoutMillis();
        this.transactionType = options.getTransactionType();
        this.durability = transactionType == ONE_PHASE ? 0 : options.getDurability();
        this.txOwnerUuid = txOwnerUuid == null ? nodeEngine.getLocalMember().getUuid() : txOwnerUuid;
        this.checkThreadAccess = txOwnerUuid == null;

        this.logger = nodeEngine.getLogger(getClass());
        this.rollbackExceptionHandler = logAllExceptions(logger, "Error during rollback!", Level.FINEST);
        this.rollbackTxExceptionHandler = logAllExceptions(logger, "Error during tx rollback backup!", Level.FINEST);
        this.replicationTxExceptionHandler = createReplicationTxExceptionHandler(logger);
        this.originatedFromClient = originatedFromClient;
    }

    // used by tx backups
    TransactionImpl(TransactionManagerServiceImpl transactionManagerService, NodeEngine nodeEngine,
                    UUID txnId, List<TransactionLogRecord> transactionLog, long timeoutMillis,
                    long startTime, UUID txOwnerUuid) {
        this.transactionLog = new TransactionLog(transactionLog);
        this.transactionManagerService = transactionManagerService;
        this.nodeEngine = nodeEngine;
        this.txnId = txnId;
        this.timeoutMillis = timeoutMillis;
        this.startTime = startTime;
        this.durability = 0;
        this.transactionType = TWO_PHASE;
        this.state = PREPARED;
        this.txOwnerUuid = txOwnerUuid;
        this.checkThreadAccess = false;

        this.logger = nodeEngine.getLogger(getClass());
        this.rollbackExceptionHandler = logAllExceptions(logger, "Error during rollback!", Level.FINEST);
        this.rollbackTxExceptionHandler = logAllExceptions(logger, "Error during tx rollback backup!", Level.FINEST);
        this.replicationTxExceptionHandler = createReplicationTxExceptionHandler(logger);
    }

    @Override
    public UUID getTxnId() {
        return txnId;
    }

    public long getStartTime() {
        return startTime;
    }

    @Override
    public UUID getOwnerUuid() {
        return txOwnerUuid;
    }

    public boolean isOriginatedFromClient() {
        return originatedFromClient;
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    protected TransactionLog getTransactionLog() {
        return transactionLog;
    }

    @Override
    public void add(TransactionLogRecord record) {
        if (state != Transaction.State.ACTIVE) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
        checkThread();
        transactionLog.add(record);
    }

    @Override
    public TransactionLogRecord get(Object key) {
        return transactionLog.get(key);
    }

    @Override
    public void remove(Object key) {
        transactionLog.remove(key);
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
        if (TRANSACTION_EXISTS.get() != null) {
            throw new IllegalStateException("Nested transactions are not allowed!");
        }
        startTime = currentTimeMillis();
        backupAddresses = transactionManagerService.pickBackupLogAddresses(durability);

        //init caller thread
        if (threadId == null) {
            threadId = Thread.currentThread().getId();
            setThreadFlag(TRUE);
        }
        state = ACTIVE;
        transactionManagerService.startCount.inc();
    }

    private void setThreadFlag(Boolean flag) {
        if (checkThreadAccess) {
            TRANSACTION_EXISTS.set(flag);
        }
    }

    @Override
    public void prepare() throws TransactionException {
        if (state != ACTIVE) {
            throw new TransactionNotActiveException("Transaction is not active");
        }

        checkThread();
        checkTimeout();
        try {
            createBackupLogs();
            state = PREPARING;
            List<Future> futures = transactionLog.prepare(nodeEngine);
            waitUntilAllRespondedWithDeadline(futures, timeoutMillis, MILLISECONDS, RETHROW_TRANSACTION_EXCEPTION);
            state = PREPARED;
            replicateTxnLog();
        } catch (Throwable e) {
            throw rethrow(e, TransactionException.class);
        }
    }

    /**
     * Checks if this Transaction needs to be prepared.
     * <p>
     * Preparing a transaction costs time since the backup log potentially needs to be copied and
     * each logrecord needs to prepare its content (e.g. by acquiring locks). This takes time.
     * <p>
     * If a transaction is local or if there is 1 or 0 items in the transaction log, instead of
     * preparing, we are just going to try to commit. If the lock is still acquired, the write
     * succeeds, and if the lock isn't acquired, the write fails; this is the same effect as a
     * prepare would have.
     *
     * @return true if {@link #prepare()} is required.
     */
    public boolean requiresPrepare() {
        if (transactionType == ONE_PHASE) {
            return false;
        }

        return transactionLog.size() > 1;
    }

    @Override
    public void commit() throws TransactionException, IllegalStateException {
        try {
            if (transactionType == TWO_PHASE) {
                if (transactionLog.size() > 1) {
                    if (state != PREPARED) {
                        throw new IllegalStateException("Transaction is not prepared");
                    }
                } else {
                    // when a transaction log contains less than 2 items, it can be committed without preparing
                    if (state != PREPARED && state != ACTIVE) {
                        throw new IllegalStateException("Transaction is not prepared or active");
                    }
                }
            } else if (transactionType == ONE_PHASE && state != ACTIVE) {
                throw new IllegalStateException("Transaction is not active");
            }

            checkThread();
            checkTimeout();
            try {
                state = COMMITTING;
                List<Future> futures = transactionLog.commit(nodeEngine);
                waitWithDeadline(futures, Long.MAX_VALUE, MILLISECONDS, RETHROW_TRANSACTION_EXCEPTION);
                state = COMMITTED;
                transactionManagerService.commitCount.inc();
                transactionLog.onCommitSuccess();
            } catch (Throwable e) {
                state = COMMIT_FAILED;
                transactionLog.onCommitFailure();
                throw rethrow(e, TransactionException.class);
            } finally {
                purgeBackupLogs();
            }
        } finally {
            setThreadFlag(null);
        }
    }

    private void checkTimeout() throws TransactionTimedOutException {
        if (startTime + timeoutMillis < currentTimeMillis()) {
            throw new TransactionTimedOutException("Transaction is timed-out!");
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
                //TODO: Do we need both a purge and rollback?
                rollbackBackupLogs();
                List<Future> futures = transactionLog.rollback(nodeEngine);
                waitWithDeadline(futures, Long.MAX_VALUE, MILLISECONDS, rollbackExceptionHandler);
                purgeBackupLogs();
            } catch (Throwable e) {
                throw rethrow(e);
            } finally {
                state = ROLLED_BACK;
                transactionManagerService.rollbackCount.inc();
            }
        } finally {
            setThreadFlag(null);
        }
    }

    private void replicateTxnLog() {
        if (skipBackupLogReplication()) {
            return;
        }

        OperationService operationService = nodeEngine.getOperationService();
        ClusterService clusterService = nodeEngine.getClusterService();
        List<Future> futures = new ArrayList<Future>(backupAddresses.length);
        for (Address backupAddress : backupAddresses) {
            if (clusterService.getMember(backupAddress) != null) {
                Operation op = createReplicateTxBackupLogOperation();
                Future f = operationService.invokeOnTarget(SERVICE_NAME, op, backupAddress);
                futures.add(f);
            }
        }
        waitWithDeadline(futures, timeoutMillis, MILLISECONDS, replicationTxExceptionHandler);
    }

    /**
     * Some data-structures like the Transaction List rely on (empty) backup logs to be created before any change on the
     * data-structure is made. That is why when such a data-structure is loaded, it should the creation.
     * <p>
     * Not every data-structure, e.g. the Transactional Map, relies on it and in some cases can even skip it.
     */
    public void ensureBackupLogsExist() {
        // we can't take the TransactionLog size in consideration because this call can be made before an item
        // is added to the transactionLog.

        if (backupLogsCreated || backupAddresses.length == 0) {
            return;
        }

        forceCreateBackupLogs();
    }

    private void createBackupLogs() {
        if (backupLogsCreated || skipBackupLogReplication()) {
            return;
        }

        forceCreateBackupLogs();
    }

    private void forceCreateBackupLogs() {
        backupLogsCreated = true;
        OperationService operationService = nodeEngine.getOperationService();
        List<Future> futures = new ArrayList<Future>(backupAddresses.length);
        for (Address backupAddress : backupAddresses) {
            if (nodeEngine.getClusterService().getMember(backupAddress) != null) {
                final CreateTxBackupLogOperation op = createCreateTxBackupLogOperation();
                Future f = operationService.invokeOnTarget(SERVICE_NAME, op, backupAddress);
                futures.add(f);
            }
        }

        waitWithDeadline(futures, timeoutMillis, MILLISECONDS, replicationTxExceptionHandler);
    }

    private void rollbackBackupLogs() {
        if (!backupLogsCreated) {
            return;
        }

        OperationService operationService = nodeEngine.getOperationService();
        ClusterService clusterService = nodeEngine.getClusterService();
        List<Future> futures = new ArrayList<Future>(backupAddresses.length);
        for (Address backupAddress : backupAddresses) {
            if (clusterService.getMember(backupAddress) != null) {
                Future f = operationService.invokeOnTarget(SERVICE_NAME, createRollbackTxBackupLogOperation(), backupAddress);
                futures.add(f);
            }
        }

        waitWithDeadline(futures, timeoutMillis, MILLISECONDS, rollbackTxExceptionHandler);
    }

    private void purgeBackupLogs() {
        if (!backupLogsCreated) {
            return;
        }

        OperationService operationService = nodeEngine.getOperationService();
        ClusterService clusterService = nodeEngine.getClusterService();
        for (Address backupAddress : backupAddresses) {
            if (clusterService.getMember(backupAddress) != null) {
                try {
                    operationService.invokeOnTarget(SERVICE_NAME, createPurgeTxBackupLogOperation(), backupAddress);
                } catch (Throwable e) {
                    logger.warning("Error during purging backups!", e);
                }
            }
        }
    }

    private boolean skipBackupLogReplication() {
        //todo: what if backupLogsCreated are created?

        return durability == 0 || transactionLog.size() <= 1 || backupAddresses.length == 0;
    }

    protected CreateTxBackupLogOperation createCreateTxBackupLogOperation() {
        return new CreateTxBackupLogOperation(txOwnerUuid, txnId);
    }

    protected ReplicateTxBackupLogOperation createReplicateTxBackupLogOperation() {
        return new ReplicateTxBackupLogOperation(
                transactionLog.getRecords(), txOwnerUuid, txnId, timeoutMillis, startTime);
    }

    protected RollbackTxBackupLogOperation createRollbackTxBackupLogOperation() {
        return new RollbackTxBackupLogOperation(txnId);
    }

    protected PurgeTxBackupLogOperation createPurgeTxBackupLogOperation() {
        return new PurgeTxBackupLogOperation(txnId);
    }

    @Override
    public TransactionType getTransactionType() {
        return transactionType;
    }

    @Override
    public String toString() {
        return "Transaction{"
                + "txnId='" + txnId + '\''
                + ", state=" + state
                + ", txType=" + transactionType
                + ", timeoutMillis=" + timeoutMillis
                + '}';
    }

    static ExceptionHandler createReplicationTxExceptionHandler(final ILogger logger) {
        return new ExceptionHandler() {
            @Override
            public void handleException(Throwable throwable) {
                if (throwable instanceof TimeoutException) {
                    throw new TransactionTimedOutException(throwable);
                }
                if (throwable instanceof MemberLeftException) {
                    logger.warning("Member left while replicating tx begin: " + throwable);
                    return;
                }
                if (throwable instanceof ExecutionException) {
                    Throwable cause = throwable.getCause();
                    if (cause instanceof TargetNotMemberException || cause instanceof HazelcastInstanceNotActiveException) {
                        logger.warning("Member left while replicating tx begin: " + cause);
                        return;
                    }
                }
                throw rethrow(throwable);
            }
        };
    }
}
