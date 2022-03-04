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

package com.hazelcast.transaction.impl.xa;

import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionLog;
import com.hazelcast.transaction.impl.TransactionLogRecord;
import com.hazelcast.transaction.impl.xa.operations.PutRemoteTransactionOperation;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.logging.Level;

import static com.hazelcast.internal.util.FutureUtil.RETHROW_TRANSACTION_EXCEPTION;
import static com.hazelcast.internal.util.FutureUtil.logAllExceptions;
import static com.hazelcast.internal.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.transaction.impl.Transaction.State.ACTIVE;
import static com.hazelcast.transaction.impl.Transaction.State.COMMITTED;
import static com.hazelcast.transaction.impl.Transaction.State.COMMITTING;
import static com.hazelcast.transaction.impl.Transaction.State.COMMIT_FAILED;
import static com.hazelcast.transaction.impl.Transaction.State.NO_TXN;
import static com.hazelcast.transaction.impl.Transaction.State.PREPARED;
import static com.hazelcast.transaction.impl.Transaction.State.PREPARING;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLED_BACK;
import static com.hazelcast.transaction.impl.Transaction.State.ROLLING_BACK;
import static com.hazelcast.transaction.impl.xa.XAService.SERVICE_NAME;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * XA {@link Transaction} implementation.
 * <p>
 * This class does not need to be thread-safe, it is only used via XAResource
 * All visibility guarantees handled by XAResource
 */
public final class XATransaction implements Transaction {

    private static final int ROLLBACK_TIMEOUT_MINUTES = 5;
    private static final int COMMIT_TIMEOUT_MINUTES = 5;

    private final FutureUtil.ExceptionHandler commitExceptionHandler;
    private final FutureUtil.ExceptionHandler rollbackExceptionHandler;

    private final NodeEngine nodeEngine;
    private final long timeoutMillis;
    private final UUID txnId;
    private final SerializableXID xid;
    private final UUID txOwnerUuid;
    private final TransactionLog transactionLog;

    private State state = NO_TXN;
    private long startTime;

    private boolean originatedFromClient;

    public XATransaction(NodeEngine nodeEngine, Xid xid, UUID txOwnerUuid, int timeout, boolean originatedFromClient) {
        this.nodeEngine = nodeEngine;
        this.transactionLog = new TransactionLog();
        this.timeoutMillis = SECONDS.toMillis(timeout);
        this.txnId = UuidUtil.newUnsecureUUID();
        this.xid = new SerializableXID(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());
        this.txOwnerUuid = txOwnerUuid == null ? nodeEngine.getLocalMember().getUuid() : txOwnerUuid;

        ILogger logger = nodeEngine.getLogger(getClass());
        this.commitExceptionHandler = logAllExceptions(logger, "Error during commit!", Level.WARNING);
        this.rollbackExceptionHandler = logAllExceptions(logger, "Error during rollback!", Level.WARNING);

        this.originatedFromClient = originatedFromClient;
    }

    public XATransaction(NodeEngine nodeEngine, Collection<TransactionLogRecord> logs,
                         UUID txnId, SerializableXID xid, UUID txOwnerUuid, long timeoutMillis, long startTime) {
        this.nodeEngine = nodeEngine;
        this.transactionLog = new TransactionLog(logs);
        this.timeoutMillis = timeoutMillis;
        this.txnId = txnId;
        this.xid = xid;
        this.txOwnerUuid = txOwnerUuid;

        ILogger logger = nodeEngine.getLogger(getClass());
        this.commitExceptionHandler = logAllExceptions(logger, "Error during commit!", Level.WARNING);
        this.rollbackExceptionHandler = logAllExceptions(logger, "Error during rollback!", Level.WARNING);

        this.startTime = startTime;
        state = PREPARED;
    }

    @Override
    public void begin() throws IllegalStateException {
        if (state == ACTIVE) {
            throw new IllegalStateException("Transaction is already active");
        }
        startTime = Clock.currentTimeMillis();
        state = ACTIVE;
    }

    @Override
    public void prepare() throws TransactionException {
        if (state != ACTIVE) {
            throw new TransactionNotActiveException("Transaction is not active");
        }
        checkTimeout();
        try {
            state = PREPARING;
            List<Future> futures = transactionLog.prepare(nodeEngine);
            waitWithDeadline(futures, timeoutMillis, MILLISECONDS, RETHROW_TRANSACTION_EXCEPTION);
            futures.clear();
            putTransactionInfoRemote();
            state = PREPARED;
        } catch (Throwable e) {
            throw ExceptionUtil.rethrow(e, TransactionException.class);
        }
    }

    private void putTransactionInfoRemote() throws ExecutionException, InterruptedException {
        PutRemoteTransactionOperation operation = new PutRemoteTransactionOperation(
                transactionLog.getRecords(), txnId, xid, txOwnerUuid, timeoutMillis, startTime);
        OperationService operationService = nodeEngine.getOperationService();
        IPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionId = partitionService.getPartitionId(xid);
        InternalCompletableFuture<Object> future = operationService.invokeOnPartition(SERVICE_NAME, operation, partitionId);
        future.get();
    }

    @Override
    public void commit() throws TransactionException, IllegalStateException {
        if (state != PREPARED) {
            throw new IllegalStateException("Transaction is not prepared");
        }
        checkTimeout();
        try {
            state = COMMITTING;
            List<Future> futures = transactionLog.commit(nodeEngine);

            // We should rethrow exception if transaction is not TWO_PHASE

            waitWithDeadline(futures, COMMIT_TIMEOUT_MINUTES, MINUTES, commitExceptionHandler);

            state = COMMITTED;
            transactionLog.onCommitSuccess();
        } catch (Throwable e) {
            state = COMMIT_FAILED;
            transactionLog.onCommitFailure();
            throw ExceptionUtil.rethrow(e, TransactionException.class);
        }
    }

    public void commitAsync(BiConsumer callback) {
        if (state != PREPARED) {
            throw new IllegalStateException("Transaction is not prepared");
        }
        checkTimeout();
        state = COMMITTING;

        BiConsumer wrappedCallback = (input, throwable) -> {
            try {
                callback.accept(input, throwable);
            } finally {
                if (throwable != null) {
                    transactionLog.onCommitFailure();
                } else {
                    transactionLog.onCommitSuccess();
                }
            }
        };

        transactionLog.commitAsync(nodeEngine, wrappedCallback);
        // We should rethrow exception if transaction is not TWO_PHASE

        state = COMMITTED;
    }

    @Override
    public void rollback() throws IllegalStateException {
        if (state == NO_TXN || state == ROLLED_BACK) {
            throw new IllegalStateException("Transaction is not active");
        }
        state = ROLLING_BACK;
        try {
            List<Future> futures = transactionLog.rollback(nodeEngine);
            waitWithDeadline(futures, ROLLBACK_TIMEOUT_MINUTES, MINUTES, rollbackExceptionHandler);
        } catch (Throwable e) {
            throw ExceptionUtil.rethrow(e);
        } finally {
            state = ROLLED_BACK;
        }
    }

    public void rollbackAsync(BiConsumer callback) {
        if (state == NO_TXN || state == ROLLED_BACK) {
            throw new IllegalStateException("Transaction is not active");
        }
        state = ROLLING_BACK;
        transactionLog.rollbackAsync(nodeEngine, callback);
        //todo: I doubt this is correct; rollbackAsync is an async operation so has potentially not yet completed.
        state = ROLLED_BACK;
    }

    @Override
    public UUID getTxnId() {
        return txnId;
    }

    public long getStartTime() {
        return startTime;
    }

    public Collection<TransactionLogRecord> getTransactionRecords() {
        return transactionLog.getRecords();
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public TransactionType getTransactionType() {
        return TransactionType.TWO_PHASE;
    }

    @Override
    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    @Override
    public void add(TransactionLogRecord record) {
        if (state != Transaction.State.ACTIVE) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }

        transactionLog.add(record);
    }

    @Override
    public void remove(Object key) {
        transactionLog.remove(key);
    }

    @Override
    public TransactionLogRecord get(Object key) {
        return transactionLog.get(key);
    }

    @Override
    public UUID getOwnerUuid() {
        return txOwnerUuid;
    }

    @Override
    public boolean isOriginatedFromClient() {
        return originatedFromClient;
    }

    public SerializableXID getXid() {
        return xid;
    }

    private void checkTimeout() {
        if (startTime + timeoutMillis < Clock.currentTimeMillis()) {
            ExceptionUtil.sneakyThrow(new XAException(XAException.XA_RBTIMEOUT));
        }
    }
}
