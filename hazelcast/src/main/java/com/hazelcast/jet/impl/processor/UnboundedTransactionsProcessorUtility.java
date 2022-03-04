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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.function.RunnableEx;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionId;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionalResource;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;

import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * Utility to handle transactions in a processor that is able to have unbounded
 * number of open transactions and is able to enumerate those pertaining to a
 * job.
 *
 * @param <RES> the transaction type
 */
public class UnboundedTransactionsProcessorUtility<TXN_ID extends TransactionId, RES extends TransactionalResource<TXN_ID>>
        extends TwoPhaseSnapshotCommitUtility<TXN_ID, RES> {

    private final Supplier<TXN_ID> createTxnIdFn;
    private final RunnableEx abortUnfinishedTransactionsAction;

    private LoggingNonThrowingResource<TXN_ID, RES> activeTransaction;
    private final List<LoggingNonThrowingResource<TXN_ID, RES>> pendingTransactions;
    private final Queue<TXN_ID> snapshotQueue = new ArrayDeque<>();
    private boolean initialized;
    private boolean snapshotInProgress;
    private boolean unfinishedTransactionsAborted;

    /**
     * @param abortUnfinishedTransactionsAction when called, it should abort
     *      all unfinished transactions found in the external system that
     *      pertain to the processor
     */
    public UnboundedTransactionsProcessorUtility(
            @Nonnull Outbox outbox,
            @Nonnull Context procContext,
            @Nonnull ProcessingGuarantee externalGuarantee,
            @Nonnull Supplier<TXN_ID> createTxnIdFn,
            @Nonnull FunctionEx<TXN_ID, RES> createTxnFn,
            @Nonnull ConsumerEx<TXN_ID> recoverAndCommitFn,
            @Nonnull RunnableEx abortUnfinishedTransactionsAction
    ) {
        super(outbox, procContext, false, externalGuarantee, createTxnFn, recoverAndCommitFn,
                txnId -> {
                    throw new UnsupportedOperationException();
                });
        this.createTxnIdFn = createTxnIdFn;
        this.abortUnfinishedTransactionsAction = abortUnfinishedTransactionsAction;
        pendingTransactions = usesTransactionLifecycle() ? new ArrayList<>() : null;
    }

    @Nonnull @Override
    public RES activeTransaction() {
        if (activeTransaction == null) {
            if (!initialized) {
                if (usesTransactionLifecycle()) {
                    try {
                        procContext().logger().fine("aborting unfinished transactions");
                        abortUnfinishedTransactions();
                    } catch (Exception e) {
                        throw sneakyThrow(e);
                    }
                }
                initialized = true;
            }
            activeTransaction = createTxnFn().apply(createTxnIdFn.get());
            if (usesTransactionLifecycle()) {
                activeTransaction.begin();
            }
        }
        return activeTransaction.wrapped();
    }

    /**
     * Force a new transaction outside of the snapshot cycle. The next call to
     * {@link #activeTransaction()} will return a new transaction.
     */
    public void finishActiveTransaction() {
        if (activeTransaction == null) {
            return;
        }
        if (usesTransactionLifecycle()) {
            pendingTransactions.add(activeTransaction);
            activeTransaction.endAndPrepare();
        } else {
            activeTransaction.release();
        }
        activeTransaction = null;
    }

    @Override
    public void afterCompleted() {
        if (activeTransaction == null) {
            return;
        }
        if (usesTransactionLifecycle()) {
            pendingTransactions.add(activeTransaction);
            if (!snapshotInProgress) {
                commitPendingTransactions();
            }
        } else {
            activeTransaction.release();
        }
        activeTransaction = null;
    }

    @Override
    public boolean snapshotCommitPrepare() {
        if (usesTransactionLifecycle()) {
            if (snapshotQueue.isEmpty()) {
                finishActiveTransaction();
                for (LoggingNonThrowingResource<TXN_ID, RES> txn : pendingTransactions) {
                    snapshotQueue.add(txn.id());
                }
            }
        } else {
            if (activeTransaction != null) {
                activeTransaction.flush();
            }
        }
        for (TXN_ID txnId; (txnId = snapshotQueue.peek()) != null; ) {
            if (!getOutbox().offerToSnapshot(broadcastKey(txnId), false)) {
                return false;
            }
            snapshotQueue.remove();
        }

        snapshotInProgress = true;
        return true;
    }

    @Override
    public boolean snapshotCommitFinish(boolean success) {
        assert snapshotInProgress : "no snapshot in progress";
        snapshotInProgress = false;
        if (usesTransactionLifecycle() && success) {
            commitPendingTransactions();
        }
        return true;
    }

    private void commitPendingTransactions() {
        for (LoggingNonThrowingResource<TXN_ID, RES> txn : pendingTransactions) {
            txn.commit();
            txn.release();
        }
        pendingTransactions.clear();
    }

    private void abortUnfinishedTransactions() {
        abortUnfinishedTransactionsAction.run();
        unfinishedTransactionsAborted = true;
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        @SuppressWarnings("unchecked")
        TXN_ID txnId = ((BroadcastKey<TXN_ID>) key).key();
        if (txnId.index() % procContext().totalParallelism() == procContext().globalProcessorIndex()) {
            recoverAndCommitFn().accept(txnId);
        }
    }

    @Override
    public void close() {
        if (!unfinishedTransactionsAborted) {
            abortUnfinishedTransactions();
        }
        if (activeTransaction != null) {
            activeTransaction.rollback();
            activeTransaction.release();
            activeTransaction = null;
        }
        if (pendingTransactions != null) {
            for (LoggingNonThrowingResource<TXN_ID, RES> txn : pendingTransactions) {
                txn.release();
            }
            pendingTransactions.clear();
        }
    }
}
