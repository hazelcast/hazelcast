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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.ConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionId;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionalResource;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.spi.exception.RetryableHazelcastException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.Collections.singletonList;

/**
 * A utility to handle transactions where each local processor uses a pool of
 * transactional resources alternately. This is needed if we can't reliably
 * list transactions that were created by our processor and solves it by having
 * a deterministic transaction IDs.
 */
public class TransactionPoolSnapshotUtility<TXN_ID extends TransactionId, RES extends TransactionalResource<TXN_ID>>
        extends TwoPhaseSnapshotCommitUtility<TXN_ID, RES> {

    private static final int TXN_PROBING_FACTOR = 5;

    private final int poolSize;
    private final List<TXN_ID> transactionIds;
    private List<LoggingNonThrowingResource<TXN_ID, RES>> transactions;
    private int activeTxnIndex;
    private boolean activeTransactionUsed;
    private TXN_ID preparedTxnId;
    private LoggingNonThrowingResource<TXN_ID, RES> transactionToCommit;
    private boolean flushed;
    private boolean processorCompleted;
    private boolean transactionsReleased;

    /**
     * @param createTxnIdFn input is {processorIndex, transactionIndex}
     */
    public TransactionPoolSnapshotUtility(
            @Nonnull Outbox outbox,
            @Nonnull Context procContext,
            boolean isSource,
            @Nonnull ProcessingGuarantee externalGuarantee,
            int poolSize,
            @Nonnull BiFunctionEx<Integer, Integer, TXN_ID> createTxnIdFn,
            @Nonnull FunctionEx<TXN_ID, RES> createTxnFn,
            @Nonnull ConsumerEx<TXN_ID> recoverAndCommitFn,
            @Nonnull ConsumerEx<TXN_ID> recoverAndAbortFn
    ) {
        super(outbox, procContext, isSource, externalGuarantee, createTxnFn, recoverAndCommitFn,
                processorIndex -> {
                    for (int i = 0; i < adjustPoolSize(externalGuarantee, isSource, poolSize); i++) {
                        TXN_ID txnId = createTxnIdFn.apply(processorIndex, i);
                        LoggingUtil.logFine(procContext.logger(), "recover and abort %s", txnId);
                        recoverAndAbortFn.accept(txnId);
                    }
                });

        this.poolSize = adjustPoolSize(externalGuarantee, isSource, poolSize);
        LoggingUtil.logFine(procContext.logger(), "Actual pool size used: %d", this.poolSize);
        if (this.poolSize > 1) {
            transactionIds = new ArrayList<>(this.poolSize);
            for (int i = 0; i < this.poolSize; i++) {
                transactionIds.add(createTxnIdFn.apply(procContext().globalProcessorIndex(), i));
                assert i == 0 || !transactionIds.get(i).equals(transactionIds.get(i - 1)) : "two equal IDs generated";
            }
        } else {
            transactionIds = singletonList(null);
        }
    }

    private static int adjustPoolSize(@Nonnull ProcessingGuarantee externalGuarantee, boolean isSource, int poolSize) {
        // we need at least 1 transaction or 2 for ex-once. More than 3 is never needed.
        if (externalGuarantee == EXACTLY_ONCE && poolSize < 2 || poolSize < 1 || poolSize > 3) {
            throw new IllegalArgumentException("poolSize=" + poolSize);
        }
        // for at-least-once source we don't need more than two transactions
        if (externalGuarantee == AT_LEAST_ONCE && isSource) {
            poolSize = Math.min(2, poolSize);
        }
        // for no guarantee and for an at-least-once sink we need just 1 txn
        if (externalGuarantee == NONE || externalGuarantee == AT_LEAST_ONCE && !isSource) {
            poolSize = 1;
        }
        return poolSize;
    }

    @Override
    public boolean tryProcess() {
        ensureTransactions();
        return true;
    }

    @Nullable @Override
    public RES activeTransaction() {
        ensureTransactions();
        if (usesTransactionLifecycle() && poolSize < (preparedTxnId != null ? 3 : 2)) {
            return null;
        }
        LoggingNonThrowingResource<TXN_ID, RES> activeTransaction = transactions.get(activeTxnIndex);
        if (!activeTransactionUsed && usesTransactionLifecycle()) {
            activeTransaction.begin();
        }
        activeTransactionUsed = true;
        return activeTransaction.wrapped();
    }

    private void rollbackOtherTransactions() {
        if (!usesTransactionLifecycle()) {
            return;
        }
        // If a member is removed or the local parallelism is reduced, the
        // transactions with higher processor index won't be used. We need to
        // roll these back too. We probe transaction IDs with processorIndex
        // beyond those of the current execution, up to 5x (the
        // TXN_PROBING_FACTOR) of the current total parallelism. We only roll
        // back "our" transactions, that is those where index%parallelism =
        // ourIndex
        for (
                int index = procContext().globalProcessorIndex();
                index < procContext().totalParallelism() * TXN_PROBING_FACTOR;
                index += procContext().totalParallelism()
        ) {
            recoverAndAbortFn().accept(index);
        }
    }

    @Override
    public boolean snapshotCommitPrepare() {
        if (externalGuarantee() == NONE) {
            return true;
        }
        ensureTransactions();
        assert preparedTxnId == null : "preparedTxnId != null";
        transactionToCommit = transactions.get(activeTxnIndex);
        incrementActiveTxnIndex();
        if (!activeTransactionUsed) {
            LoggingUtil.logFine(procContext().logger(),
                    "transaction not used, ignoring snapshot, txnId=%s", transactionToCommit.id());
            return true;
        }
        activeTransactionUsed = false;
        // `flushed` is used to avoid double flushing when outbox.offerToSnapshot() fails
        if (!flushed && !(flushed = transactionToCommit.flush())) {
            procContext().logger().fine("flush returned false");
            return false;
        }
        if (usesTransactionLifecycle()) {
            preparedTxnId = transactionToCommit.id();
            if (!getOutbox().offerToSnapshot(broadcastKey(preparedTxnId), false)) {
                return false;
            }
            transactionToCommit.endAndPrepare();
        }
        flushed = false;
        return true;
    }

    @Override
    public boolean snapshotCommitFinish(boolean success) {
        if (!usesTransactionLifecycle() || preparedTxnId == null) {
            return true;
        }
        preparedTxnId = null;
        if (!success) {
            // we can't ignore the snapshot failure
            throw new RetryableHazelcastException("the snapshot failed");
        }
        transactionToCommit.commit();
        if (processorCompleted) {
            doRelease();
        }
        return true;
    }

    @Override
    public void afterCompleted() {
        // if the processor completes and a snapshot is in progress, the onSnapshotComplete
        // will be called anyway - we'll not release in that case
        processorCompleted = true;
        if (preparedTxnId == null) {
            doRelease();
        }
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        assert !activeTransactionUsed : "transaction already begun";
        @SuppressWarnings("unchecked")
        TXN_ID txnId = ((BroadcastKey<TXN_ID>) key).key();
        if (usesTransactionLifecycle()
                && txnId.index() % procContext().totalParallelism() == procContext().globalProcessorIndex()) {
            for (int i = 0; i < poolSize; i++) {
                if (txnId.equals(transactionIds.get(i))) {
                    // If we restored a txnId of one of our transactions, make the next transaction
                    // active. We must avoid using the same transaction that we committed in the
                    // snapshot we're restoring from, because if the job fails without creating a
                    // snapshot, we would commit the transaction that should be rolled back. If
                    // poolSize=3, we also can't use the txn ID before the one we just restored
                    // because if the job failed after preparing the next txn, but before the
                    // snapshot was successful, it could write more items using the restored txn
                    // which we would also incorrectly commit after the restart.
                    //
                    // Note that we can restore a TxnId that is neither of our current IDs in case
                    // the job is upgraded and has a new jobId.
                    activeTxnIndex = i;
                    incrementActiveTxnIndex();
                    break;
                }
            }
            LoggingUtil.logFine(procContext().logger(), "recover and commit %s", txnId);
            recoverAndCommitFn().accept(txnId);
        }
    }

    @Override
    public void close() {
        try {
            doRelease();
        } catch (Exception e) {
            procContext().logger().warning("Exception when releasing, ignoring it: " + e, e);
        }
    }

    private void ensureTransactions() {
        if (transactions == null) {
            if (transactionsReleased) {
                throw new IllegalStateException("transactions already released");
            }
            rollbackOtherTransactions();
            transactions = toList(transactionIds, createTxnFn());
        }
    }

    private void incrementActiveTxnIndex() {
        activeTxnIndex++;
        if (activeTxnIndex == poolSize) {
            activeTxnIndex = 0;
        }
    }

    private void doRelease() {
        if (transactionsReleased) {
            return;
        }
        transactionsReleased = true;
        if (transactions != null) {
            if (usesTransactionLifecycle() && activeTransactionUsed) {
                transactions.get(activeTxnIndex).rollback();
            }
            for (LoggingNonThrowingResource<TXN_ID, RES> txn : transactions) {
                txn.release();
            }
        }
    }
}
