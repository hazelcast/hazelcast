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
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionId;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionalResource;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map.Entry;
import java.util.function.Consumer;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * A base class for transaction utilities implementing different transaction
 * strategies.
 * <p>
 * The protected methods are intended for utility implementations. The
 * public methods are intended for utility users, the processors.
 *
 * @param <TXN_ID> type fo the transaction ID
 * @param <RES> type of the transactional resource
 */
public abstract class TwoPhaseSnapshotCommitUtility<TXN_ID extends TransactionId,
        RES extends TransactionalResource<TXN_ID>> {

    private final boolean isSource;
    private final Outbox outbox;
    private final Context procContext;
    private final ProcessingGuarantee externalGuarantee;
    private final FunctionEx<TXN_ID, LoggingNonThrowingResource<TXN_ID, RES>> createTxnFn;
    private final Consumer<TXN_ID> recoverAndCommitFn;
    private final ConsumerEx<Integer> recoverAndAbortFn;

    /**
     * @param outbox the outbox passed to the {@link Processor#init} method
     * @param procContext the context passed to the {@link Processor#init} method
     * @param isSource true, if the processor is a source (a reader), false for
     *      a sink (a writer)
     * @param externalGuarantee guarantee required for the source/sink. Must
     *      not be higher than the job's guarantee
     * @param createTxnFn creates a {@link TransactionalResource} based on a
     *      transaction id. The implementation needs to ensure that in case
     *      when the transaction ID was used before, the work from the previous
     *      use is rolled back
     * @param recoverAndCommitFn a function to finish the commit of transaction
     *      identified by the given ID
     * @param recoverAndAbortFn a function to rollback the work of the all the
     *      transactions that were created by the given processor index
     */
    protected TwoPhaseSnapshotCommitUtility(
            @Nonnull Outbox outbox,
            @Nonnull Context procContext,
            boolean isSource,
            @Nonnull ProcessingGuarantee externalGuarantee,
            @Nonnull FunctionEx<TXN_ID, RES> createTxnFn,
            @Nonnull ConsumerEx<TXN_ID> recoverAndCommitFn,
            @Nonnull ConsumerEx<Integer> recoverAndAbortFn
    ) {
        if (externalGuarantee.ordinal() > procContext.processingGuarantee().ordinal()) {
            throw new IllegalArgumentException("unsupported combination, job guarantee cannot by lower than external "
                    + "guarantee. Job guarantee: " + procContext.processingGuarantee() + ", external guarantee: "
                    + externalGuarantee);
        }
        this.isSource = isSource;
        this.outbox = outbox;
        this.procContext = procContext;
        this.externalGuarantee = externalGuarantee;
        this.createTxnFn = txnId -> new LoggingNonThrowingResource<>(procContext.logger(), createTxnFn.apply(txnId));
        this.recoverAndCommitFn = recoverAndCommitFn;
        this.recoverAndAbortFn = recoverAndAbortFn;
    }

    public ProcessingGuarantee externalGuarantee() {
        return externalGuarantee;
    }

    protected Outbox getOutbox() {
        return outbox;
    }

    protected Context procContext() {
        return procContext;
    }

    protected FunctionEx<TXN_ID, LoggingNonThrowingResource<TXN_ID, RES>> createTxnFn() {
        return createTxnFn;
    }

    protected Consumer<TXN_ID> recoverAndCommitFn() {
        return recoverAndCommitFn;
    }

    protected ConsumerEx<Integer> recoverAndAbortFn() {
        return recoverAndAbortFn;
    }

    /**
     * Delegate handling of {@link Processor#tryProcess()} to this method.
     */
    public boolean tryProcess() {
        return true;
    }

    /**
     * Returns the active transaction that can be used to store an item or
     * query the source. It's null in case when a transaction is not available
     * now. In that case the processor should back off and retry later.
     */
    @Nullable
    public abstract RES activeTransaction();

    /**
     * For sinks and inner vertices, call from {@link Processor#complete()}.
     * For batch sources call after the source emitted everything. Never call
     * it for streaming sources.
     */
    public abstract void afterCompleted();

    /**
     * Delegate handling of {@link Processor#snapshotCommitPrepare()} to this
     * method.
     *
     * @return a value to return from {@code snapshotCommitPrepare()}
     */
    public abstract boolean snapshotCommitPrepare();

    /**
     * Delegate handling of {@link Processor#snapshotCommitFinish(boolean)} to
     * this method.
     *
     * @param success value passed to {@code snapshotCommitFinish}
     * @return value to return from {@code snapshotCommitFinish}
     */
    public abstract boolean snapshotCommitFinish(boolean success);

    /**
     * Delegate handling of {@link Processor#restoreFromSnapshot(Inbox)} to
     * this method. If you save custom items to snapshot besides those saved by
     * {@link #snapshotCommitPrepare()} of this utility, use {@link
     * #restoreFromSnapshot(Object, Object)} to pass only entries not handled
     * by your processor.
     *
     * @param inbox the inbox passed to {@code Processor.restoreFromSnapshot()}
     */
    @SuppressWarnings("unchecked")
    public void restoreFromSnapshot(@Nonnull Inbox inbox) {
        for (Object item; (item = inbox.poll()) != null; ) {
            Entry<BroadcastKey<TXN_ID>, Boolean> castedItem = (Entry<BroadcastKey<TXN_ID>, Boolean>) item;
            restoreFromSnapshot(castedItem.getKey(), castedItem.getValue());
        }
    }

    /**
     * Delegate handling of {@link
     * AbstractProcessor#restoreFromSnapshot(Object, Object)} to this method.
     * <p>
     * See also {@link #restoreFromSnapshot(Inbox)}.
     *
     * @param key a key from the snapshot
     * @param value a value from the snapshot
     */
    public abstract void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value);

    /**
     * Call from {@link Processor#close()}.
     * <p>
     * The implementation must not commit or rollback any pending transactions
     * - the job might have failed between after snapshot phase 1 and 2. The
     * pending transactions might be recovered after the restart.
     */
    public abstract void close() throws Exception;

    public boolean usesTransactionLifecycle() {
        return externalGuarantee == EXACTLY_ONCE ||
                externalGuarantee == AT_LEAST_ONCE && isSource;
    }

    /**
     * A handle for a transactional resource.
     * <p>
     * The methods are called depending on the external guarantee:<ul>
     *
     * <li>EXACTLY_ONCE source & sink, AT_LEAST_ONCE source
     *
     * <ol>
     *     <li>{@link #begin()} - called before the transaction is first
     *         returned from {@link #activeTransaction()}
     *     <li>{@link #flush()}
     *     <li>{@link #endAndPrepare()} - after this the transaction will no
     *         longer be returned from {@link #activeTransaction()}, i.e. no
     *         more data will be written to it. Called in the 1st snapshot
     *         phase. For AT_LEAST_ONCE source this call can be ignored, it's
     *         not required to be able to finish the commit after restart
     *     <li>{@link #commit()} - called in the 2nd snapshot phase
     *     <li>if the utility recycles transactions, the process can go to (1)
     *     <li>{@link #release()}
     * </ol>
     *
     * <li>AT_LEAST_ONCE sink
     *
     * The transaction should be in auto-commit mode, {@link #commit()} and
     * other transaction methods won't be called.
     *
     * <ol>
     *     <li>{@link #flush()} - ensure all writes are stored in the external
     *         system. Called in the 1st snapshot phase
     *     <li>if the utility recycles transaction, the process can go to (1)
     *     <li>{@link #release()}
     * </ol>
     *
     * <li>NONE
     *
     * <ol>
     *     <li>{@link #release()}
     * </ol>
     *
     * </ul>
     *
     * @param <TXN_ID> type of transaction identifier. Must be serializable, will
     *                be saved to state snapshot
     */
    public interface TransactionalResource<TXN_ID> {

        /**
         * Returns the ID of this transaction, it should be the ID passed to
         * the {@link #createTxnFn()}.
         */
        TXN_ID id();

        /**
         * Begins the transaction. The method will be called before the
         * transaction is returned from {@link #activeTransaction()} for the
         * first time after creation or after {@link #commit()}.
         * <p>
         * This method is called in exactly-once mode; in at-least-once mode
         * it's called only if the processor is a source. It's never called if
         * there's no processing guarantee.
         * <p>
         * See also the {@linkplain TransactionalResource class javadoc}.
         *
         * @throws UnsupportedOperationException if the transaction was created
         * with {@code null} id passed to the {@link #createTxnFn()}
         */
        default void begin() throws Exception {
            throw new UnsupportedOperationException("Resource without transaction support");
        }

        /**
         * Flushes all previous writes to the external system and ensures all
         * pending items are emitted to the downstream.
         * <p>
         * See also the {@linkplain TransactionalResource class javadoc}.
         *
         * @return if all was flushed and emitted. If the method returns false,
         *      it will be called again before any other method is called.
         */
        default boolean flush() throws Exception {
            return true;
        }

        /**
         * Prepares for a commit. To achieve correctness, the transaction must
         * be able to eventually commit after this call, writes must be durably
         * stored in the external system.
         * <p>
         * After this call, the transaction will never again be returned
         * from {@link #activeTransaction()} until it's committed.
         * <p>
         * This method is called in exactly-once mode; in at-least-once mode
         * it's called only if the processor is a source. It's never called if
         * there's no processing guarantee.
         * <p>
         * See also the {@linkplain TransactionalResource class javadoc}.
         */
        default void endAndPrepare() throws Exception {
        }

        /**
         * Makes the changes visible to others and acknowledges consumed items.
         * <p>
         * This method is called in exactly-once mode; in at-least-once mode
         * it's called only if the processor is a source. It's never called if
         * there's no processing guarantee.
         * <p>
         * See also the {@linkplain TransactionalResource class javadoc}.
         */
        default void commit() throws Exception {
            throw new UnsupportedOperationException();
        }

        /**
         * Roll back the transaction. Only called for non-prepared transactions
         * when the job execution ends.
         * <p>
         * Will only be called for a transaction that was {@linkplain
         * #begin() begun}.
         */
        default void rollback() throws Exception {
        }

        /**
         * Finish the pending operations and release the associated resources.
         * If a transaction was begun, must not commit or roll it back, the
         * transaction can be later recovered from the durable storage and
         * continued.
         * <p>
         * See also the {@linkplain TransactionalResource class javadoc}.
         */
        default void release() throws Exception {
        }
    }

    public interface TransactionId {

        /**
         * Returns the index of the processor that will handle this transaction
         * ID. Used when restoring transaction IDs to determine which processor
         * owns which transactions.
         * <p>
         * After restoring the ID from the snapshot the index might be out
         * of range (greater or equal to the current total parallelism).
         */
        int index();
    }

    /**
     * A wrapper for {@link TransactionalResource} adding logging and not
     * throwing checked exceptions. Aimed to simplify subclass implementation.
     */
    protected static final class LoggingNonThrowingResource<TXN_ID, RES extends TransactionalResource<TXN_ID>>
            implements TransactionalResource<TXN_ID> {

        private final ILogger logger;
        private final RES wrapped;

        private LoggingNonThrowingResource(ILogger logger, RES wrapped) {
            this.logger = logger;
            this.wrapped = wrapped;
        }

        public RES wrapped() {
            return wrapped;
        }

        @Override
        public TXN_ID id() {
            return wrapped.id();
        }

        @Override
        public void begin() {
            LoggingUtil.logFine(logger, "begin %s", id());
            try {
                wrapped.begin();
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        }

        @Override
        public boolean flush() {
            LoggingUtil.logFine(logger, "flush %s", id());
            try {
                return wrapped.flush();
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        }

        @Override
        public void endAndPrepare() {
            LoggingUtil.logFine(logger, "endAndPrepare %s", id());
            try {
                wrapped.endAndPrepare();
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        }

        @Override
        public void commit() {
            LoggingUtil.logFine(logger, "commit %s", id());
            try {
                wrapped.commit();
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        }

        @Override
        public void rollback() {
            LoggingUtil.logFine(logger, "rollback %s", id());
            try {
                wrapped.rollback();
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        }

        @Override
        public void release() {
            LoggingUtil.logFine(logger, "release %s", id());
            try {
                wrapped.release();
            } catch (Exception e) {
                throw sneakyThrow(e);
            }
        }
    }
}
