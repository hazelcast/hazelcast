/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kafka.impl;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.processor.TransactionPoolSnapshotUtility;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility.TransactionalResource;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.jet.kafka.KafkaDataConnection;
import com.hazelcast.jet.kafka.KafkaProcessors;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.logging.ILogger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;

/**
 * See {@link KafkaProcessors#writeKafkaP}.
 */
public final class WriteKafkaP<T, K, V> implements Processor {

    public static final int TXN_POOL_SIZE = 2;
    private final FunctionEx<String, KafkaProducer<K, V>> getProducerFn;
    private final Function<? super T, ? extends ProducerRecord<K, V>> toRecordFn;
    private final boolean exactlyOnce;

    private Context context;
    private TransactionPoolSnapshotUtility<KafkaTransactionId, KafkaTransaction<K, V>> snapshotUtility;
    private final AtomicReference<Throwable> lastError = new AtomicReference<>();

    private final Callback callback = (metadata, exception) -> {
        // Note: this method may be called on different thread.
        if (exception != null) {
            lastError.compareAndSet(null, exception);
        }
    };

    private WriteKafkaP(
            @Nonnull FunctionEx<String, KafkaProducer<K, V>> getProducerFn,
            @Nonnull Function<? super T, ? extends ProducerRecord<K, V>> toRecordFn,
            boolean exactlyOnce
    ) {
        this.getProducerFn = getProducerFn;
        this.toRecordFn = toRecordFn;
        this.exactlyOnce = exactlyOnce;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        this.context = context;
        ProcessingGuarantee guarantee = context.processingGuarantee() == EXACTLY_ONCE && !exactlyOnce
                ? AT_LEAST_ONCE
                : context.processingGuarantee();

        snapshotUtility = new TransactionPoolSnapshotUtility<>(outbox, context, false, guarantee, TXN_POOL_SIZE,
                (processorIndex, txnIndex) -> new KafkaTransactionId(
                        context.jobId(), context.jobConfig().getName(), context.vertexName(), processorIndex, txnIndex),
                txnId -> {
                    KafkaProducer<K, V> producer = getProducerFn.apply(txnId == null ? null : txnId.getKafkaId());
                    return new KafkaTransaction<>(txnId, producer, context.logger());
                },
                txnId -> {
                    try {
                        recoverTransaction(txnId, true);
                    } catch (ProducerFencedException e) {
                        context.logger().warning("Failed to finish the commit of a transaction ID saved in the " +
                                "snapshot, data loss can occur. Transaction id: " + txnId.getKafkaId(), e);
                    }
                },
                txnId -> recoverTransaction(txnId, false)
        );
    }

    @Override
    public boolean tryProcess() {
        checkError();
        return snapshotUtility.tryProcess();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        KafkaTransaction<K, V> txn = snapshotUtility.activeTransaction();
        if (txn == null) {
            return;
        }
        checkError();
        for (Object item; (item = inbox.peek()) != null; ) {
            try {
                txn.producer.send(toRecordFn.apply((T) item), callback);
            } catch (TimeoutException ignored) {
                // apply backpressure, the item will be retried
                return;
            }
            inbox.remove();
        }
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    @Override
    public boolean complete() {
        KafkaTransaction<K, V> transaction = snapshotUtility.activeTransaction();
        if (transaction == null) {
            return false;
        }
        transaction.flush();
        LoggingUtil.logFinest(context.logger(), "flush in complete() done, %s", transaction.transactionId);
        checkError();
        snapshotUtility.afterCompleted();
        return true;
    }

    @Override
    public boolean snapshotCommitPrepare() {
        if (!snapshotUtility.snapshotCommitPrepare()) {
            return false;
        }
        checkError();
        return true;
    }

    @Override
    public boolean snapshotCommitFinish(boolean commitTransactions) {
        return snapshotUtility.snapshotCommitFinish(commitTransactions);
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Inbox inbox) {
        snapshotUtility.restoreFromSnapshot(inbox);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    private void recoverTransaction(KafkaTransactionId txnId, boolean commit) {
        try (KafkaProducer<?, ?> p = getProducerFn.apply(txnId.getKafkaId())) {
            if (commit) {
                ResumeTransactionUtil.resumeTransaction(p, txnId.producerId(), txnId.epoch());
                try {
                    p.commitTransaction();
                } catch (InvalidTxnStateException e) {
                    context.logger().fine("Failed to commit transaction with ID restored from the snapshot. This " +
                            "happens normally when the transaction was committed in phase 2 of the snapshot and can " +
                            "be ignored, but can happen also if the transaction wasn't committed in phase 2 and the " +
                            "broker lost it (in this case data written in it is lost). Transaction ID: " + txnId, e);
                }
            } else {
                p.initTransactions();
            }
        }
    }

    @Override
    public void close() {
        if (snapshotUtility != null) {
            snapshotUtility.close();
        }
    }

    private void checkError() {
        Throwable t = lastError.get();
        if (t != null) {
            throw sneakyThrow(t);
        }
    }

    /**
     * Use {@link KafkaProcessors#writeKafkaP(Properties, FunctionEx, boolean)}
     */
    public static <T, K, V> SupplierEx<Processor> supplier(
            @Nonnull Properties properties,
            @Nonnull Function<? super T, ? extends ProducerRecord<K, V>> toRecordFn,
            boolean exactlyOnce
    ) {
        if (properties.containsKey("transactional.id")) {
            throw new IllegalArgumentException("Property `transactional.id` must not be set, Jet sets it as needed");
        }
        return () -> new WriteKafkaP<>(s -> {
            @SuppressWarnings({"rawtypes", "unchecked"})
            Map<String, Object> castProperties = (Map) properties;
            Map<String, Object> copy = new HashMap<>(castProperties);
            if (s != null) {
                copy.put("transactional.id", s);
            }
            return new KafkaProducer<>(copy);
        }, toRecordFn, exactlyOnce);
    }

    /**
     * Use {@link KafkaProcessors#writeKafkaP(DataConnectionRef, FunctionEx, boolean)}
     */
    public static <T, K, V> ProcessorSupplier supplier(
            @Nonnull DataConnectionRef dataConnectionRef,
            @Nonnull Function<? super T, ? extends ProducerRecord<K, V>> toRecordFn,
            boolean exactlyOnce
    ) {
        return new ProcessorSupplier() {

            private transient KafkaDataConnection kafkaDataConnection;

            @Override
            public void init(@Nonnull Context context) {
                kafkaDataConnection = context
                        .dataConnectionService()
                        .getAndRetainDataConnection(dataConnectionRef.getName(), KafkaDataConnection.class);
            }

            @Nonnull
            @Override
            public Collection<? extends Processor> get(int count) {
                return IntStream.range(0, count)
                                .mapToObj(i -> new WriteKafkaP<T, K, V>(
                                        (txnId) -> kafkaDataConnection.getProducer(txnId),
                                        toRecordFn,
                                        exactlyOnce
                                ))
                                .collect(Collectors.toList());
            }

            @Override
            public void close(@Nullable Throwable error) {
                if (kafkaDataConnection != null) {
                    kafkaDataConnection.release();
                }
            }
        };
    }

    /**
     * Use {@link KafkaProcessors#writeKafkaP(DataConnectionRef, Properties, FunctionEx, boolean)}
     */
    public static <T, K, V> ProcessorSupplier supplier(
            @Nonnull DataConnectionRef dataConnectionRef,
            @Nonnull Properties properties,
            @Nonnull Function<? super T, ? extends ProducerRecord<K, V>> toRecordFn,
            boolean exactlyOnce
    ) {
        return new ProcessorSupplier() {

            private transient KafkaDataConnection kafkaDataConnection;

            @Override
            public void init(@Nonnull Context context) {
                kafkaDataConnection = context
                        .dataConnectionService()
                        .getAndRetainDataConnection(dataConnectionRef.getName(), KafkaDataConnection.class);
            }

            @Nonnull
            @Override
            public Collection<? extends Processor> get(int count) {
                return IntStream.range(0, count)
                        .mapToObj(i -> new WriteKafkaP<T, K, V>(
                                (txnId) -> kafkaDataConnection.getProducer(txnId, properties),
                                toRecordFn,
                                exactlyOnce
                        ))
                        .collect(Collectors.toList());
            }

            @Override
            public void close(@Nullable Throwable error) {
                if (kafkaDataConnection != null) {
                    kafkaDataConnection.release();
                }
            }
        };
    }

    /**
     * A simple class wrapping a KafkaProducer that ensures that
     * `producer.initTransactions` is called exactly once before first
     * `beginTransaction` call.
     */
    private static final class KafkaTransaction<K, V> implements TransactionalResource<KafkaTransactionId> {
        private final KafkaProducer<K, V> producer;
        private final ILogger logger;
        private final KafkaTransactionId transactionId;
        private boolean txnInitialized;

        private KafkaTransaction(KafkaTransactionId transactionId, KafkaProducer<K, V> producer, ILogger logger) {
            this.transactionId = transactionId;
            this.producer = producer;
            this.logger = logger;
        }

        @Override
        public KafkaTransactionId id() {
            return transactionId;
        }

        @Override
        public void begin() {
            if (!txnInitialized) {
                LoggingUtil.logFine(logger, "initTransactions in begin %s", transactionId);
                txnInitialized = true;
                producer.initTransactions();
                transactionId.updateProducerAndEpoch(producer);
            }
            producer.beginTransaction();
        }

        @Override
        public boolean flush() {
            producer.flush();
            return true;
        }

        @Override
        public void commit() {
            if (transactionId != null) {
                producer.commitTransaction();
            }
        }

        @Override
        public void rollback() {
            if (transactionId != null) {
                producer.abortTransaction();
            }
        }

        @Override
        public void release() {
            producer.close(Duration.ZERO);
        }
    }

    public static class KafkaTransactionId implements TwoPhaseSnapshotCommitUtility.TransactionId, Serializable {

        private static final long serialVersionUID = 1L;

        private final int processorIndex;
        private long producerId = -1;
        private short epoch = -1;
        private final String kafkaId;
        private final int hashCode;

        KafkaTransactionId(long jobId, String jobName, @Nonnull String vertexId, int processorIndex,
                           int transactionIndex) {
            this.processorIndex = processorIndex;

            kafkaId = "jet.job-" + idToString(jobId) + '.' + sanitize(jobName) + '.' + sanitize(vertexId) + '.'
                    + processorIndex + "-" + transactionIndex;
            hashCode = Objects.hash(jobId, vertexId, processorIndex);
        }

        @Override
        public int index() {
            return processorIndex;
        }

        long producerId() {
            return producerId;
        }

        short epoch() {
            return epoch;
        }

        void updateProducerAndEpoch(KafkaProducer<?, ?> producer) {
            producerId = ResumeTransactionUtil.getProducerId(producer);
            epoch = ResumeTransactionUtil.getEpoch(producer);
        }

        /**
         * Get a String representation of this ID.
         */
        @Override
        public String toString() {
            return getKafkaId() + ",producerId=" + producerId + ",epoch=" + epoch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KafkaTransactionId that = (KafkaTransactionId) o;
            return this.getKafkaId().equals(that.getKafkaId());
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Nonnull
        String getKafkaId() {
            return kafkaId;
        }

        private static String sanitize(String s) {
            if (s == null) {
                return "";
            }
            return s.replaceAll("[^\\p{Alnum}.\\-_$#/{}\\[\\]]", "_");
        }
    }
}
