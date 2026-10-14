/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.pulsar.impl;

import com.hazelcast.dataconnection.DataConnectionService;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ThrowingSupplier;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.RestartableException;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.processor.TwoPhaseSnapshotCommitUtility;
import com.hazelcast.jet.impl.processor.UnboundedTransactionsProcessorUtility;
import com.hazelcast.jet.pulsar.CloseablePulsarClient;
import com.hazelcast.jet.pulsar.PulsarDataConnection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.utils.RetryUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.PulsarClientImpl;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.ExceptionUtil.safeClose;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.pulsar.impl.Utils.wrapFailure;
import static java.lang.System.lineSeparator;
import static java.util.stream.Collectors.joining;

/**
 * Processing Guarantees:
 * Processing Guarantee is calculated for each processor and can be different to this set by the job.
 * In particular, {@link ProcessingGuarantee#EXACTLY_ONCE} may get lowered to {@link ProcessingGuarantee#AT_LEAST_ONCE},
 * because user sets {@link PulsarSinkConfig#isTransactional()} to false. In such cases the processor will not use transactions,
 * so real guarantee will be {@link ProcessingGuarantee#AT_LEAST_ONCE}.
 *
 * @param <E> the type of object produced by the connector ({@code M} before applying projectionFn).
 * @param <M> the type of messages send to Pulsar.
 */
public class WritePulsarP<E, M> extends AbstractProcessor {
    private static final int NUM_DEDUPLICATION_CHECK = 5;
    private ILogger logger;
    private PulsarClient client;
    private Producer<M> producer;

    private final FunctionEx<? super E, M> extractValueFn;
    private final FunctionEx<? super E, String> extractKeyFn;
    private final FunctionEx<? super E, Map<String, String>> extractPropertiesFn;
    private final FunctionEx<? super E, Long> extractTimestampFn;

    private final PulsarSinkConfig<E, M> pulsarConfig;
    private UnboundedTransactionsProcessorUtility<PulsarTransactionId, PulsarTransaction> transactionUtility;
    private volatile Throwable error;

    public WritePulsarP(PulsarSinkConfig<E, M> pulsarConfig) {
        this.pulsarConfig = pulsarConfig;
        this.extractValueFn = pulsarConfig.getExtractValueFn();
        this.extractKeyFn = pulsarConfig.getExtractKeyFn();
        this.extractPropertiesFn = pulsarConfig.getExtractPropertiesFn();
        this.extractTimestampFn = pulsarConfig.getExtractTimestampFn();
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        var processorIndex = context.globalProcessorIndex();
        logger = context.logger();
        client = pulsarConfig.getTargetConnectionSupplier(context).getEx();

        ProcessingGuarantee guarantee = context.processingGuarantee() == EXACTLY_ONCE && !pulsarConfig.isTransactional()
                                        ? AT_LEAST_ONCE
                                        : context.processingGuarantee();

        checkDeduplicationStatus(context, guarantee);

        ProducerBuilder<M> builder =
            client.newProducer(pulsarConfig.getSchemaSupplier().get())
                  .topic(pulsarConfig.getTopic())
                  .loadConf(pulsarConfig.getOptions());
        pulsarConfig.getProducerCustomizer().accept(builder);
        producer = builder.autoUpdatePartitions(true)
                          .blockIfQueueFull(true)
                          .create();

        transactionUtility = new UnboundedTransactionsProcessorUtility<>(
            getOutbox(),
            context,
            guarantee,
            () -> new PulsarTransactionId(context, processorIndex),
            txId -> new PulsarTransaction(client, txId, logger),
            this::recoverTransaction,
            () -> {
                // we let the transactions expire
                // to change timeout, please set PulsarSinkBuilder#transactionTimeout
                // Set the value high enough, so that job suspension/restyart and recovery can happen before transaction
                // expires
            }
        );
    }

    /**
     * Deduplication is needed for true exactly-once semantics. If message is delivered, but client does not get ACK in time,
     * it will retry adding the entry to a transaction. Without deduplication, the messages in transaction log will duplicate
     * and we will have duplicates after commit.
     */
    private void checkDeduplicationStatus(@Nonnull Context context, ProcessingGuarantee guarantee) {
        if (guarantee != EXACTLY_ONCE) {
            return;
        }
        if (pulsarConfig.getDataConnectionRef() == null && pulsarConfig.getHttpServiceUrl() == null) {
            logger.severe("Processing Guarantee is EXACTLY_ONCE, "
                              + "but Sink has no DataConnection nor httpServiceUrl configured. "
                              + "We are not able to check if deduplication is enabled! "
                              + "Please set Data Connection Ref or set httpServiceUrl builder parameter");
            return;
        }
        Boolean status = null;
        try {
            if (pulsarConfig.getDataConnectionRef() != null) {
                DataConnectionService dataConnectionService = context.dataConnectionService();
                var dataConnection =
                    dataConnectionService.getAndRetainDataConnection(pulsarConfig.getDataConnectionRef().getName(),
                                                                     PulsarDataConnection.class);
                try {
                    status = readDeduplicationStatus(dataConnection::getAdmin);
                } catch (Exception e) {
                    status = false;
                } finally {
                    dataConnection.release();
                }
            } else {
                try {
                    status = readDeduplicationStatus(() -> PulsarAdmin.builder()
                                                                      .serviceHttpUrl(pulsarConfig.getHttpServiceUrl())
                                                                      .build());
                } catch (Exception e) {
                    status = false;
                }
            }

        } catch (Exception e) {
            logger.severe("Cannot check duplication status due to: " + e.getMessage(), e);
        }
        if (!Boolean.TRUE.equals(status)) {
            throw new JetException("Deduplication is not enabled on the broker or we are unable to check it's status. "
                                       + "If deduplication is not enabled, then true Exactly One semantics are not possible. "
                                       + "Please enable deduplication for " + pulsarConfig.getTopic());
        }
    }

    private Boolean readDeduplicationStatus(ThrowingSupplier<PulsarAdmin> adminSupplier) {
        return RetryUtils.retry(() -> {
            try (var admin  = adminSupplier.get()) {
                return admin.topicPolicies().getDeduplicationStatus(pulsarConfig.getTopic(), true);
            }
        }, NUM_DEDUPLICATION_CHECK);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(int ordinal, @Nonnull Inbox inbox) {
        try {
            E element;
            List<CompletableFuture<MessageId>> futures = new ArrayList<>();
            while ((element = (E) inbox.peek()) != null) {
                M value = extractValueFn.apply(element);
                String key = extractKeyFn.apply(element);
                Map<String, String> properties = extractPropertiesFn.apply(element);
                Long timestamp = extractTimestampFn.apply(element);

                if (transactionUtility.usesTransactionLifecycle()) {
                    PulsarTransaction pulsarTransaction = transactionUtility.activeTransaction();
                    checkNotNull(pulsarTransaction, "there is no active transaction");
                    futures.add(pulsarTransaction.add(key, value, timestamp, properties));
                } else {
                    var sendFuture = sendMessage(key, value, timestamp, properties, null);
                    futures.add(sendFuture);
                }
                inbox.remove();
            }
            if (!futures.isEmpty()) {
                waitForSending(futures);
            }
            Throwable throwable = error;
            if (throwable != null) {
                error = null;
                throw new JetException(throwable);
            }
        } catch (Exception e) {
            throw wrapFailure(e.getMessage(), e);
        }
    }

    private CompletableFuture<MessageId> sendMessage(String key, M value, Long timestamp, Map<String, String> properties,
                                                     Transaction transaction) {
        TypedMessageBuilder<M> builder =
            transactionUtility.usesTransactionLifecycle()
            ? producer.newMessage(transaction)
            : producer.newMessage();
        builder
            .value(value)
            .properties(properties);
        if (timestamp != null) {
            builder = builder.eventTime(timestamp);
        }
        if (key != null) {
            builder = builder.key(key);
        }

        return builder.sendAsync().whenComplete((result, e) -> {
            if (e != null) {
                error = e;
            }
        });
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean snapshotCommitPrepare() {
        return transactionUtility.snapshotCommitPrepare();
    }

    @Override
    public boolean snapshotCommitFinish(boolean success) {
        try {
            return transactionUtility.snapshotCommitFinish(success);
        } catch (Exception e) {
            if (PulsarClientException.isRetriableError(e)) {
                return false;
            }
            throw new RestartableException(e.getMessage(), e);
        }
    }

    @Override
    public boolean complete() {
        if (transactionUtility.usesTransactionLifecycle()) {
            transactionUtility.afterCompleted();
        }
        return true;
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        transactionUtility.restoreFromSnapshot(key, value);
    }

    @Override
    public void close() {
        List<Exception> errors = new ArrayList<>();
        safeClose(transactionUtility, errors);
        safeClose(producer, errors);
        safeClose(client, errors);
        if (!errors.isEmpty()) {
            throw new JetException(errors.stream().map(Throwable::getMessage).collect(joining(lineSeparator())));
        }
    }

    private static class PulsarTransactionId implements TwoPhaseSnapshotCommitUtility.TransactionId, Serializable {

        @Serial
        private static final long serialVersionUID = 1L;

        private final int processorIndex;
        private final String pulsarId;
        private final int hashCode;
        private TxnID txId;

        PulsarTransactionId(Context context, int processorIndex) {
            this.processorIndex = processorIndex;
            long jobId = context.jobId();
            String jobName = context.jobConfig().getName();

            this.pulsarId = "jet.job-" + idToString(jobId) + '.' + sanitize(jobName) + '.' + sanitize(context.vertexName()) + '.'
                                + processorIndex;
            hashCode = Objects.hash(jobName, pulsarId, processorIndex);
        }

        @Override
        public int index() {
            return processorIndex;
        }

        @Override
        public String toString() {
            return "PulsarTransactionId{" +
                       "processorIndex=" + processorIndex +
                       ", txId=" + txId +
                       ", pulsarId='" + pulsarId + '\'' +
                       ", hashCode=" + hashCode +
                       '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PulsarTransactionId that = (PulsarTransactionId) o;
            return processorIndex == that.processorIndex
                       && hashCode == that.hashCode
                       && Objects.equals(pulsarId, that.pulsarId)
                       && Objects.equals(txId, that.txId);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        private static String sanitize(String s) {
            if (s == null) {
                return "";
            }
            return s.replaceAll("[^\\p{Alnum}.\\-_$#/{}\\[\\]]", "_");
        }
    }

    private void recoverTransaction(PulsarTransactionId transactionId) {
        if (!transactionUtility.usesTransactionLifecycle()) {
            return;
        }
        if (transactionId == null || transactionId.txId == null) {
            return;
        }
        PulsarClientImpl impl = (PulsarClientImpl) CloseablePulsarClient.unwrap(client);
        TransactionCoordinatorClient tcClient = impl.getTcClient();
        try {
            tcClient.commit(transactionId.txId);
        } catch (TransactionCoordinatorClientException e) {
            if (e instanceof TransactionCoordinatorClientException.TransactionNotFoundException) {
                logger.warning(("transaction %s is no longer reachable to perform commit, most likely a timeout was hit "
                                    + "or transaction was committed earlier."
                                    + "Check how long the transaction lasted; in case of long duration the timeout may have been "
                                    + "hit and and data may be lost.")
                                   .formatted(transactionId));
            } else {
                throw wrapFailure(e.getMessage(), e);
            }
        }
    }

    private void waitForSending(List<CompletableFuture<MessageId>> futures) {
        if (!futures.isEmpty()) {
            try {
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                                 .get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw wrapFailure(e.getMessage(), e);
            } catch (ExecutionException e) {
                throw wrapFailure(e.getMessage(), e);
            }
        }
    }

    private final class PulsarTransaction implements TwoPhaseSnapshotCommitUtility.TransactionalResource<PulsarTransactionId> {
        private final PulsarClient client;
        private final ILogger logger;
        private final PulsarTransactionId transactionId;
        private boolean txnInitialized;
        private Transaction transaction;

        private PulsarTransaction(@Nonnull PulsarClient client,
                                  @Nonnull PulsarTransactionId transactionId,
                                  @Nonnull ILogger logger) {
            this.client = client;
            this.transactionId = transactionId;
            this.logger = logger;
        }

        @Override
        public PulsarTransactionId id() {
            return transactionId;
        }

        @Override
        public void begin() {
            if (!txnInitialized) {
                logger.fine("beginning transaction %s", transactionId);
                startTransactionQuietly();
                txnInitialized = true;
            }
        }

        @Override
        public void commit() {
            try {
                transaction.commit().get();
            } catch (InterruptedException | ExecutionException e) {
                if (e instanceof  InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                var peeled = ExceptionUtil.peel(e, TransactionCoordinatorClientException.TransactionNotFoundException.class,
                                                null);
                if (peeled instanceof TransactionCoordinatorClientException.TransactionNotFoundException) {
                    throw wrapFailure("Error while committing the transaction, transaction does not exist. "
                                           + " Check how long the transaction lasted; in case of long duration"
                                          + " the timeout may have been hit and and data is most likely lost.", peeled);
                }
                throw wrapFailure("Error while committing the transaction", e);
            }
        }

        private void startTransactionQuietly() {
            try {
                Duration transactionTimeout = pulsarConfig.getTransactionTimeout();
                transaction = client.newTransaction()
                                    .withTransactionTimeout(transactionTimeout.toMillis(), TimeUnit.MILLISECONDS)
                                    .build().get();
                transactionId.txId = transaction.getTxnID();
            } catch (Exception e) {
                throw wrapFailure(e.getMessage(), e);
            }
        }

        @Override
        public void rollback() {
            tryExternalRollback();
        }

        private void tryExternalRollback() {
            try {
                transaction.abort().get();
            } catch (IllegalStateException | InterruptedException | ExecutionException e) {
                throw wrapFailure(e.getMessage(), e);
            }
        }

        @Override
        public void release() {
        }

        CompletableFuture<MessageId> add(String key, M value, Long timestamp, Map<String, String> properties) {
            return sendMessage(key, value, timestamp, properties, transaction);
        }
    }

    @Nonnull
    public static <E, M> SupplierEx<Processor> processorSupplier(@Nonnull PulsarSinkConfig<M, E> config) {
        return new WritePulsarP.ProcessorSupplierEx<>(config);
    }

    @Nonnull
    public static <E, M> ProcessorMetaSupplier processorMetaSupplier(@Nonnull PulsarSinkConfig<M, E> config) {
        int preferredParallelism = config.getLocalParallelism();
        return ProcessorMetaSupplier.of(
            preferredParallelism,
            processorSupplier(config)
        );
    }

    public static class ProcessorSupplierEx<E, M> implements SupplierEx<Processor>, DataSerializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private PulsarSinkConfig<E, M> config;

        // needed for serialization
        @SuppressWarnings("unused")
        public ProcessorSupplierEx() {
        }

        ProcessorSupplierEx(PulsarSinkConfig<E, M> config) {
            this.config = config;
        }

        @Override
        public Processor getEx() {
            return new WritePulsarP<>(config);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(config);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            config = in.readObject();
        }
    }
}
