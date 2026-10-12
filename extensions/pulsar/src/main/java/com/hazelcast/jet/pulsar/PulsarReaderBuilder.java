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

package com.hazelcast.jet.pulsar;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ThrowingSupplier;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.logging.ILogger;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.impl.util.Util.checkNonNullAndSerializable;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Validation.validate;
import static java.util.Objects.requireNonNull;

/**
 * See {@link PulsarSources#pulsarReaderBuilder(String, SupplierEx, SupplierEx, FunctionEx)}
 *
 * @param <M> the type of the message read by {@code pulsarReader}
 * @param <T> the type of data emitted from {@code StreamSource}
 */
public final class PulsarReaderBuilder<M, T> implements Serializable {

    private List<String> topics;
    private SupplierEx<PulsarClient> connectionSupplier;
    private DataConnectionRef dataConnectionRef;
    private Map<String, Object> readerConfig;
    private final SupplierEx<Schema<M>> schemaSupplier;
    private FunctionEx<Message<M>, T> projectionFn;

    public PulsarReaderBuilder(SupplierEx<Schema<M>> schemaSupplier) {
        checkNonNullAndSerializable(schemaSupplier, "schemaSupplier");
        this.schemaSupplier = requireNonNull(schemaSupplier);
        this.readerConfig = getDefaultReaderConfig();
    }

    /**
     * Required fields of Pulsar reader
     *
     * @param topic              Pulsar topic name to consume from
     * @param connectionSupplier Pulsar client supplier
     * @param schemaSupplier     Pulsar messaging schema supplier.
     * @param projectionFn       converts a Pulsar message to an emitted item.
     */
    public PulsarReaderBuilder(
            @Nonnull String topic,
            @Nonnull SupplierEx<PulsarClient> connectionSupplier,
            @Nonnull SupplierEx<Schema<M>> schemaSupplier,
            @Nonnull FunctionEx<Message<M>, T> projectionFn) {
        checkSerializable(connectionSupplier, "connectionSupplier");
        checkSerializable(schemaSupplier, "schemaSupplier");
        checkSerializable(projectionFn, "projectionFn");
        this.topics = List.of(topic);
        this.connectionSupplier = connectionSupplier;
        this.readerConfig = getDefaultReaderConfig();
        this.schemaSupplier = schemaSupplier;
        this.projectionFn = projectionFn;
    }

    @Nonnull
    public PulsarReaderBuilder<M, T> topic(@Nonnull String... topics) {
        checkNotNull(topics, "topics");
        this.topics = List.of(topics);
        return this;
    }

    @Nonnull
    public PulsarReaderBuilder<M, T> connectionSupplier(@Nonnull SupplierEx<PulsarClient> connectionSupplier) {
        checkSerializable(connectionSupplier, "connectionSupplier");
        this.connectionSupplier = connectionSupplier;
        return this;
    }

    @Nonnull
    public PulsarReaderBuilder<M, T> dataConnectionRef(@Nonnull DataConnectionRef dataConnectionRef) {
        requireNonNull(dataConnectionRef, "dataConnectionRef");
        this.dataConnectionRef = dataConnectionRef;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public <NEW_T> PulsarReaderBuilder<M, NEW_T> projectionFn(@Nonnull FunctionEx<Message<M>, NEW_T> projectionFn) {
        checkNonNullAndSerializable(projectionFn, "projectionFn");
        this.projectionFn = (FunctionEx<Message<M>, T>) projectionFn;
        return (PulsarReaderBuilder<M, NEW_T>) this;
    }


    private static Map<String, Object> getDefaultReaderConfig() {
        Map<String, Object> defaultReaderConfig = new HashMap<>();
        defaultReaderConfig.put("readerName", "hazelcast-jet-reader");
        return defaultReaderConfig;
    }

    /**
     * @param readerConfig Pulsar reader configurations that must contain reader name
     */
    @Nonnull
    public PulsarReaderBuilder<M, T> readerConfig(@Nonnull Map<String, Object> readerConfig) {
        checkSerializable(readerConfig, "readerConfig");
        this.readerConfig = readerConfig;
        return this;
    }

    /**
     * Creates and returns the Pulsar Reader {@link StreamSource} with using builder configurations set before.
     */
    @Nonnull
    public StreamSource<T> build() {
        validate()
            .checkNotNull(topics, "topics")
            .checkNotNull(readerConfig, "readerConfig")
            .checkNotNullAndSerializable(schemaSupplier, "schemaSupplier")
            .checkNotNullAndSerializable(projectionFn, "projectionFn")
            .checkSerializableIfNotNull(connectionSupplier, "connectionSupplier")
            .check(Utils.exactlyOnlyOneIsNotNull(connectionSupplier, dataConnectionRef),
                   "Either connectionSupplier or dataConnectionRef must be provided")
            .throwIfErrors();
        return SourceBuilder.timestampedStream("pulsar-reader-source",
                                               ctx -> new ReaderContext<>(
                                                   ctx, connectionSupplier, dataConnectionRef,
                                                   topics, readerConfig,
                                                   schemaSupplier, projectionFn))
                .<T>fillBufferFn(ReaderContext::fillBuffer)
                            .initFn(ctx -> requireNonNull(ctx).init())
                .createSnapshotFn(ReaderContext::createSnapshot)
                .restoreSnapshotFn(ReaderContext::restoreSnapshot)
                .destroyFn(ctx -> requireNonNull(ctx).destroy())
                .build();
    }

    /**
     * A context for the Pulsar Reader, the fault-tolerant stream source of Apache Pulsar
     *
     * @param <M> the type of the value of message read by {@code PulsarConsumer}
     * @param <T> the type of the emitted item after projection.
     */
    private static final class ReaderContext<M, T> {
        private static final int QUEUE_CAP = 1024;
        private static final int MAX_FILL_MESSAGES = 128;

        private final ILogger logger;
        private PulsarClient client;
        private final BlockingQueue<Message<M>> queue = new ArrayBlockingQueue<>(QUEUE_CAP);

        private final FunctionEx<Message<M>, T> projectionFn;
        private final Map<String, Object> readerConfig;
        private final Schema<M> schema;
        private final List<String> topics;
        private Reader<M> reader;
        private MessageId offset = MessageId.earliest;

        private ThrowingSupplier<PulsarClient> clientSupplier;

        private ReaderContext(
                @Nonnull Processor.Context ctx,
                @Nullable SupplierEx<PulsarClient> clientSupplier,
                @Nullable DataConnectionRef dataConnectionRef,
                @Nonnull List<String> topics,
                @Nonnull Map<String, Object> readerConfig,
                @Nonnull SupplierEx<Schema<M>> schemaSupplier,
                @Nonnull FunctionEx<Message<M>, T> projectionFn
        ) {
            this.logger = ctx.logger();
            this.clientSupplier = () -> Utils.getClient(ctx, clientSupplier, dataConnectionRef);
            this.topics = topics;
            this.readerConfig = readerConfig;
            this.schema = schemaSupplier.get();
            this.projectionFn = projectionFn;
        }

        public void init() {
            try {
                client = clientSupplier.get();
                clientSupplier = null;
            } catch (Exception e) {
                throw new JetException("Unable to initialize context for Pulsar Reader", e);
            }
        }

        /**
         * Receive the messages as a batch.
         * In this method, emitted items are created by applying the projection function
         * to the messages received from Pulsar client. If there is an event time
         * associated with the message, it sets the event time as the timestamp of the
         * emitted item. Otherwise, it sets the publishing time(which always exists)
         * of the message as the timestamp.
         */
        private void fillBuffer(SourceBuilder.TimestampedSourceBuffer<T> sourceBuffer) throws PulsarClientException {
            if (reader == null) {
                createReader();
            }
            int count = 0;
            while (!queue.isEmpty() && count++ < MAX_FILL_MESSAGES) {
                Message<M> message = queue.poll();
                long timestamp;
                if (message.getEventTime() != 0) {
                    timestamp = message.getEventTime();
                } else {
                    timestamp = message.getPublishTime();
                }
                T item = projectionFn.apply(message);
                offset = message.getMessageId();
                if (item != null) {
                    sourceBuffer.add(item, timestamp);
                }
            }
        }

        private void createReader() throws PulsarClientException {
            reader = client.newReader(schema)
                           .topics(topics)
                           .startMessageId(offset)
                           .loadConf(readerConfig)
                           .readerListener((r, msg) -> {
                               try {
                                   queue.put(msg);
                               } catch (InterruptedException e) {
                                   Thread.currentThread().interrupt();
                               }
                           })
                           .receiverQueueSize(QUEUE_CAP)
                           .create();
        }

        byte[] createSnapshot() {
            return offset.toByteArray();
        }

        void restoreSnapshot(List<byte[]> snapshots) throws IOException {
            offset = MessageId.fromByteArray(snapshots.get(0));
        }

        private void destroy() {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                logger.warning("Error while closing the 'Pulsar Reader'.", e);
            }
            try {
                client.shutdown();
            } catch (PulsarClientException e) {
                logger.warning("Error while shutting down the 'PulsarClient'.", e);
            }
        }
    }
}
