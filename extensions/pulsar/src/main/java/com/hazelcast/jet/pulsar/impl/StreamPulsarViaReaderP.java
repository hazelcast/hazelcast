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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.common.naming.TopicName;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.impl.util.JetExceptionUtil.rethrow;
import static com.hazelcast.jet.pulsar.impl.Utils.wrapFailure;
import static org.apache.pulsar.common.naming.TopicDomain.persistent;

/**
 * Reads each physical topic through a single {@link Reader}.
 * Before the reading it queries partition and subscribes only to a subset
 * of them.
 * <p>The subscription backing the reader must be dedicated to this source and must not be deleted,
 * expired or reset externally as long as the job is running. Topic archival policies must be configured to provide sufficient
 * delay to allow message re-read after job restart. Please see
 * <a href="https://pulsar.apache.org/docs/5.0.x/concepts-clients/#reader">Official Pulsar Documentation</a> for more details.
 *
 * <p>New topic partitions are discovered as soon as they are created. Partition removal is not supported.
 *
 * @param <M> type of message we receive from Pulsar
 * @param <T> type of message we send to later stages of the pipeline
 */
public class StreamPulsarViaReaderP<M, T> extends AbstractProcessor {
    private static final long PARTITION_GET_TIMEOUT = 120;
    private static final int SCAN_DELAY_SECONDS = 5;

    private final FunctionEx<Message<M>, T> projectionFn;
    private final PulsarSourceConfig<M, T> pulsarConfig;
    private final EventTimeMapper<? super T> eventTimeMapper;
    private final Map<String, Integer> assignedPartitionIndexesForWatermarks = new HashMap<>();
    private final Map<String, MessageId> offsets = new HashMap<>();

    private ILogger logger;
    private final PulsarClient client;
    private Reader<M> reader;
    private Traverser<Object> traverser = Traversers.empty();
    private Traverser<Map.Entry<BroadcastKey<String>, SnapshotData>> snapshotTraverser;
    private final AtomicBoolean partitionNumberChanged = new AtomicBoolean(false);
    private final List<String> assignedPartitions = new ArrayList<>();
    private Context context;
    private Future<Message<M>> nextMessageFuture;
    private final AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
    private PartitionAssigner partitionAssigner;

    public StreamPulsarViaReaderP(@Nonnull PulsarSourceConfig<M, T> config,
                                  @Nonnull EventTimePolicy<? super T> eventTimePolicy,
                                  @NonNull PulsarClient pulsarClient) {
        this.pulsarConfig = config;
        this.projectionFn = config.getProjectionFn();
        this.eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
        this.client = pulsarClient;
    }

    public void notifyPartitionChanged() {
        partitionNumberChanged.set(true);
    }

    @Override
    protected void init(@Nonnull Context context) {
        logger = context.logger();
        this.context = context;
        partitionAssigner = new PartitionAssigner(pulsarConfig.getTopics(), context);
        assignPartitions();
    }

    private void assignPartitions() {
        int oldPartitionCount = assignedPartitions.size();
        boolean snapshottingEnabled = context.snapshottingEnabled();
        try {
            for (String topic : pulsarConfig.getTopics()) {
                List<String> partitions = client.getPartitionsForTopic(topic, true)
                                                .get(PARTITION_GET_TIMEOUT, TimeUnit.SECONDS);
                for (String discoveredPartition : partitions) {
                    TopicName topicName = TopicName.get(discoveredPartition);
                    String fullTopicName = topicName.toString();
                    if (snapshottingEnabled && topicName.getDomain() != persistent) {
                        throw new JetException("Snapshotting requires persistent Pulsar topics: " + fullTopicName);
                    }
                    // Use the same canonical name for assignment, snapshot keys and received messages.
                    // This also assigns a non-partitioned topic to exactly one processor.
                    if (partitionAssigner.belongsToThisProcessor(topicName)
                            && assignedPartitionIndexesForWatermarks
                                    .putIfAbsent(fullTopicName, assignedPartitions.size()) == null) {
                        assignedPartitions.add(fullTopicName);
                    }
                }
            }
            int count = eventTimeMapper.partitionCount();
            if (assignedPartitions.size() > count) {
                eventTimeMapper.addPartitions(assignedPartitions.size() - count);
            }

            assert eventTimeMapper.partitionCount() == assignedPartitions.size();

            // force recreation of reader
            if (reader != null && oldPartitionCount != assignedPartitions.size()) {
                reader.close();
                reader = null;
            }
            logger.info("Assigned partitions: " + assignedPartitions);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw wrapFailure(e.getMessage(), e.getCause());
        } catch (ExecutionException | TimeoutException | IOException e) {
            throw wrapFailure("Unable to subscribe to Pulsar partitions", e);
        }
    }

    // tbd to parameterize
    @SuppressWarnings("checkstyle:MagicNumber")
    private void createReaderAndStartReading() {
        if (!assignedPartitions.isEmpty()) {
            try {
                ReaderBuilder<M> builder =
                    client.newReader(pulsarConfig.getSchemaSupplier().get());
                pulsarConfig.getReaderCustomizer().accept(builder);
                reader = builder.autoUpdatePartitions(false)
                                .topics(List.copyOf(assignedPartitions))
                                .poolMessages(false)
                                .startMessageId(offsets.isEmpty() ? MessageId.earliest : MessageId.latest)
                                .create();
                reader.seek(topic -> offsets.getOrDefault(topic, MessageId.earliest));
                nextMessageFuture = reader.readNextAsync();
            } catch (IOException e) {
                throw wrapFailure(e.getMessage(), e.getCause());
            }
        }
    }

    @Override
    public boolean complete() {
        if (traverser != null && !emitFromTraverser(traverser)) {
            return false;
        }
        Throwable exception = exceptionRef.get();
        if (exception != null) {
            exceptionRef.set(null);
            throw rethrow(exception);
        }
        if (partitionNumberChanged.compareAndSet(true, false)) {
            assignPartitions();
        }
        if (assignedPartitions.isEmpty()) {
            // no-op consumer
            emitFromTraverser(eventTimeMapper.flatMapIdle());
            return false;
        }
        if (reader == null) {
            createReaderAndStartReading();
        }

        if (!nextMessageFuture.isDone()) {
            emitFromTraverser(eventTimeMapper.flatMapIdle());
            return false;
        }
        try {
            List<Message<M>> messages = new ArrayList<>();

            while (nextMessageFuture.isDone()) {
                messages.add(nextMessageFuture.get());
                nextMessageFuture = reader.readNextAsync();
            }
            traverser = traverseIterable(messages).flatMap(this::processMessage)
                                                  .onFirstNull(() -> traverser = null);
            emitFromTraverser(traverser);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw wrapFailure(e.getMessage(), e.getCause());
        } catch (ExecutionException e) {
            throw wrapFailure(e.getMessage(), e.getCause());
        }
        return false;
    }

    private Traverser<Object> processMessage(Message<M> message) {
        String topicName = message.getTopicName();
        Integer partitionIndex = assignedPartitionIndexesForWatermarks.get(topicName);
        if (partitionIndex == null) {
            throw new JetException("Received a message from a removed Pulsar partition and"
                                       + " connector does not support removal of partitions");
        }
        MessageId messageId = message.getMessageId();
        T projectedRecord = projectionFn.apply(message);
        offsets.put(topicName, messageId);
        long timestamp = message.getEventTime() != 0 ? message.getEventTime() : message.getPublishTime();
        return projectedRecord == null
               ? eventTimeMapper.flatMapIdle()
               : eventTimeMapper.flatMapEvent(projectedRecord, partitionIndex, timestamp);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    record SnapshotData (byte[] offset, long watermark) implements Serializable {
    }

    @Override
    public boolean saveToSnapshot() {
        if (traverser != null && !emitFromTraverser(traverser)) {
            return false;
        }
        if (snapshotTraverser == null) {
            snapshotTraverser = traverseIterable(assignedPartitionIndexesForWatermarks.entrySet())
                    .map(e -> {
                        MessageId offset = offsets.get(e.getKey());
                        return Map.entry(BroadcastKey.broadcastKey(e.getKey()),
                                         new SnapshotData(
                                             offset == null ? null : offset.toByteArray(),
                                             eventTimeMapper.getWatermark(e.getValue())
                                         ));
                    })
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        Object partition = ((BroadcastKey<?>) key).key();
        if (!(partition instanceof String partitionString)) {
            throw new JetException("Incompatible Pulsar source snapshot: expected a canonical topic name");
        }
        TopicName topicName = TopicName.get(partitionString);
        if (!partitionAssigner.belongsToThisProcessor(topicName)) {
            return;
        }
        Integer partitionIndex = assignedPartitionIndexesForWatermarks.get(partition);
        SnapshotData state = (SnapshotData) value;
        if (state.offset() != null) {
            try {
                MessageId messageId = MessageId.fromByteArray(state.offset());
                offsets.put(partitionString, TopicMessageId.create(partitionString, messageId));

            } catch (IOException e) {
                throw wrapFailure("Unable to restore Pulsar message ID for " + partition, e);
            }
        }
        eventTimeMapper.restoreWatermark(partitionIndex, state.watermark());
    }

    @Override
    public void close() {
        if (reader != null) {
            try {
                reader.close();
            } catch (Exception e) {
                throw new JetException("Error while closing the Pulsar Reader processor.", e);
            }
        }
    }

    private void notifyAbout(Throwable exception) {
        exceptionRef.set(exception);
    }

    @Nonnull
    public static <M, T> ProcessorSupplier processorSupplier(@Nonnull PulsarSourceConfig<M, T> config,
                                                             @Nonnull EventTimePolicy<? super T> eventTimePolicy) {
        return new PulsarProcessorSupplier<>(config, eventTimePolicy);
    }

    @Nonnull
    public static <M, T> ProcessorMetaSupplier processorMetaSupplier(@Nonnull PulsarSourceConfig<M, T> config,
                                                                     @Nonnull EventTimePolicy<? super T> eventTimePolicy) {
        int preferredParallelism = config.getLocalParallelism();
        return ProcessorMetaSupplier.of(
            preferredParallelism,
            processorSupplier(config, eventTimePolicy)
        );
    }

    public static class PulsarProcessorSupplier<M, T> implements ProcessorSupplier, DataSerializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private PulsarSourceConfig<M, T> config;
        private EventTimePolicy<? super T> eventTimePolicy;
        private transient List<StreamPulsarViaReaderP<M, T>> processors = Collections.emptyList();
        private final transient ScheduledExecutorService partitionWatcherExecutor = Executors.newSingleThreadScheduledExecutor();
        private transient PulsarClient pulsarClient;

        // needed for serialization
        @SuppressWarnings("unused")
        public PulsarProcessorSupplier() {
        }

        PulsarProcessorSupplier(PulsarSourceConfig<M, T> config, EventTimePolicy<? super T> eventTimePolicy) {
            this.config = config;
            this.eventTimePolicy = eventTimePolicy;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(config);
            out.writeObject(eventTimePolicy);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            config = in.readObject();
            eventTimePolicy = in.readObject();
        }

        @Override
        public void init(@Nonnull ProcessorSupplier.Context context) {
            pulsarClient = config.getTargetConnectionSupplier(context).get();
            Map<String, Integer> partitionNumberPerTopic = new HashMap<>();
            partitionWatcherExecutor.scheduleWithFixedDelay(() -> {
                for (String topic : config.getTopics()) {
                    pulsarClient.getPartitionsForTopic(topic, true)
                                .thenAccept(partitions -> {
                                    int newSize = partitions.size();
                                    Integer old = partitionNumberPerTopic.put(topic, newSize);
                                    if (old != null && newSize != old) {
                                        processors.forEach(StreamPulsarViaReaderP::notifyPartitionChanged);
                                    }
                                }).whenComplete((ignored, exception) -> {
                                    if (exception != null) {
                                        processors.forEach(p -> p.notifyAbout(exception));
                                    }
                                });
                }
            }, SCAN_DELAY_SECONDS, SCAN_DELAY_SECONDS, TimeUnit.SECONDS);
        }

        @Override
        public void close(@Nullable Throwable error) throws Exception {
            partitionWatcherExecutor.shutdown();
            if (pulsarClient != null) {
                pulsarClient.shutdown();
            }
        }

        @Override
        public @NonNull Collection<? extends Processor> get(int count) {
            processors = IntStream.range(0, count)
                                  .mapToObj(i -> new StreamPulsarViaReaderP<>(config, eventTimePolicy, pulsarClient))
                                  .toList();
            return processors;
        }
    }
}
