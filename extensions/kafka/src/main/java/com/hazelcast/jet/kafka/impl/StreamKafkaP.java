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
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.EventTimeMapper;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.kafka.KafkaDataConnection;
import com.hazelcast.jet.kafka.KafkaProcessors;
import com.hazelcast.jet.kafka.TopicsConfig;
import com.hazelcast.jet.pipeline.DataConnectionRef;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;

/**
 * See {@link KafkaProcessors#streamKafkaP}.
 */
public final class StreamKafkaP<K, V, T> extends AbstractProcessor {

    public static final int PREFERRED_LOCAL_PARALLELISM = 4;
    private static final long METADATA_CHECK_INTERVAL_NANOS = SECONDS.toNanos(5);
    private static final String PARTITION_COUNTS_SNAPSHOT_KEY = "partitionCounts";

    Map<TopicPartition, Integer> currentAssignment = new HashMap<>();

    private final FunctionEx<Context, Consumer<K, V>> kafkaConsumerFn;

    private final FunctionEx<? super ConsumerRecord<K, V>, ? extends T> projectionFn;
    private final EventTimeMapper<? super T> eventTimeMapper;
    private final TopicsConfig topicsConfig;
    private List<String> topics;
    private int totalParallelism;
    private ProcessingGuarantee processingGuarantee;

    private Consumer<K, V> consumer;
    private long nextMetadataCheck = Long.MIN_VALUE;

    /**
     * Key: topicName<br>
     * Value: partition offsets, at index I is offset for partition I.<br>
     * Offsets are -1 initially and remain -1 for partitions not assigned to this processor.
     */
    private final Map<String, long[]> offsets = new HashMap<>();
    private Traverser<Entry<BroadcastKey<?>, ?>> snapshotTraverser;
    private int processorIndex;
    private Traverser<Object> traverser = Traversers.empty();

    public StreamKafkaP(
            @Nonnull FunctionEx<Context, Consumer<K, V>> kafkaConsumerFn,
            @Nonnull List<String> topics,
            @Nonnull FunctionEx<? super ConsumerRecord<K, V>, ? extends T> projectionFn,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        this(kafkaConsumerFn, new TopicsConfig().addTopics(topics), projectionFn, eventTimePolicy);
    }

    public StreamKafkaP(
            @Nonnull FunctionEx<Context, Consumer<K, V>> kafkaConsumerFn,
            @Nonnull TopicsConfig topicsConfig,
            @Nonnull FunctionEx<? super ConsumerRecord<K, V>, ? extends T> projectionFn,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        this.kafkaConsumerFn = kafkaConsumerFn;
        this.topicsConfig = topicsConfig;
        this.projectionFn = projectionFn;
        this.eventTimeMapper = new EventTimeMapper<>(eventTimePolicy);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected void init(@Nonnull Context context) {
        topics = new ArrayList<>(topicsConfig.getTopicNames());
        for (String topic : topics) {
            offsets.put(topic, new long[0]);
        }
        processorIndex = context.globalProcessorIndex();
        totalParallelism = context.totalParallelism();
        processingGuarantee = context.processingGuarantee();
        consumer = kafkaConsumerFn.apply(context);
        if (processingGuarantee == NONE) {
            warnWhenInitialOffsetsProvided();
        }
    }

    private void warnWhenInitialOffsetsProvided() {
        topicsConfig.getTopicConfigs().forEach((topicName, config) -> {
            Map<Integer, Long> partitionsInitialOffsets = config.getPartitionsInitialOffsets();
            if (!partitionsInitialOffsets.isEmpty()) {
                getLogger().warning("Provided partitions initial offsets: " + partitionsInitialOffsets
                        + " for topic: " + topicName
                        + " will be ignored, because job's processing guarantee is: " + processingGuarantee);
            }
        });
    }

    private void assignPartitions() {
        if (System.nanoTime() < nextMetadataCheck) {
            return;
        }
        for (int topicIndex = 0; topicIndex < topics.size(); topicIndex++) {
            int newPartitionCount;
            String topicName = topics.get(topicIndex);
            try {
                List<PartitionInfo> partitionInfo = consumer.partitionsFor(topicName, Duration.ofSeconds(1));
                // partitionInfo is null if the topic doesn't exist in Kafka
                newPartitionCount = partitionInfo == null ? 0 : partitionInfo.size();
            } catch (TimeoutException e) {
                // If we fail to get the metadata, don't try other topics (they are likely to fail too)
                getLogger().warning("Unable to get partition metadata, ignoring: " + e, e);
                return;
            }

            handleNewPartitions(topicIndex, newPartitionCount, false);
        }

        nextMetadataCheck = System.nanoTime() + METADATA_CHECK_INTERVAL_NANOS;
    }

    private void handleNewPartitions(int topicIndex, int newPartitionCount, boolean isRestoring) {
        String topicName = topics.get(topicIndex);
        long[] oldTopicOffsets = offsets.get(topicName);
        if (oldTopicOffsets.length >= newPartitionCount) {
            return;
        }
        // extend the offsets array for this topic
        long[] newOffsets = Arrays.copyOf(oldTopicOffsets, newPartitionCount);
        Arrays.fill(newOffsets, oldTopicOffsets.length, newOffsets.length, -1);
        offsets.put(topicName, newOffsets);
        Collection<TopicPartition> newAssignments = new ArrayList<>();
        for (int partition = oldTopicOffsets.length; partition < newPartitionCount; partition++) {
            if (handledByThisProcessor(topicIndex, partition)) {
                TopicPartition tp = new TopicPartition(topicName, partition);
                currentAssignment.put(tp, currentAssignment.size());
                newAssignments.add(tp);
            }
        }
        if (newAssignments.isEmpty()) {
            return;
        }
        getLogger().info("New partition(s) assigned: " + newAssignments);
        eventTimeMapper.addPartitions(newAssignments.size());
        consumer.assign(currentAssignment.keySet());
        if (!isRestoring) {
            if (oldTopicOffsets.length > 0) {
                // For partitions detected later during the runtime we seek to their
                // beginning. It can happen that a partition is added, and some messages
                // are added to it before we start consuming from it. If we started at the
                // current position, we will miss those, so we explicitly seek to the
                // beginning. Note that this behavior is still in-force, even when partitions
                // initial offsets was provided. This means for partitions discovered later
                // during the runtime initial offsets configuration will not be respected.
                getLogger().info("Seeking to the beginning of newly-discovered partitions: " + newAssignments);
                consumer.seekToBeginning(newAssignments);
            } else if (processingGuarantee != NONE) {
                // For processing guarantee equal to NONE partitions initial offsets
                // configuration is always ignored.
                seekToInitialOffsets(newAssignments);
            }
        }
        logFinest(getLogger(), "Currently assigned partitions: %s", currentAssignment);
    }

    private void seekToInitialOffsets(Collection<TopicPartition> newAssignments) {
        for (TopicPartition topicPartition : newAssignments) {
            int partition = topicPartition.partition();
            Long initialOffset = topicsConfig.getInitialOffsetFor(topicPartition.topic(), partition);
            if (initialOffset == null || initialOffset < 0) {
                continue;
            }
            long[] topicOffsets = offsets.get(topicPartition.topic());
            assert topicOffsets != null && topicOffsets.length > partition;
            topicOffsets[partition] = initialOffset;
            getLogger().info("Seeking to specified initial offset: " + initialOffset
                    + " of topic-partition: " + topicPartition);
            consumer.seek(topicPartition, initialOffset);
        }
    }

    @Override
    public boolean complete() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        ConsumerRecords<K, V> records = null;
        assignPartitions();
        if (!currentAssignment.isEmpty()) {
            records = consumer.poll(Duration.ZERO);
        }

        traverser = isEmpty(records)
                ? eventTimeMapper.flatMapIdle()
                : traverseIterable(records).flatMap(record -> {
            offsets.get(record.topic())[record.partition()] = record.offset();
            T projectedRecord = projectionFn.apply(record);
            if (projectedRecord == null) {
                return Traversers.empty();
            }
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            return eventTimeMapper.flatMapEvent(projectedRecord, currentAssignment.get(topicPartition),
                    record.timestamp());
        });

        emitFromTraverser(traverser);
        return false;
    }

    @Override
    public void close() {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (org.apache.kafka.common.errors.InterruptException ignored) {
            }
        }
    }

    @Override
    public boolean saveToSnapshot() {
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        if (snapshotTraverser == null) {
            Stream<Entry<BroadcastKey<?>, ?>> snapshotStream =
                    offsets.entrySet().stream()
                            .flatMap(entry -> IntStream.range(0, entry.getValue().length)
                                    .filter(partition -> entry.getValue()[partition] >= 0)
                                    .mapToObj(partition -> {
                                        TopicPartition key = new TopicPartition(entry.getKey(), partition);
                                        long offset = entry.getValue()[partition];
                                        long watermark = eventTimeMapper.getWatermark(currentAssignment.get(key));
                                        return entry(broadcastKey(key), new long[]{offset, watermark});
                                    }));
            snapshotTraverser = traverseStream(snapshotStream)
                    .onFirstNull(() -> {
                        snapshotTraverser = null;
                        if (getLogger().isFinestEnabled()) {
                            getLogger().finest("Finished saving snapshot." +
                                    " Saved offsets: " + offsets() + ", Saved watermarks: " + watermarks());
                        }
                    });

            if (processorIndex == 0) {
                Entry<BroadcastKey<?>, ?> partitionCountsItem = entry(
                        broadcastKey(PARTITION_COUNTS_SNAPSHOT_KEY),
                        topics.stream()
                                .collect(toMap(topic -> topic, topic -> offsets.get(topic).length)));
                snapshotTraverser = snapshotTraverser.append(partitionCountsItem);
            }
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Object key0, @Nonnull Object value) {
        @SuppressWarnings("unchecked")
        Object key = ((BroadcastKey<Object>) key0).key();
        if (PARTITION_COUNTS_SNAPSHOT_KEY.equals(key)) {
            @SuppressWarnings("unchecked")
            Map<String, Integer> partitionCounts = (Map<String, Integer>) value;
            for (Entry<String, Integer> partitionCountEntry : partitionCounts.entrySet()) {
                String topicName = partitionCountEntry.getKey();
                int partitionCount = partitionCountEntry.getValue();
                int topicIndex = topics.indexOf(topicName);
                assert topicIndex >= 0;
                handleNewPartitions(topicIndex, partitionCount, true);
            }
        } else {
            TopicPartition topicPartition = (TopicPartition) key;
            long[] value1 = (long[]) value;
            long offset = value1[0];
            long watermark = value1[1];
            if (!offsets.containsKey(topicPartition.topic())) {
                getLogger().warning("Offset for topic '" + topicPartition.topic()
                        + "' is restored from the snapshot, but the topic is not supposed to be read, ignoring");
                return;
            }
            int topicIndex = topics.indexOf(topicPartition.topic());
            assert topicIndex >= 0;
            handleNewPartitions(topicIndex, topicPartition.partition() + 1, true);
            if (!handledByThisProcessor(topicIndex, topicPartition.partition())) {
                return;
            }
            long[] topicOffsets = offsets.get(topicPartition.topic());
            assert topicOffsets[topicPartition.partition()] < 0 : "duplicate offset for topicPartition '" + topicPartition
                    + "' restored, offset1=" + topicOffsets[topicPartition.partition()] + ", offset2=" + offset;
            topicOffsets[topicPartition.partition()] = offset;
            consumer.seek(topicPartition, offset + 1);
            Integer partitionIndex = currentAssignment.get(topicPartition);
            assert partitionIndex != null;
            eventTimeMapper.restoreWatermark(partitionIndex, watermark);
        }
    }

    @Override
    public boolean finishSnapshotRestore() {
        if (getLogger().isFineEnabled()) {
            getLogger().fine("Finished restoring snapshot. Restored offsets: " + offsets()
                    + " and watermarks:" + watermarks());
        }
        return true;
    }

    private boolean isEmpty(ConsumerRecords<K, V> records) {
        return records == null || records.isEmpty();
    }

    private Map<TopicPartition, Long> offsets() {
        return currentAssignment.keySet().stream()
                .collect(toMap(tp -> tp, tp -> offsets.get(tp.topic())[tp.partition()]));
    }

    private Map<TopicPartition, Long> watermarks() {
        return currentAssignment.entrySet().stream()
                .collect(toMap(Entry::getKey, e -> eventTimeMapper.getWatermark(e.getValue())));
    }

    @Nonnull
    public static <K, V, T> SupplierEx<Processor> processorSupplier(
            @Nonnull FunctionEx<Context, Consumer<K, V>> kafkaConsumerSup,
            @Nonnull TopicsConfig topicsConfig,
            @Nonnull FunctionEx<? super ConsumerRecord<K, V>, ? extends T> projectionFn,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        return () -> new StreamKafkaP<>(kafkaConsumerSup, topicsConfig, projectionFn, eventTimePolicy);
    }

    private boolean handledByThisProcessor(int topicIndex, int partition) {
        return handledByThisProcessor(totalParallelism, offsets.size(), processorIndex, topicIndex, partition);
    }

    static boolean handledByThisProcessor(
            int totalParallelism, int topicsCount, int processorIndex, int topicIndex, int partition) {
        int startIndex = topicIndex * Math.max(1, totalParallelism / topicsCount);
        int topicPartitionHandledBy = (startIndex + partition) % totalParallelism;
        return topicPartitionHandledBy == processorIndex;
    }

    public static <K, V> FunctionEx<Processor.Context, Consumer<K, V>> kafkaConsumerFn(Properties properties) {
        return (c) -> new KafkaConsumer<>(properties);
    }

    public static <K, V> FunctionEx<Processor.Context, Consumer<K, V>> kafkaConsumerFn(
            DataConnectionRef dataConnectionRef
    ) {
        return (context) -> {
            KafkaDataConnection kafkaDataConnection = context
                    .dataConnectionService()
                    .getAndRetainDataConnection(dataConnectionRef.getName(), KafkaDataConnection.class);
            try {
                return kafkaDataConnection.newConsumer();
            } finally {
                kafkaDataConnection.release();
            }
        };
    }

    public static <K, V> FunctionEx<Processor.Context, Consumer<K, V>> kafkaConsumerFn(
            DataConnectionRef dataConnectionRef,
            Properties mappingProperties
    ) {
        return (context) -> {
            KafkaDataConnection kafkaDataConnection = context
                    .dataConnectionService()
                    .getAndRetainDataConnection(dataConnectionRef.getName(), KafkaDataConnection.class);
            try {
                return kafkaDataConnection.newConsumer(mappingProperties);
            } finally {
                kafkaDataConnection.release();
            }
        };
    }

}
