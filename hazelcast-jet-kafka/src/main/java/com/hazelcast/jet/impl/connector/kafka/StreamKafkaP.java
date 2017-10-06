/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.connector.kafka;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.CloseableProcessorSupplier;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.nio.Address;
import com.hazelcast.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static java.lang.System.arraycopy;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * See {@link com.hazelcast.jet.core.processor.KafkaProcessors#streamKafkaP(
 *Properties, String...)}.
 */
public final class StreamKafkaP<K, V, T> extends AbstractProcessor implements Closeable {

    private static final long KAFKA_DEFAULT_REFRESH_INTERVAL = 300_000;
    private static final int POLL_TIMEOUT_MS = 50;

    private final Properties properties;
    private final List<String> topics;
    private final DistributedBiFunction<K, V, T> projectionFn;
    private final int globalParallelism;
    private boolean snapshottingEnabled;
    private KafkaConsumer<Object, Object> consumer;

    private long nextPartitionCheck = Long.MIN_VALUE;

    /**
     * Key: topicName<br>
     * Value: partition offsets, at index I is offset for partition I.<br>
     * Offsets are -1 initially and for unassigned partitions.
     */
    private final Map<String, long[]> offsets = new HashMap<>();
    private Traverser<Entry<BroadcastKey<TopicPartition>, Long>> snapshotTraverser;
    private Set<TopicPartition> currentAssignment = new HashSet<>();
    private long metadataRefreshInterval;
    private int processorIndex;
    private Traverser<T> traverser;
    private ConsumerRecord<Object, Object> lastEmittedItem;

    StreamKafkaP(Properties properties, List<String> topics, DistributedBiFunction<K, V, T> projectionFn,
                 int globalParallelism, long metadataRefreshInterval) {
        this.properties = properties;
        this.topics = topics;
        this.projectionFn = projectionFn;
        this.globalParallelism = globalParallelism;
        this.metadataRefreshInterval = metadataRefreshInterval;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        processorIndex = context.globalProcessorIndex();
        snapshottingEnabled = context.snapshottingEnabled();
        consumer = new KafkaConsumer<>(properties);
        assignPartitions(false);
    }

    private void assignPartitions(boolean seekToBeginning) {
        List<Integer> partitionCounts = topics.stream().map(t -> consumer.partitionsFor(t).size()).collect(toList());
        KafkaPartitionAssigner assigner = new KafkaPartitionAssigner(topics, partitionCounts, globalParallelism);
        Set<TopicPartition> newAssignments = assigner.topicPartitionsFor(processorIndex);
        logFinest(getLogger(), "Currently assigned partitions: %s", newAssignments);

        newAssignments.removeAll(currentAssignment);
        if (!newAssignments.isEmpty()) {
            getLogger().info("Partition assignments changed, new partitions: " + newAssignments);
            currentAssignment.addAll(newAssignments);
            consumer.assign(currentAssignment);
            if (seekToBeginning) {
                // for newly detected partitions, we should always seek to the beginning
                consumer.seekToBeginning(newAssignments);
            }
        }

        createOrExtendOffsetsArrays(partitionCounts);
        nextPartitionCheck = System.nanoTime() + MILLISECONDS.toNanos(metadataRefreshInterval);
    }

    private void createOrExtendOffsetsArrays(List<Integer> partitionCounts) {
        for (int topicIdx = 0; topicIdx < partitionCounts.size(); topicIdx++) {
            int newPartitionCount = partitionCounts.get(topicIdx);
            String topicName = topics.get(topicIdx);
            long[] oldOffsets = offsets.get(topicName);
            if (oldOffsets != null && oldOffsets.length == newPartitionCount) {
                continue;
            }
            long[] newOffsets = new long[newPartitionCount];
            Arrays.fill(newOffsets, -1);
            if (oldOffsets != null) {
                arraycopy(oldOffsets, 0, newOffsets, 0, oldOffsets.length);
            }
            offsets.put(topicName, newOffsets);
        }
    }

    @Override
    public boolean complete() {
        if (System.nanoTime() >= nextPartitionCheck) {
            assignPartitions(true);
        }
        if (currentAssignment.isEmpty()) {
            return false;
        }
        if (traverser == null) {
            ConsumerRecords<Object, Object> records = consumer.poll(POLL_TIMEOUT_MS);
            if (records.isEmpty()) {
                return false;
            }
            traverser = traverseIterable(records)
                    .peek(r -> lastEmittedItem = r)
                    .map(r -> projectionFn.apply((K) r.key(), (V) r.value()))
                    .onFirstNull(() -> traverser = null);
        }

        emitFromTraverser(traverser,
                e -> offsets.get(lastEmittedItem.topic())[lastEmittedItem.partition()] = lastEmittedItem.offset());

        if (!snapshottingEnabled) {
            consumer.commitSync();
        }

        return false;
    }

    @Override
    public void close() {
        consumer.close();
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean saveToSnapshot() {
        if (snapshotTraverser == null) {
            Stream<Entry<BroadcastKey<TopicPartition>, Long>> snapshotStream =
                    offsets.entrySet().stream()
                           .flatMap(entry -> IntStream.range(0, entry.getValue().length)
                                  .filter(partition -> entry.getValue()[partition] >= 0)
                                  .mapToObj(partition -> entry(
                                          broadcastKey(new TopicPartition(entry.getKey(), partition)),
                                          entry.getValue()[partition])));
            snapshotTraverser = traverseStream(snapshotStream)
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        TopicPartition topicPartition = ((BroadcastKey<TopicPartition>) key).key();
        long offset = (long) value;
        long[] topicOffsets = offsets.get(topicPartition.topic());
        if (topicOffsets == null) {
            getLogger().severe("Offset for topic '" + topicPartition.topic()
                    + "' is present in snapshot, but the topic is not supposed to be read");
            return;
        }
        if (topicPartition.partition() >= topicOffsets.length) {
            getLogger().severe("Offset for partition '" + topicPartition + "' is present in snapshot," +
                    " but that topic currently has only " + topicOffsets.length + " partitions");
        }
        if (currentAssignment.contains(topicPartition)) {
            assert topicOffsets[topicPartition.partition()] < 0 : "duplicate offset for topicPartition '" + topicPartition
                    + "' restored, offset1=" + topicOffsets[topicPartition.partition()] + ", offset2=" + offset;
            topicOffsets[topicPartition.partition()] = offset;
            consumer.seek(topicPartition, offset + 1);
        }
    }

    public static class MetaSupplier<K, V, T> implements ProcessorMetaSupplier {

        private final Properties properties;
        private final List<String> topics;
        private final DistributedBiFunction<K, V, T> projectionFn;
        private final long metadataRefreshInterval;
        private int totalParallelism;

        public MetaSupplier(Properties properties, List<String> topics, DistributedBiFunction<K, V, T> projectionFn) {
            this.properties = new Properties();
            this.properties.putAll(properties);
            this.topics = topics;
            this.projectionFn = projectionFn;

            // Save the value of metadata.max.age.ms to a variable and zero it in the properties.
            // We'll do metadata refresh on our own.
            if (properties.containsKey("metadata.max.age.ms")) {
                metadataRefreshInterval = Long.parseLong(properties.getProperty("metadata.max.age.ms"));
            } else {
                metadataRefreshInterval = KAFKA_DEFAULT_REFRESH_INTERVAL;
            }
            // Set metadata caching to 1 second: we read the metadata for multiple partitions one by one. If we
            // set this to 0, consumer.partitionsFor(topic) would probably fetch metadata for each topic anew.
            this.properties.setProperty("metadata.max.age.ms", "1000");
        }

        @Override
        public void init(@Nonnull Context context) {
            totalParallelism = context.totalParallelism();
        }

        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new CloseableProcessorSupplier<>(
                    () -> new StreamKafkaP(properties, topics, projectionFn, totalParallelism, metadataRefreshInterval));
        }
    }

    /**
     * Helper class for assigning partitions to processor indices in a round robin fashion
     */
    static class KafkaPartitionAssigner {

        private final List<String> topics;
        private final List<Integer> partitionCounts;
        private final int globalParallelism;

        KafkaPartitionAssigner(List<String> topics, List<Integer> partitionCounts, int globalParallelism) {
            Preconditions.checkTrue(topics.size() == partitionCounts.size(),
                    "Different length between topics and partition counts");
            this.topics = topics;
            this.partitionCounts = partitionCounts;
            this.globalParallelism = globalParallelism;
        }

        Set<TopicPartition> topicPartitionsFor(int processorIndex) {
            Set<TopicPartition> assignments = new LinkedHashSet<>();
            for (int topicIndex = 0; topicIndex < topics.size(); topicIndex++) {
                for (int partition = 0; partition < partitionCounts.get(topicIndex); partition++) {
                    if (processorIndexFor(topicIndex, partition) == processorIndex) {
                        assignments.add(new TopicPartition(topics.get(topicIndex), partition));
                    }
                }
            }
            return assignments;
        }

        private int processorIndexFor(int topicIndex, int partition) {
            int startIndex = topicIndex * Math.max(1, globalParallelism / topics.size());
            return (startIndex + partition) % globalParallelism;
        }
    }
}
