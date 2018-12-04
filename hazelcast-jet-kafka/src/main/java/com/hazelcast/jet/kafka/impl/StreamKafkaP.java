/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.kafka.impl;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.WatermarkSourceUtil;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.kafka.KafkaProcessors;
import com.hazelcast.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static java.lang.System.arraycopy;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * See {@link KafkaProcessors#streamKafkaP}.
 */
public final class StreamKafkaP<K, V, T> extends AbstractProcessor {

    private static final long METADATA_CHECK_INTERVAL_NANOS = SECONDS.toNanos(5);
    private static final int POLL_TIMEOUT_MS = 50;

    Map<TopicPartition, Integer> currentAssignment = new HashMap<>();

    private final Properties properties;
    private final List<String> topics;
    private final DistributedFunction<? super ConsumerRecord<K, V>, ? extends T> projectionFn;
    private final WatermarkSourceUtil<? super T> watermarkSourceUtil;
    private int totalParallelism;
    private boolean snapshottingEnabled;

    private KafkaConsumer<K, V> consumer;
    private final int[] partitionCounts;
    private long nextMetadataCheck = Long.MIN_VALUE;

    /**
     * Key: topicName<br>
     * Value: partition offsets, at index I is offset for partition I.<br>
     * Offsets are -1 initially and remain -1 for partitions not assigned to this instance.
     */
    private final Map<String, long[]> offsets = new HashMap<>();
    private Traverser<Entry<BroadcastKey<TopicPartition>, long[]>> snapshotTraverser;
    private int processorIndex;
    private Traverser<Object> traverser = Traversers.empty();

    StreamKafkaP(
            @Nonnull Properties properties,
            @Nonnull List<String> topics,
            @Nonnull DistributedFunction<? super ConsumerRecord<K, V>, ? extends T> projectionFn,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        this.properties = properties;
        this.topics = topics;
        this.projectionFn = projectionFn;
        watermarkSourceUtil = new WatermarkSourceUtil<>(eventTimePolicy);
        partitionCounts = new int[topics.size()];
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected void init(@Nonnull Context context) {
        processorIndex = context.globalProcessorIndex();
        totalParallelism = context.totalParallelism();
        snapshottingEnabled = context.snapshottingEnabled();
        consumer = new KafkaConsumer<>(properties);
        assignPartitions(false);
    }

    private void assignPartitions(boolean seekToBeginning) {
        if (System.nanoTime() < nextMetadataCheck) {
            return;
        }
        boolean allEqual = true;
        for (int i = 0; i < topics.size(); i++) {
            int newCount = consumer.partitionsFor(topics.get(i)).size();
            allEqual &= partitionCounts[i] == newCount;
            partitionCounts[i] = newCount;
        }
        if (allEqual) {
            return;
        }

        KafkaPartitionAssigner assigner = new KafkaPartitionAssigner(topics, partitionCounts, totalParallelism);
        Set<TopicPartition> newAssignments = assigner.topicPartitionsFor(processorIndex);
        logFinest(getLogger(), "Currently assigned partitions: %s", newAssignments);

        newAssignments.removeAll(currentAssignment.keySet());
        if (!newAssignments.isEmpty()) {
            getLogger().info("Partition assignments changed, added partitions: " + newAssignments);
            for (TopicPartition tp : newAssignments) {
                currentAssignment.put(tp, currentAssignment.size());
            }
            watermarkSourceUtil.increasePartitionCount(currentAssignment.size());
            consumer.assign(currentAssignment.keySet());
            if (seekToBeginning) {
                // for newly detected partitions, we should always seek to the beginning
                consumer.seekToBeginning(newAssignments);
            }
        }

        createOrExtendOffsetsArrays();
        nextMetadataCheck = System.nanoTime() + METADATA_CHECK_INTERVAL_NANOS;
    }

    private void createOrExtendOffsetsArrays() {
        for (int topicIdx = 0; topicIdx < partitionCounts.length; topicIdx++) {
            int newPartitionCount = partitionCounts[topicIdx];
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
        if (!emitFromTraverser(traverser)) {
            return false;
        }

        try {
            ConsumerRecords<K, V> records = null;
            assignPartitions(true);
            if (!currentAssignment.isEmpty()) {
                records = consumer.poll(POLL_TIMEOUT_MS);
            }

            traverser = isEmpty(records)
                    ? watermarkSourceUtil.handleNoEvent()
                    : traverseIterable(records).flatMap(record -> {
                        offsets.get(record.topic())[record.partition()] = record.offset();
                        T projectedRecord = projectionFn.apply(record);
                        if (projectedRecord == null) {
                            return Traversers.empty();
                        }
                        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                        return watermarkSourceUtil.handleEvent(projectedRecord, currentAssignment.get(topicPartition));
                    });

            emitFromTraverser(traverser);

            if (!snapshottingEnabled) {
                consumer.commitSync();
            }
        } catch (org.apache.kafka.common.errors.InterruptException e) {
            return false;
        }

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
            Stream<Entry<BroadcastKey<TopicPartition>, long[]>> snapshotStream =
                    offsets.entrySet().stream()
                           .flatMap(entry -> IntStream.range(0, entry.getValue().length)
                                  .filter(partition -> entry.getValue()[partition] >= 0)
                                  .mapToObj(partition -> {
                                      TopicPartition key = new TopicPartition(entry.getKey(), partition);
                                      long offset = entry.getValue()[partition];
                                      long watermark = watermarkSourceUtil.getWatermark(currentAssignment.get(key));
                                      return entry(broadcastKey(key), new long[]{offset, watermark});
                                  }));
            snapshotTraverser = traverseStream(snapshotStream)
                    .onFirstNull(() -> {
                        snapshotTraverser = null;
                        if (getLogger().isFineEnabled()) {
                            getLogger().fine("Finished saving snapshot." +
                                    " Saved offsets: " + offsets() + ", Saved watermarks: " + watermarks());
                        }
                    });
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        TopicPartition topicPartition = ((BroadcastKey<TopicPartition>) key).key();
        long[] value1 = (long[]) value;
        long offset = value1[0];
        long watermark = value1[1];
        long[] topicOffsets = offsets.get(topicPartition.topic());
        if (topicOffsets == null) {
            getLogger().warning("Offset for topic '" + topicPartition.topic()
                    + "' is present in snapshot, but the topic is not supposed to be read");
            return;
        }
        if (topicPartition.partition() >= topicOffsets.length) {
            getLogger().warning("Offset for partition '" + topicPartition + "' is present in snapshot," +
                    " but that topic currently has only " + topicOffsets.length + " partitions");
        }
        Integer partitionIndex = currentAssignment.get(topicPartition);
        if (partitionIndex != null) {
            assert topicOffsets[topicPartition.partition()] < 0 : "duplicate offset for topicPartition '" + topicPartition
                    + "' restored, offset1=" + topicOffsets[topicPartition.partition()] + ", offset2=" + offset;
            topicOffsets[topicPartition.partition()] = offset;
            consumer.seek(topicPartition, offset + 1);
            watermarkSourceUtil.restoreWatermark(partitionIndex, watermark);
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
                .collect(Collectors.toMap(tp -> tp, tp -> offsets.get(tp.topic())[tp.partition()]));
    }

    private Map<TopicPartition, Long> watermarks() {
        return currentAssignment.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, e -> watermarkSourceUtil.getWatermark(e.getValue())));
    }

    @Nonnull
    public static <K, V, T> DistributedSupplier<Processor> processorSupplier(
            @Nonnull Properties properties,
            @Nonnull List<String> topics,
            @Nonnull DistributedFunction<? super ConsumerRecord<K, V>, ? extends T> projectionFn,
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        return () -> new StreamKafkaP<>(properties, topics, projectionFn, eventTimePolicy);
    }

    /**
     * Helper class for assigning partitions to processor indices in a round robin fashion
     */
    static class KafkaPartitionAssigner {

        private final List<String> topics;
        private final int[] partitionCounts;
        private final int totalParallelism;

        KafkaPartitionAssigner(List<String> topics, int[] partitionCounts, int totalParallelism) {
            Preconditions.checkTrue(topics.size() == partitionCounts.length,
                    "Different length between topics and partition counts");
            this.topics = topics;
            this.partitionCounts = partitionCounts;
            this.totalParallelism = totalParallelism;
        }

        Set<TopicPartition> topicPartitionsFor(int processorIndex) {
            Set<TopicPartition> assignments = new LinkedHashSet<>();
            for (int topicIndex = 0; topicIndex < topics.size(); topicIndex++) {
                for (int partition = 0; partition < partitionCounts[topicIndex]; partition++) {
                    if (processorIndexFor(topicIndex, partition) == processorIndex) {
                        assignments.add(new TopicPartition(topics.get(topicIndex), partition));
                    }
                }
            }
            return assignments;
        }

        private int processorIndexFor(int topicIndex, int partition) {
            int startIndex = topicIndex * Math.max(1, totalParallelism / topics.size());
            return (startIndex + partition) % totalParallelism;
        }
    }
}
