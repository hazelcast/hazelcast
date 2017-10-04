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
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.CloseableProcessorSupplier;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.nio.Address;
import com.hazelcast.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * See {@link com.hazelcast.jet.core.processor.KafkaProcessors#streamKafka(
 *Properties, String...)}.
 */
public final class StreamKafkaP extends AbstractProcessor implements Closeable {

    private static final long KAFKA_DEFAULT_REFRESH_INTERVAL = 300_000;
    private static final int POLL_TIMEOUT_MS = 50;

    private final Properties properties;
    private final List<String> topics;
    private final int globalParallelism;
    private boolean snapshottingEnabled;
    private KafkaConsumer<?, ?> consumer;

    private long nextPartitionCheck = Long.MIN_VALUE;

    private final Map<TopicPartition, Long> offsets = new HashMap<>();
    private Traverser<Entry<BroadcastKey<TopicPartition>, Long>> snapshotTraverser;
    private Set<TopicPartition> currentAssignment = new HashSet<>();
    private long metadataRefreshInterval;
    private int processorIndex;

    StreamKafkaP(Properties properties, List<String> topics, int globalParallelism, long metadataRefreshInterval) {
        this.properties = properties;
        this.topics = topics;
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
        nextPartitionCheck = System.nanoTime() + MILLISECONDS.toNanos(metadataRefreshInterval);
    }

    @Override
    public boolean complete() {
        if (System.nanoTime() >= nextPartitionCheck) {
            assignPartitions(true);
        }

        if (currentAssignment.isEmpty()) {
            // this happens when there are less kafka partitions than globalParallelism of
            // this vertex. Processor will just idle rather then finish
            // as there might be more partitions that can be assigned in the future.
            LockSupport.parkNanos(MILLISECONDS.toNanos(POLL_TIMEOUT_MS));
            return false;
        }

        ConsumerRecords<?, ?> records = consumer.poll(POLL_TIMEOUT_MS);
        for (ConsumerRecord<?, ?> r : records) {
            if (snapshottingEnabled) {
                offsets.put(new TopicPartition(r.topic(), r.partition()), r.offset());
            }
            emit(entry(r.key(), r.value()));
        }
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
            snapshotTraverser = Traversers.traverseIterable(offsets.entrySet())
                                          .map(e -> entry(broadcastKey(e.getKey()), e.getValue()))
                                          .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        TopicPartition partition = ((BroadcastKey<TopicPartition>) key).key();
        long offset = (long) value;
        if (currentAssignment.contains(partition)) {
            Long oldValue = offsets.put(partition, offset);
            assert oldValue == null : "duplicate offset for partition '" + partition + "' restored, offset1="
                    + oldValue + ", offset2=" + offset;
            consumer.seek(partition, offset + 1);
        }
    }

    public static class MetaSupplier implements ProcessorMetaSupplier {

        private final Properties properties;
        private final List<String> topics;
        private final long metadataRefreshInterval;
        private int totalParallelism;

        public MetaSupplier(Properties properties, List<String> topics) {
            this.properties = new Properties();
            this.topics = topics;

            this.properties.putAll(properties);

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
                    () -> new StreamKafkaP(properties, topics, totalParallelism, metadataRefreshInterval));
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
