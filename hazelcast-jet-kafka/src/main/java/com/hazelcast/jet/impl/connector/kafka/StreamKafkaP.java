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

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * See {@link com.hazelcast.jet.core.processor.KafkaProcessors#streamKafka(
 * Properties, String...)}.
 */
public final class StreamKafkaP extends AbstractProcessor implements Closeable {

    private static final long KAFKA_DEFAULT_REFRESH_INTERVAL = 300_000;
    private static final int POLL_TIMEOUT_MS = 50;

    private final Properties properties;
    private final List<String> topicIds;
    private final int processorCount;
    private boolean snapshottingEnabled;
    private KafkaConsumer<?, ?> consumer;

    // next possible partition index assignable to this processor, index is the topic index in topicIds
    private final int[] nextAssignablePtions;

    private long nextPartitionCheck = Long.MIN_VALUE;

    private final Map<TopicPartition, Long> offsets = new HashMap<>();
    private Traverser<Entry<TopicPartition, Long>> snapshotTraverser;
    private Set<TopicPartition> assignment = new HashSet<>();
    private long metadataRefreshInterval;

    StreamKafkaP(Properties properties, List<String> topicIds, int processorCount, int processorIndex,
                 long metadataRefreshInterval) {
        this.properties = properties;
        this.properties.putAll(properties);

        this.topicIds = topicIds;
        this.processorCount = processorCount;

        this.nextAssignablePtions = new int[topicIds.size()];
        Arrays.fill(nextAssignablePtions, processorIndex);
        this.metadataRefreshInterval = metadataRefreshInterval;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        snapshottingEnabled = context.snapshottingEnabled();
        consumer = new KafkaConsumer<>(properties);
        reassignPartitions();
    }

    private void reassignPartitions() {
        boolean changed = false;
        // check for added partitions (kafka doesn't support partition removal). Initially, all partitions are added.
        for (int topicIdx = 0; topicIdx < topicIds.size(); topicIdx++) {
            String topicName = topicIds.get(topicIdx);
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topicName);
            getLogger().finest("Num of ptions for topic " + topicName + ": " + partitionInfos.size());
            while (nextAssignablePtions[topicIdx] < partitionInfos.size()) {
                int partition = nextAssignablePtions[topicIdx];
                nextAssignablePtions[topicIdx] += processorCount;
                assert partitionInfos.get(partition).partition() == partition;
                assignment.add(new TopicPartition(topicName, partition));
                changed = true;
            }
        }

        if (changed) {
            getLogger().info("Partition assignment changed: " + assignment);
            consumer.assign(assignment);
        }
        nextPartitionCheck = System.nanoTime() + MILLISECONDS.toNanos(metadataRefreshInterval);
    }

    @Override
    public boolean complete() {
        if (System.nanoTime() >= nextPartitionCheck) {
            reassignPartitions();
        }

        if (!assignment.isEmpty()) {
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
        } else {
            LockSupport.parkNanos(MILLISECONDS.toNanos(POLL_TIMEOUT_MS));
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
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        TopicPartition partition = (TopicPartition) key;
        long offset = (long) value;
        if (assignment.contains(partition)) {
            consumer.seek(partition, offset);
        }
    }

    /**
     * Please use {@link com.hazelcast.jet.core.processor.KafkaProcessors#streamKafka(Properties, String...)}.
     */
    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final List<String> topicIds;
        private final int memberCount;
        private final int memberIndex;
        private final long metadataRefreshInterval;

        private final Properties properties;
        private int localParallelism;

        private transient List<StreamKafkaP> processors;
        private transient ILogger logger;

        Supplier(Properties properties, List<String> topicIds, int memberCount, int memberIndex,
                 long metadataRefreshInterval) {
            this.properties = properties;
            this.topicIds = topicIds;
            this.memberCount = memberCount;
            this.memberIndex = memberIndex;
            this.metadataRefreshInterval = metadataRefreshInterval;
        }

        @Override
        public void init(@Nonnull Context context) {
            localParallelism = context.localParallelism();
            logger = context.jetInstance().getHazelcastInstance().getLoggingService().getLogger(getClass());
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            // localParallelism is equal on all members
            processors = IntStream.range(0, count)
                                         .mapToObj(i -> new StreamKafkaP(properties, topicIds,
                                                 memberCount * localParallelism, memberIndex * localParallelism + i,
                                                 metadataRefreshInterval))
                                         .collect(toList());
            return (List) processors;
        }

        @Override
        public void complete(Throwable error) {
            Throwable firstError = null;
            // close all processors, ignoring their failures and throwing the first failure (if any)
            for (StreamKafkaP p : processors) {
                try {
                    p.close();
                } catch (Throwable e) {
                    if (firstError == null) {
                        firstError = e;
                    } else {
                        logger.severe(e);
                    }
                }
            }

            if (firstError != null) {
                throw sneakyThrow(firstError);
            }
        }
    }

    public static class MetaSupplier implements ProcessorMetaSupplier {

        private final Properties properties;
        private final List<String> topicIds;
        private final long metadataRefreshInterval;

        public MetaSupplier(Properties properties, List<String> topicIds) {
            this.properties = new Properties();
            this.topicIds = topicIds;

            this.properties.putAll(properties);

            // Save the value of metadata.max.age.ms to a variable and zero it in the properties.
            // We'll do metadata refresh on our own.
            if (properties.containsKey("metadata.max.age.ms")) {
                metadataRefreshInterval = Long.parseLong(properties.getProperty("metadata.max.age.ms"));
            } else {
                metadataRefreshInterval = KAFKA_DEFAULT_REFRESH_INTERVAL;
            }
            // Set metadata caching to 1 second: we get metadata for multiple partitions one by one, but
            // internally it is fetched at once. This enables for the other calls to just query fetched metadata.
            this.properties.setProperty("metadata.max.age.ms", "1000");
        }

        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new Supplier(properties, topicIds, addresses.size(), addresses.indexOf(address),
                    metadataRefreshInterval);
        }
    }
}
