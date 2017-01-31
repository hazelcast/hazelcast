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

package com.hazelcast.jet.connector.kafka;

import com.hazelcast.core.Member;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.Optional;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Processors.NoopProcessor;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.nio.Address;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;


/**
 * Kafka Consumer for Jet, emits records read from Kafka as Map.Entry
 *
 * @param <K> type of the message key
 * @param <V> type of the message value
 */
public final class KafkaReader<K, V> extends AbstractProcessor implements Closeable {
    private static final int POLL_TIMEOUT_MS = 100;
    private final Properties properties;
    private final String topic;
    private final List<Integer> partitions;
    private KafkaConsumer<byte[], byte[]> consumer;
    private long[] partitionOffsets;
    private final Function<byte[], K> deserializeKey;
    private final Function<byte[], V> deserializeValue;

    private KafkaReader(String topic, Properties properties, List<Integer> partitions,
                        Function<byte[], K> deserializeKey, Function<byte[], V> deserializeValue) {
        this.topic = topic;
        this.properties = properties;
        this.partitions = partitions;
        this.partitionOffsets = new long[partitions.stream().max(Comparator.naturalOrder()).get() + 1];
        this.deserializeKey = deserializeKey;
        this.deserializeValue = deserializeValue;
        Arrays.fill(partitionOffsets, -1L);

    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        consumer = new KafkaConsumer<>(properties);
        consumer.assign(partitions.stream().map(i -> new TopicPartition(topic, i)).collect(toList()));
    }

    private static Properties getProperties(String zkAddress, String groupId, String brokerConnectionString) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkAddress);
        props.put("group.id", groupId);
        props.put("bootstrap.servers", brokerConnectionString);
        props.put("key.deserializer", ByteArrayDeserializer.class.getCanonicalName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getCanonicalName());
        return props;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_TIMEOUT_MS);
        if (records.isEmpty()) {
            return false;
        }
        for (ConsumerRecord<byte[], byte[]> record : records) {
            K key = Optional.ofNullable(record.key()).map(deserializeKey).orElse(null);
            V value = deserializeValue.apply(record.value());

            partitionOffsets[record.partition()] = record.offset();
            emit(new AbstractMap.SimpleImmutableEntry<>(key, value));
            if (getOutbox().isHighWater()) {
                for (int p = 0; p < partitionOffsets.length; p++) {
                    long offset = partitionOffsets[p];
                    if (offset != -1) {
                        consumer.seek(new TopicPartition(topic, p), offset);
                    }
                }
                return false;
            }
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }

    /**
     * Creates supplier for consuming kafka topics.
     *
     * @param <K>                    type of keys read
     * @param <V>                    type of values read
     * @param zkAddress              zookeeper address
     * @param groupId                kafka consumer group name
     * @param topicId                kafka topic name
     * @param brokerConnectionString kafka broker address
     * @param deserializeKey         function for deserializing keys
     * @param deserializeValue       function for deserializing values
     * @return {@link ProcessorMetaSupplier} supplier
     */
    static <K, V> ProcessorMetaSupplier supplier(String zkAddress, String groupId, String topicId,
                                                 String brokerConnectionString,
                                                 Function<byte[], K> deserializeKey,
                                                 Function<byte[], V> deserializeValue) {
        return new MetaSupplier<>(topicId,
                getProperties(zkAddress, groupId, brokerConnectionString),
                deserializeKey, deserializeValue);
    }


    private static final class MetaSupplier<K, V> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;
        private final String topicId;
        private Properties properties;
        private final Function<byte[], K> deserializeKey;
        private final Function<byte[], V> deserializeValue;

        private transient Map<Address, List<Integer>> partitionMap;

        private MetaSupplier(String topicId, Properties properties,
                             Function<byte[], K> deserializeKey,
                             Function<byte[], V> deserializeValue) {
            this.topicId = topicId;
            this.properties = properties;
            this.deserializeKey = deserializeKey;
            this.deserializeValue = deserializeValue;
        }

        @Override
        public void init(@Nonnull Context context) {
            partitionMap = new HashMap<>();
            Member[] members = context.jetInstance().getCluster().getMembers().toArray(new Member[0]);
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties);
            List<PartitionInfo> partitions = consumer.partitionsFor(topicId);
            int memberSize = members.length;
            for (int i = 0; i < partitions.size(); i++) {
                PartitionInfo partition = partitions.get(i);
                Member member = members[i % memberSize];
                partitionMap.computeIfAbsent(member.getAddress(), v -> new ArrayList<>()).add(partition.partition());
            }
            consumer.close();
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new Supplier<>(topicId, properties, partitionMap.get(address),
                    deserializeKey, deserializeValue);
        }
    }

    private static class Supplier<K, V> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String topicId;
        private final Properties properties;
        private List<Integer> ownedPartitions;
        private final Function<byte[], K> deserializeKey;
        private final Function<byte[], V> deserializeValue;

        private transient List<Processor> processors;

        Supplier(String topicId, Properties properties, List<Integer> ownedPartitions,
                 Function<byte[], K> deserializeKey, Function<byte[], V> deserializeValue) {
            this.properties = properties;
            this.topicId = topicId;
            this.ownedPartitions = ownedPartitions;
            this.deserializeKey = deserializeKey;
            this.deserializeValue = deserializeValue;
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            Map<Integer, List<Integer>> processorToPartitions =
                    IntStream.range(0, ownedPartitions.size()).boxed()
                             .map(i -> new SimpleImmutableEntry<>(i, ownedPartitions.get(i)))
                             .collect(groupingBy(e -> e.getKey() % count,
                                     mapping(Entry::getValue, toList())));
            IntStream.range(0, count)
                     .forEach(processor -> processorToPartitions.computeIfAbsent(processor, x -> emptyList()));

            return (processors = processorToPartitions
                    .values().stream()
                    .map(partitions -> !partitions.isEmpty()
                            ? new KafkaReader<>(topicId, properties, partitions, deserializeKey, deserializeValue)
                            : new NoopProcessor()
                    )
                    .collect(toList()));
        }

        @Override
        public void complete(Throwable error) {
            processors.stream()
                      .filter(p -> p instanceof KafkaReader)
                      .map(p -> (KafkaReader) p)
                      .forEach(p -> Util.uncheckRun(p::close));
        }
    }
}
