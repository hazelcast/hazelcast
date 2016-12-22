/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.impl.AbstractProducer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.serialization.SerializationService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

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
public class KafkaReader<K, V> extends AbstractProducer {
    private static final int POLL_TIMEOUT_MS = 100;
    private static final ILogger LOGGER = Logger.getLogger(KafkaReader.class);
    private final SerializationService serializationService;
    private final Properties properties;
    private final String topic;
    private final List<Integer> partitions;
    private KafkaConsumer<byte[], byte[]> consumer;
    private long[] partitionOffsets;

    protected KafkaReader(String topic, Properties properties, List<Integer> partitions,
                          SerializationService serializationService) {
        this.topic = topic;
        this.properties = properties;
        this.partitions = partitions;
        this.serializationService = serializationService;
        this.partitionOffsets = new long[partitions.stream().max(Comparator.naturalOrder()).get() + 1];
        Arrays.fill(partitionOffsets, -1L);

    }

    @Override
    public void init(@Nonnull Outbox outbox) {
        super.init(outbox);
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
        // todo : break the loop then call consumer.close() when we have proper cancellation mechanism
        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(POLL_TIMEOUT_MS);
            if (records.isEmpty()) {
                return false;
            }
            for (ConsumerRecord<byte[], byte[]> record : records) {
                K key = serializationService.toObject(new HeapData(record.key()));
                V value = serializationService.toObject(new HeapData(record.value()));
                partitionOffsets[record.partition()] = record.offset();
                emit(new AbstractMap.SimpleImmutableEntry<>(key, value));
                if (getOutbox().isHighWater()) {
                    for (int i = 0; i < partitionOffsets.length; i++) {
                        long offset = partitionOffsets[i];
                        if (offset != -1) {
                            consumer.seek(new TopicPartition(topic, i), offset);
                        }
                    }
                    return false;
                }
            }
        }
    }

    /**
     * Creates supplier for consuming kafka topics.
     *
     * @param zkAddress              zookeeper address
     * @param groupId                kafka consumer group name
     * @param topicId                kafka topic name
     * @param brokerConnectionString kafka broker address
     * @return {@link ProcessorMetaSupplier} supplier
     */
    public static ProcessorMetaSupplier supplier(String zkAddress, String groupId, String topicId,
                                                 String brokerConnectionString) {

        return new MetaSupplier(topicId, getProperties(zkAddress, groupId, brokerConnectionString));
    }

    @SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
    private static final class MetaSupplier implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;
        private final String topicId;
        private Properties properties;

        private transient Map<Address, List<Integer>> partitionMap = new HashMap<>();

        private MetaSupplier(String topicId, Properties properties) {
            this.topicId = topicId;
            this.properties = properties;
        }

        @Override
        public void init(Context context) {
            Member[] members = context.getHazelcastInstance().getCluster().getMembers().toArray(new Member[0]);
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

        @Override
        public ProcessorSupplier get(Address address) {
            return new Supplier(topicId, properties, partitionMap.get(address));
        }
    }


    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String topicId;
        private final Properties properties;
        private List<Integer> ownedPartitions;
        private transient Context context;

        Supplier(String topicId, Properties properties,
                 List<Integer> ownedPartitions) {
            this.properties = properties;
            this.topicId = topicId;
            this.ownedPartitions = ownedPartitions;
        }

        @Override
        public void init(Context context) {
            this.context = context;
        }

        @Override
        public List<Processor> get(int count) {
            HazelcastInstanceImpl hazelcastInstance = (HazelcastInstanceImpl) context.getHazelcastInstance();
            SerializationService serializationService = hazelcastInstance.node.nodeEngine.getSerializationService();
            Map<Integer, List<Integer>> processorToPartitions = IntStream.range(0, ownedPartitions.size()).boxed()
                    .map(i -> new SimpleImmutableEntry<>(i, ownedPartitions.get(i)))
                    .collect(groupingBy(e -> e.getKey() % count,
                            mapping(e -> e.getValue(), toList())));
            IntStream.range(0, count)
                    .forEach(processor -> processorToPartitions.computeIfAbsent(processor, x -> emptyList()));
            return processorToPartitions
                    .values().stream()
                    .map(partitions -> !partitions.isEmpty()
                                    ? new KafkaReader(topicId, properties, partitions, serializationService)
                                    : new AbstractProducer() {
                            }
                    )
                    .collect(toList());
        }
    }
}
