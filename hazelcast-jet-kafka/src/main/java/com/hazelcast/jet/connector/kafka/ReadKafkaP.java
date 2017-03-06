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

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.nio.Address;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;


/**
 * Kafka Consumer for Jet, emits records read from Kafka as {@code Map.Entry}.
 *
 * @param <K> type of the message key
 * @param <V> type of the message value
 */
public final class ReadKafkaP<K, V> extends AbstractProcessor implements Closeable {

    private static final int POLL_TIMEOUT_MS = 100;
    private final Properties properties;
    private final String topic;
    private KafkaConsumer<K, V> consumer;

    private ReadKafkaP(String topic, Properties properties) {
        this.topic = topic;
        this.properties = properties;

    }

    /**
     * Returns a meta-supplier of processors that consume a kafka topic and emit
     * items from it as {@code Map.Entry} instances.
     * <p>
     * <p>
     * You can specify a partition to offset mapper to mark the start offset of each partition.
     * Any negative value as an offset will throw {@code IllegalArgumentException}
     * </p>
     *
     * @param <K>        type of keys read
     * @param <V>        type of values read
     * @param topicId    kafka topic name
     * @param properties consumer properties which should contain consumer group name,
     *                   broker address and key/value deserializers
     */
    public static <K, V> ProcessorMetaSupplier readKafka(String topicId, Properties properties) {
        return new MetaSupplier<>(topicId, properties);
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        properties.put("enable.auto.commit", false);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(topic));
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public boolean complete() {
        ConsumerRecords<K, V> records = consumer.poll(POLL_TIMEOUT_MS);
        if (records.isEmpty()) {
            return false;
        }
        for (TopicPartition topicPartition : records.partitions()) {
            List<ConsumerRecord<K, V>> partitionRecords = records.records(topicPartition);
            long latestOffset = -1;
            for (ConsumerRecord<K, V> record : partitionRecords) {
                K key = record.key();
                V value = record.value();
                emit(new AbstractMap.SimpleImmutableEntry<>(key, value));
                latestOffset = record.offset();
                if (getOutbox().isHighWater()) {
                    commitPartition(topicPartition, latestOffset);
                    return false;
                }
            }
            commitPartition(topicPartition, latestOffset);
        }
        return false;
    }

    private void commitPartition(TopicPartition topicPartition, long latestOffset) {
        if (latestOffset != -1) {
            consumer.commitSync(singletonMap(topicPartition, new OffsetAndMetadata(latestOffset + 1)));
        }
    }

    @Override
    public void close() {
        consumer.close();
    }

    private static final class MetaSupplier<K, V> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;
        private final String topicId;
        private Properties properties;

        private MetaSupplier(String topicId, Properties properties) {
            this.topicId = topicId;
            this.properties = properties;
            this.properties.put("enable.auto.commit", false);
        }

        @Override
        public void init(@Nonnull Context context) {
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new Supplier<>(topicId, properties);
        }
    }

    private static class Supplier<K, V> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String topicId;
        private final Properties properties;
        private transient List<Processor> processors;

        Supplier(String topicId, Properties properties) {
            this.properties = properties;
            this.topicId = topicId;
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return processors = range(0, count)
                    .mapToObj(i -> new ReadKafkaP<>(topicId, properties))
                    .collect(toList());
        }

        @Override
        public void complete(Throwable error) {
            processors.stream()
                      .filter(p -> p instanceof ReadKafkaP)
                      .map(p -> (ReadKafkaP) p)
                      .forEach(p -> Util.uncheckRun(p::close));
        }
    }
}
