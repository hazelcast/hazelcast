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
import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

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
    private final String[] topicIds;
    private KafkaConsumer<K, V> consumer;

    private ReadKafkaP(String[] topicIds, Properties properties) {
        this.topicIds = topicIds;
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
     * @param topicIds   kafka topic names
     * @param properties consumer properties which should contain consumer group name,
     *                   broker address and key/value deserializers
     */
    public static <K, V> ProcessorMetaSupplier readKafka(Properties properties, String... topicIds) {
        if (!properties.containsKey("group.id")) {
            throw new IllegalArgumentException("Properties should contain `group.id`");
        }
        return new MetaSupplier<>(topicIds, properties);
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        properties.put("enable.auto.commit", false);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topicIds));
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
        Iterator<TopicPartition> iterator = records.partitions().iterator();
        while (iterator.hasNext()) {
            TopicPartition topicPartition = iterator.next();
            List<ConsumerRecord<K, V>> partitionRecords = records.records(topicPartition);
            for (ConsumerRecord<K, V> record : partitionRecords) {
                K key = record.key();
                V value = record.value();
                emit(new AbstractMap.SimpleImmutableEntry<>(key, value));
                if (getOutbox().isHighWater()) {
                    consumer.seek(topicPartition, record.offset() + 1);
                    seekToTheBeginning(records, iterator);
                    consumer.commitSync();
                    return false;
                }
            }
        }
        consumer.commitSync();
        return false;
    }

    private void seekToTheBeginning(ConsumerRecords<K, V> records, Iterator<TopicPartition> iterator) {
        while (iterator.hasNext()) {
            TopicPartition topicPartition = iterator.next();
            List<ConsumerRecord<K, V>> partitionRecords = records.records(topicPartition);
            if (!partitionRecords.isEmpty()) {
                long offset = partitionRecords.get(0).offset();
                consumer.seek(topicPartition, offset);
            }
        }
    }

    @Override
    public void close() {
        consumer.close();
    }

    private static final class MetaSupplier<K, V> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;
        private final String[] topicIds;
        private Properties properties;

        private MetaSupplier(String[] topicIds, Properties properties) {
            this.topicIds = topicIds;
            this.properties = properties;
            this.properties.put("enable.auto.commit", false);
        }

        @Override
        public void init(@Nonnull Context context) {
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new Supplier<>(topicIds, properties);
        }
    }

    private static class Supplier<K, V> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String[] topicIds;
        private final Properties properties;
        private transient List<Processor> processors;

        Supplier(String[] topicIds, Properties properties) {
            this.properties = properties;
            this.topicIds = topicIds;
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return processors = range(0, count)
                    .mapToObj(i -> new ReadKafkaP<>(topicIds, properties))
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
