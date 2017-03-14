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
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Kafka writer for Jet. Receives records of type {@code Map.Entry} and publishes
 * them to a Kafka topic.
 *
 * @param <K> type of keys written
 * @param <V> type of values written
 */
public final class WriteKafkaP<K, V> extends AbstractProcessor {

    private final String topic;
    private final KafkaProducer<K, V> producer;

    WriteKafkaP(String topic, KafkaProducer<K, V> producer) {
        this.topic = topic;
        this.producer = producer;
    }

    /**
     * Returns a meta-supplier of processors that publish messages to a Kafka topic.
     * It receives items of type {@code Map.Entry<K,V>} and publishes them to Kafka.
     *
     * A single {@code KafkaProducer} is created per node using the supplied properties file.
     * The producer instance is shared across all {@code Processor} instances on that node.
     *
     * @param <K>        type of keys written
     * @param <V>        type of values written
     * @param topic      kafka topic name to publish to
     * @param properties producer properties which should contain broker address and key/value serializers
     */
    public static <K, V> ProcessorMetaSupplier writeKafka(String topic, Properties properties) {
        return ProcessorMetaSupplier.of(new Supplier<K, V>(topic, properties));
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        Map.Entry<K, V> entry = (Map.Entry<K, V>) item;
        producer.send(new ProducerRecord<>(topic, entry.getKey(), entry.getValue()));
        return true;
    }

    @Override
    public boolean complete() {
        producer.flush();
        return true;
    }

    private static class Supplier<K, V> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String topicId;
        private final Properties properties;

        private transient KafkaProducer<K, V> producer;

        Supplier(String topicId, Properties properties) {
            this.topicId = topicId;
            this.properties = properties;
        }

        @Override
        public void init(@Nonnull Context context) {
            producer = new KafkaProducer<>(properties);
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return Stream.generate(() -> new WriteKafkaP<>(topicId, producer))
                         .limit(count)
                         .collect(toList());
        }

        @Override
        public void complete(Throwable error) {
            producer.close();
        }
    }
}
