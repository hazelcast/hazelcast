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
import com.hazelcast.jet.Distributed.Optional;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Kafka Writer for Jet
 *
 * @param <K>                    type of keys written
 * @param <V>                    type of values written
 */
public final class KafkaWriter<K, V> extends AbstractProcessor {

    private final Function<K, byte[]> serializeKey;
    private final Function<V, byte[]> serializeValue;
    private final String topic;
    private final KafkaProducer<byte[], byte[]> producer;

    KafkaWriter(String topic, KafkaProducer producer,
                Function<K, byte[]> serializeKey, Function<V, byte[]> serializeValue) {

        this.topic = topic;
        this.producer = producer;
        this.serializeKey = serializeKey;
        this.serializeValue = serializeValue;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        Map.Entry<K, V> entry = (Map.Entry<K, V>) item;
        byte[] key = Optional.ofNullable(entry.getKey()).map(serializeKey).orElse(null);
        byte[] value = serializeValue.apply(entry.getValue());
        producer.send(new ProducerRecord<>(topic, null, key, value));
        return true;
    }

    @Override
    public boolean complete() {
        producer.flush();
        return true;
    }

    private static Properties getProperties(String zkAddress, String groupId, String brokerConnectionString) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkAddress);
        props.put("group.id", groupId);
        props.put("bootstrap.servers", brokerConnectionString);
        props.put("key.serializer", ByteArraySerializer.class.getCanonicalName());
        props.put("value.serializer", ByteArraySerializer.class.getCanonicalName());
        return props;
    }

    /**
     * Creates supplier for producing messages to kafka topics.
     *
     * @param <K>                    type of keys written
     * @param <V>                    type of values written
     * @param zkAddress              zookeeper address
     * @param groupId                kafka consumer group name
     * @param topicId                kafka topic name
     * @param brokerConnectionString kafka broker address
     * @param serializeKey           function for serializing keys
     * @param serializeValue         function for serializing values
     * @return {@link ProcessorMetaSupplier} supplier
     */
    public static <K, V> ProcessorMetaSupplier supplier(String zkAddress, String groupId, String topicId,
                                                        String brokerConnectionString, Function<K, byte[]> serializeKey,
                                                        Function<V, byte[]> serializeValue) {
        Properties properties = getProperties(zkAddress, groupId, brokerConnectionString);
        return ProcessorMetaSupplier.of(new Supplier<>(topicId, properties, serializeKey, serializeValue));
    }

    private static class Supplier<K, V> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String topicId;
        private final Properties properties;
        private final Function<K, byte[]> serializeKey;
        private final Function<V, byte[]> serializeValue;

        private transient KafkaProducer producer;

        Supplier(String topicId, Properties properties,
                 Function<K, byte[]> serializeKey, Function<V, byte[]> serializeValue) {
            this.topicId = topicId;
            this.properties = properties;
            this.serializeKey = serializeKey;
            this.serializeValue = serializeValue;
        }

        @Override
        public void init(@Nonnull Context context) {
            producer = new KafkaProducer<>(properties);
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return Stream.generate(() -> new KafkaWriter<>(topicId, producer, serializeKey, serializeValue))
                         .limit(count)
                         .collect(toList());
        }

        @Override public void complete(Throwable error) {
            producer.close();
        }
    }
}
