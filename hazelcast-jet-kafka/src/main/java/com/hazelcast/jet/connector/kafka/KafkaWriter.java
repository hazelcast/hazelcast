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

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
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
 * Kafka Producer for Jet
 */
public final class KafkaWriter extends AbstractProcessor {

    private static final ILogger LOGGER = Logger.getLogger(KafkaWriter.class);
    private final SerializationService serializationService;
    private final Properties properties;
    private final String topic;
    private KafkaProducer<byte[], byte[]> producer;

    private KafkaWriter(SerializationService serializationService, String topic, Properties properties) {
        this.topic = topic;
        this.serializationService = serializationService;
        this.properties = properties;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        producer = new KafkaProducer<>(properties);
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

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        Map.Entry entry = (Map.Entry) item;
        Data key = serializationService.toData(entry.getKey());
        Data value = serializationService.toData(entry.getValue());
        producer.send(new ProducerRecord<>(topic, null, key.toByteArray(), value.toByteArray()));
        return true;
    }

    @Override
    public boolean complete() {
        producer.flush();
        producer.close();
        return true;
    }

    /**
     * Creates supplier for producing messages to kafka topics.
     *
     * @param zkAddress              zookeeper address
     * @param groupId                kafka consumer group name
     * @param topicId                kafka topic name
     * @param brokerConnectionString kafka broker address
     * @return {@link ProcessorMetaSupplier} supplier
     */
    public static ProcessorMetaSupplier supplier(String zkAddress, String groupId, String topicId,
                                                 String brokerConnectionString) {
        Properties properties = getProperties(zkAddress, groupId, brokerConnectionString);
        return ProcessorMetaSupplier.of(new Supplier(topicId, properties));
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String topicId;
        private final Properties properties;
        private transient Context context;

        Supplier(String topicId, Properties properties) {
            this.topicId = topicId;
            this.properties = properties;
        }

        @Override
        public void init(@Nonnull Context context) {
            this.context = context;
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            HazelcastInstanceImpl hazelcastInstance = (HazelcastInstanceImpl)
                    context.jetInstance().getHazelcastInstance();
            SerializationService serializationService = hazelcastInstance.node.nodeEngine.getSerializationService();
            return Stream.generate(() -> new KafkaWriter(serializationService, topicId, properties))
                         .limit(count)
                         .collect(toList());
        }
    }
}
