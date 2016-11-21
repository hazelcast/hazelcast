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

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.jet2.Outbox;
import com.hazelcast.jet2.Processor;
import com.hazelcast.jet2.ProcessorMetaSupplier;
import com.hazelcast.jet2.ProcessorSupplier;
import com.hazelcast.jet2.impl.AbstractProcessor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import static java.util.stream.Collectors.toList;

/**
 * Kafka Producer for Jet
 */
public class KafkaWriter extends AbstractProcessor {

    private static final ILogger LOGGER = Logger.getLogger(KafkaWriter.class);
    private final SerializationService serializationService;
    private final Properties properties;
    private final String topic;
    private KafkaProducer<byte[], byte[]> producer;

    protected KafkaWriter(SerializationService serializationService, String topic, Properties properties) {
        this.topic = topic;
        this.serializationService = serializationService;
        this.properties = properties;
    }

    @Override
    public void init(@Nonnull Outbox outbox) {
        super.init(outbox);
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
    public boolean isBlocking() {
        return true;
    }

    @Override
    protected boolean process(int ordinal, Object item) {
        Map.Entry entry = (Map.Entry) item;
        Data key = serializationService.toData(entry.getKey());
        Data value = serializationService.toData(entry.getValue());
        producer.send(new ProducerRecord<>(topic, null, key.toByteArray(), value.toByteArray()));
        return true;
    }

    @Override
    public boolean complete() {
        producer.flush();
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
        return new MetaSupplier(topicId, getProperties(zkAddress, groupId, brokerConnectionString));
    }

    private static class MetaSupplier implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;
        private final String topicId;
        private final Properties properties;

        MetaSupplier(String topicId, Properties properties) {
            this.topicId = topicId;
            this.properties = properties;
        }

        @Override
        public ProcessorSupplier get(Address address) {
            return new Supplier(topicId, properties);
        }
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

        public void init(Context context) {
            this.context = context;
        }

        @Override
        public List<Processor> get(int count) {
            HazelcastInstanceImpl hazelcastInstance = (HazelcastInstanceImpl) context.getHazelcastInstance();
            SerializationService serializationService = hazelcastInstance.node.nodeEngine.getSerializationService();
            return Stream.generate(() -> new KafkaWriter(serializationService, topicId, properties))
                    .limit(count)
                    .collect(toList());
        }
    }
}
