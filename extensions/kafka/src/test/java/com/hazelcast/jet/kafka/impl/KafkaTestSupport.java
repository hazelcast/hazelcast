/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kafka.impl;

import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.test.DockerTestUtil;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class KafkaTestSupport {
    protected Admin admin;
    protected KafkaProducer<Integer, String> producer;
    protected String brokerConnectionString;
    private KafkaProducer<String, String> stringStringProducer;

    public static KafkaTestSupport create() {
        if (!DockerTestUtil.dockerEnabled() || OsHelper.isArmMac()) {
            if (System.getProperties().containsKey("test.kafka.version")) {
                throw new IllegalArgumentException("'test.kafka.version' system property requires docker and x86_64 CPU");
            }
            return new EmbeddedKafkaTestSupport();
        } else {
            return new DockerizedKafkaTestSupport();
        }
    }

    public abstract void createKafkaCluster() throws IOException;

    public abstract void shutdownKafkaCluster();

    public String getBrokerConnectionString() {
        return brokerConnectionString;
    }

    public void createTopic(String topicId, int partitionCount) {
        List<NewTopic> newTopics = Collections.singletonList(new NewTopic(topicId, partitionCount, (short) 1));
        CreateTopicsResult createTopicsResult = admin.createTopics(newTopics);
        try {
            createTopicsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void setPartitionCount(String topicId, int numPartitions) {
        Map<String, NewPartitions> newPartitions = new HashMap<>();
        newPartitions.put(topicId, NewPartitions.increaseTo(numPartitions));
        admin.createPartitions(newPartitions);
    }

    public Future<RecordMetadata> produce(String topic, Integer key, String value) {
        return getProducer().send(new ProducerRecord<>(topic, key, value));
    }

    public Future<RecordMetadata> produce(String topic, String key, String value) {
        return getStringStringProducer().send(new ProducerRecord<>(topic, key, value));
    }

    Future<RecordMetadata> produce(String topic, int partition, Long timestamp, Integer key, String value) {
        return getProducer().send(new ProducerRecord<>(topic, partition, timestamp, key, value));
    }

    private KafkaProducer<Integer, String> getProducer() {
        if (producer == null) {
            Properties producerProps = new Properties();
            producerProps.setProperty("bootstrap.servers", brokerConnectionString);
            producerProps.setProperty("key.serializer", IntegerSerializer.class.getCanonicalName());
            producerProps.setProperty("value.serializer", StringSerializer.class.getCanonicalName());
            producer = new KafkaProducer<>(producerProps);
        }
        return producer;
    }

    private KafkaProducer<String, String> getStringStringProducer() {
        if (stringStringProducer == null) {
            Properties producerProps = new Properties();
            producerProps.setProperty("bootstrap.servers", brokerConnectionString);
            producerProps.setProperty("key.serializer", StringSerializer.class.getCanonicalName());
            producerProps.setProperty("value.serializer", StringSerializer.class.getCanonicalName());
            stringStringProducer = new KafkaProducer<>(producerProps);
        }
        return stringStringProducer;
    }

    public void resetProducer() {
        this.producer = null;
    }

    public KafkaConsumer<Integer, String> createConsumer(String... topicIds) {
        return createConsumer(IntegerDeserializer.class, StringDeserializer.class, emptyMap(), topicIds);
    }

    public <K, V> KafkaConsumer<K, V> createConsumer(
            Class<? extends Deserializer<K>> keyDeserializerClass,
            Class<? extends Deserializer<V>> valueDeserializerClass,
            Map<String, String> properties,
            String... topicIds
    ) {
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", brokerConnectionString);
        consumerProps.setProperty("group.id", randomString());
        consumerProps.setProperty("client.id", "consumer0");
        consumerProps.setProperty("key.deserializer", keyDeserializerClass.getCanonicalName());
        consumerProps.setProperty("value.deserializer", valueDeserializerClass.getCanonicalName());
        consumerProps.setProperty("isolation.level", "read_committed");
        // to make sure the consumer starts from the beginning of the topic
        consumerProps.setProperty("auto.offset.reset", "earliest");
        consumerProps.putAll(properties);
        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(topicIds));
        return consumer;
    }

    public void assertTopicContentsEventually(
            String topic,
            Map<Integer, String> expected,
            boolean assertPartitionEqualsKey
    ) {
        try (KafkaConsumer<Integer, String> consumer = createConsumer(topic)) {
            long timeLimit = System.nanoTime() + SECONDS.toNanos(10);
            for (int totalRecords = 0; totalRecords < expected.size() && System.nanoTime() < timeLimit; ) {
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Integer, String> record : records) {
                    assertEquals("key=" + record.key(), expected.get(record.key()), record.value());
                    if (assertPartitionEqualsKey) {
                        assertEquals(record.key().intValue(), record.partition());
                    }
                    totalRecords++;
                }
            }
        }
    }

    public <K, V> void assertTopicContentsEventually(
            String topic,
            Map<K, V> expected,
            Class<? extends Deserializer<K>> keyDeserializerClass,
            Class<? extends Deserializer<V>> valueDeserializerClass
    ) {
        assertTopicContentsEventually(topic, expected, keyDeserializerClass, valueDeserializerClass, emptyMap());
    }

    public <K, V> void assertTopicContentsEventually(
            String topic,
            Map<K, V> expected,
            Class<? extends Deserializer<K>> keyDeserializerClass,
            Class<? extends Deserializer<V>> valueDeserializerClass,
            Map<String, String> consumerProperties
    ) {
        try (KafkaConsumer<K, V> consumer = createConsumer(
                keyDeserializerClass,
                valueDeserializerClass,
                consumerProperties,
                topic
        )) {
            long timeLimit = System.nanoTime() + SECONDS.toNanos(10);
            Set<K> seenKeys = new HashSet<>();
            for (int totalRecords = 0; totalRecords < expected.size() && System.nanoTime() < timeLimit; ) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<K, V> record : records) {
                    assertTrue("key=" + record.key() + " already seen", seenKeys.add(record.key()));
                    V expectedValue = expected.get(record.key());
                    assertNotNull("key=" + record.key() + " received, but not expected", expectedValue);
                    assertEquals("key=" + record.key(), expectedValue, record.value());
                    totalRecords++;
                }
            }
        }
    }
}
