/*
 * Copyright 2023 Hazelcast Inc.
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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.kafka.HazelcastKafkaAvroSerializer;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.eclipse.jetty.server.Server;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.test.DockerTestUtil.dockerEnabled;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class KafkaTestSupport {
    static final long KAFKA_MAX_BLOCK_MS = MINUTES.toMillis(2);
    private final Map<String, KafkaProducer<Object, Object>> producers = new HashMap<>();
    private final Map<String, Map<String, String>> producerProperties = new HashMap<>();

    private String brokerConnectionString;
    private Admin admin;
    private Server schemaRegistryServer;
    private KafkaSchemaRegistry schemaRegistry;

    public static KafkaTestSupport create() {
        if (!dockerEnabled()) {
            assertPropertyNotSet("test.kafka.version");
            assertPropertyNotSet("test.redpanda.version");
            assertPropertyNotSet("test.kafka.use.redpanda");
            return new EmbeddedKafkaTestSupport();
        } else {
            if (System.getProperties().containsKey("test.kafka.use.redpanda")) {
                return new DockerizedRedPandaTestSupport();
            } else {
                return new DockerizedKafkaTestSupport();
            }
        }
    }

    private static void assertPropertyNotSet(String key) {
        if (System.getProperties().containsKey(key)) {
            throw new IllegalArgumentException("'" + key + "' system property requires docker enabled");
        }
    }

    public void createKafkaCluster() throws IOException {
        brokerConnectionString = createKafkaCluster0();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerConnectionString);
        admin = Admin.create(props);
    }

    /** Returns the broker connection string. */
    protected abstract String createKafkaCluster0() throws IOException;

    public void shutdownKafkaCluster() {
        shutdownKafkaCluster0();
        brokerConnectionString = null;
        if (admin != null) {
            admin.close();
            admin = null;
        }
        producers.values().forEach(KafkaProducer::close);
        producers.clear();
    }

    protected abstract void shutdownKafkaCluster0();

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

    public void deleteTopic(String topicId) {
        try {
            admin.deleteTopics(List.of(topicId)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void setPartitionCount(String topicId, int numPartitions) {
        Map<String, NewPartitions> newPartitions = new HashMap<>();
        newPartitions.put(topicId, NewPartitions.increaseTo(numPartitions));
        admin.createPartitions(newPartitions);
        producers.remove(topicId); // existing producer will not see new partitions
    }

    public void setProducerProperties(String topicId, Map<String, String> properties) {
        producerProperties.put(topicId, properties);
        producers.remove(topicId); // existing producer will not use new properties
    }

    public Future<RecordMetadata> produce(String topic, Object key, Object value) {
        return producers.computeIfAbsent(topic, t -> createProducer(topic, key, value))
                .send(new ProducerRecord<>(topic, key, value));
    }

    public void produceSync(String topic, Object key, Object value) {
        try {
            produce(topic, key, value).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public Future<RecordMetadata> produce(String topic, int partition, Long timestamp, Object key, Object value) {
        return producers.computeIfAbsent(topic, t -> createProducer(topic, key, value))
                .send(new ProducerRecord<>(topic, partition, timestamp, key, value));
    }

    private KafkaProducer<Object, Object> createProducer(String topic, Object key, Object value) {
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", brokerConnectionString);
        producerProps.setProperty("key.serializer", resolveSerializer(topic, key));
        producerProps.setProperty("value.serializer", resolveSerializer(topic, value));
        producerProps.setProperty("max.block.ms", String.valueOf(KAFKA_MAX_BLOCK_MS));
        Optional.ofNullable(producerProperties.get(topic)).ifPresent(producerProps::putAll);
        return new KafkaProducer<>(producerProps);
    }

    /**
     * @see org.apache.kafka.common.serialization.Serdes#serdeFrom(Class)
     * @see com.hazelcast.jet.sql.impl.connector.kafka.PropertiesResolver#resolveSerializer(String)
     */
    @SuppressWarnings("ReturnCount")
    private String resolveSerializer(String topic, Object object) {
        if (object instanceof String) {
            return "org.apache.kafka.common.serialization.StringSerializer";
        } else if (object instanceof Short) {
            return "org.apache.kafka.common.serialization.ShortSerializer";
        } else if (object instanceof Integer) {
            return "org.apache.kafka.common.serialization.IntegerSerializer";
        } else if (object instanceof Long) {
            return "org.apache.kafka.common.serialization.LongSerializer";
        } else if (object instanceof Float) {
            return "org.apache.kafka.common.serialization.FloatSerializer";
        } else if (object instanceof Double) {
            return "org.apache.kafka.common.serialization.DoubleSerializer";
        } else if (object instanceof byte[]) {
            return "org.apache.kafka.common.serialization.ByteArraySerializer";
        } else if (object instanceof ByteBuffer) {
            return "org.apache.kafka.common.serialization.ByteBufferSerializer";
        } else if (object instanceof Bytes) {
            return "org.apache.kafka.common.serialization.BytesSerializer";
        } else if (object instanceof UUID) {
            return "org.apache.kafka.common.serialization.UUIDSerializer";
        } else if (object instanceof GenericRecord) {
            Map<String, String> producerProps = producerProperties.get(topic);
            return producerProps != null && producerProps.containsKey("schema.registry.url")
                    ? "io.confluent.kafka.serializers.KafkaAvroSerializer"
                    : HazelcastKafkaAvroSerializer.class.getCanonicalName();
        } else if (object instanceof HazelcastJsonValue) {
            return HazelcastJsonValueSerializer.class.getCanonicalName();
        } else {
            throw new IllegalArgumentException("Unknown class: " + object.getClass().getCanonicalName()
                    + ". Supported types are: String, Short, Integer, Long, Float, Double, "
                    + "ByteArray, ByteBuffer, Bytes, UUID, GenericRecord, HazelcastJsonValue");
        }
    }

    public KafkaConsumer<Integer, String> createConsumer(String... topicIds) {
        return createConsumer(IntegerDeserializer.class, StringDeserializer.class, emptyMap(), topicIds);
    }

    public <K, V> KafkaConsumer<K, V> createConsumer(
            Class<? extends Deserializer<? super K>> keyDeserializerClass,
            Class<? extends Deserializer<? super V>> valueDeserializerClass,
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
        consumer.subscribe(List.of(topicIds));
        return consumer;
    }

    public void createSchemaRegistry(SchemaRegistryConfig config) throws Exception {
        SchemaRegistryRestApplication schemaRegistryApplication = new SchemaRegistryRestApplication(config);
        schemaRegistryServer = schemaRegistryApplication.createServer();
        schemaRegistryServer.start();
        schemaRegistry = schemaRegistryApplication.schemaRegistry();
    }

    public void shutdownSchemaRegistry() throws Exception {
        if (schemaRegistryServer != null) {
            schemaRegistryServer.stop();
        }
    }

    public URI getSchemaRegistryURI() {
        return schemaRegistryServer.getURI();
    }

    public int registerSchema(String subject, Schema schema) throws SchemaRegistryException {
        return schemaRegistry.register(subject, new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
                subject, -1, -1, AvroSchema.TYPE, emptyList(), schema.toString()));
    }

    public int getLatestSchemaVersion(String subject) throws SchemaRegistryException {
        return Optional.ofNullable(schemaRegistry.getLatestVersion(subject)).map(s -> s.getVersion())
                .orElseThrow(() -> new SchemaRegistryException("No schema found in subject '" + subject + "'"));
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
            Class<? extends Deserializer<? super K>> keyDeserializerClass,
            Class<? extends Deserializer<? super V>> valueDeserializerClass
    ) {
        assertTopicContentsEventually(topic, expected, keyDeserializerClass, valueDeserializerClass, emptyMap());
    }

    public <K, V> void assertTopicContentsEventually(
            String topic,
            Map<K, V> expected,
            Class<? extends Deserializer<? super K>> keyDeserializerClass,
            Class<? extends Deserializer<? super V>> valueDeserializerClass,
            Map<String, String> consumerProperties
    ) {
        try (KafkaConsumer<K, V> consumer = createConsumer(
                keyDeserializerClass,
                valueDeserializerClass,
                consumerProperties,
                topic
        )) {
            long timeLimit = System.nanoTime() + SECONDS.toNanos(KAFKA_MAX_BLOCK_MS);
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
