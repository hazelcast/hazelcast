/*
 * Copyright 2026 Hazelcast Inc.
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
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

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

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.jet.kafka.impl.DockerizedKafkaTestSupport.getKafkaVersion;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static com.hazelcast.test.DockerTestUtil.dockerEnabled;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("resource")
public abstract class KafkaTestSupport {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestSupport.class);
    static final long KAFKA_MAX_BLOCK_MS = MINUTES.toMillis(2);
    private final Map<String, KafkaProducer<Object, Object>> producers = new HashMap<>();
    private final Map<String, Map<String, String>> producerProperties = new HashMap<>();

    /**
     * Network used by Kafka / Redpanda and Schema Registry containers, if one of those is selected.
     */
    protected Network network;

    private String brokerConnectionString;
    private Admin admin;
    private GenericContainer<?> schemaRegisterContainer;
    protected SchemaRegistryClient schemaRegistryClient;

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
        createKafkaCluster(emptyMap());
    }

    public void createKafkaCluster(Map<String, String> properties) throws IOException {
        if (network == null && dockerEnabled()) {
            network = Network.newNetwork();
        }
        brokerConnectionString = createKafkaCluster0(properties);
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerConnectionString);
        admin = Admin.create(props);
    }

    /** Returns the broker connection string. */
    protected abstract String createKafkaCluster0(Map<String, String> properties) throws IOException;


    public void waitForKafkaReady() {
        try {
            waitForCondition(() -> {
                try {
                    // Send a real request to the cluster to check if it is ready
                    admin.describeCluster().nodes().get();
                    // Cluster is ready
                    return true;
                } catch (Exception e) {
                    // Cluster is not ready yet
                    return false;
                }
            }, DEFAULT_MAX_WAIT_MS, "Kafka cluster not ready within " + DEFAULT_MAX_WAIT_MS + "ms.");
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed waiting for Kafka cluster to be ready.", e);
        }
    }

    public void shutdownKafkaCluster() {
        shutdownKafkaCluster0();
        brokerConnectionString = null;
        if (admin != null) {
            admin.close();
            admin = null;
        }
        producers.values().forEach(KafkaProducer::close);
        producers.clear();

        if (network != null) {
            network.close();
            network = null;
        }
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

    public void createSchemaRegistry() {
        assumeDockerEnabled();
        schemaRegisterContainer = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry")
                                                                        .withTag(getKafkaVersion()))
                                      .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix("schema-registry"))
                                      .withExposedPorts(8081)
                                      .withNetwork(network)
                                      .withNetworkAliases("schema-registry")
                                      .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                                      .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                                      // Point Schema Registry to the internal Kafka network alias
                                      .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
                                      .withEnv("SCHEMA_REGISTRY_DEBUG", "true")
                                      // Ensure the container is ready before tests start
                                      .waitingFor(Wait.forHttp("/").forStatusCode(200));
        schemaRegisterContainer.start();
        URI schemaRegistryUrl = getSchemaRegistryURI();

        schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl.toString(), 100);
    }

    public void shutdownSchemaRegistry() {
        closeResource(schemaRegisterContainer);
    }

    public URI getSchemaRegistryURI() {
        return URI.create("http://" + schemaRegisterContainer.getHost() + ":" + schemaRegisterContainer.getMappedPort(8081));
    }

    /** Registers the specified {@code schema} and returns its ID. */
    public int registerSchema(String subject, Schema schema) throws RestClientException, IOException {
        return schemaRegistryClient.register(subject, new AvroSchema(schema));
    }

    public int getLatestSchemaVersion(String subject) throws RestClientException, IOException {
        schemaRegistryClient.reset();
        SchemaMetadata metadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
        if (metadata == null) {
            throw new RuntimeException("No schema found in subject '" + subject + "'");
        }
        return metadata.getVersion();
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
                    assertEquals(expected.get(record.key()), record.value(), "key=" + record.key());
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
                    assertTrue(seenKeys.add(record.key()), "key=" + record.key() + " already seen");
                    V expectedValue = expected.get(record.key());
                    assertNotNull(expectedValue, "key=" + record.key() + " received, but not expected");
                    assertEquals(expectedValue, record.value(), "key=" + record.key());
                    totalRecords++;
                }
            }
        }
    }
}
