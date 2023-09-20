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

package com.hazelcast.jet.kafka;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.dataconnection.DataConnectionResource;
import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;


public class KafkaDataConnectionTest {

    private static final KafkaTestSupport kafkaTestSupport = KafkaTestSupport.create();
    private KafkaDataConnection kafkaDataConnection;

    @BeforeClass
    public static void beforeClass() throws Exception {
        kafkaTestSupport.createKafkaCluster();
    }

    @AfterClass
    public static void afterClass() {
        kafkaTestSupport.shutdownKafkaCluster();
    }

    @After
    public void tearDown() {
        if (kafkaDataConnection != null) {
            kafkaDataConnection.destroy();
        }
    }

    @Test
    public void should_create_new_consumer_for_each_call() {
        kafkaDataConnection = createNonSharedKafkaDataConnection();

        try (Consumer<Object, Object> c1 = kafkaDataConnection.newConsumer();
             Consumer<Object, Object> c2 = kafkaDataConnection.newConsumer()) {
            assertThat(c1).isNotSameAs(c2);
        }
    }

    @Test
    public void newConsumer_should_fail_with_shared_data_connection() {
        kafkaDataConnection = createKafkaDataConnection(kafkaTestSupport);

        assertThatThrownBy(() -> kafkaDataConnection.newConsumer())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("KafkaConsumer is not thread-safe and can't be used"
                        + " with shared DataConnection 'kafka-data-connection'");

        assertThatThrownBy(() -> kafkaDataConnection.newConsumer(new Properties()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("KafkaConsumer is not thread-safe and can't be used"
                        + " with shared DataConnection 'kafka-data-connection'");
    }

    @Test
    public void shared_data_connection_should_return_same_producer() {
        kafkaDataConnection = createKafkaDataConnection(kafkaTestSupport);

        try (Producer<Object, Object> p1 = kafkaDataConnection.getProducer(null);
             Producer<Object, Object> p2 = kafkaDataConnection.getProducer(null)) {
            assertThat(p1).isSameAs(p2);
        }
    }

    @Test
    public void non_shared_data_connection_should_return_new_producer() {
        kafkaDataConnection = createNonSharedKafkaDataConnection();

        try (Producer<Object, Object> p1 = kafkaDataConnection.getProducer(null);
             Producer<Object, Object> p2 = kafkaDataConnection.getProducer(null)) {
            assertThat(p1).isNotSameAs(p2);
        }
    }

    @Test
    public void list_resources_should_return_empty_list_for_no_topics() {
        kafkaDataConnection = createKafkaDataConnection(kafkaTestSupport);

        Collection<DataConnectionResource> resources = kafkaDataConnection.listResources();
        List<DataConnectionResource> withoutConfluent =
                resources.stream().filter(r -> !Arrays.toString(r.name()).contains("__confluent")).collect(toList());
        assertThat(withoutConfluent).isEmpty();
    }

    @Test
    public void list_resources_should_return_topics() throws IOException {
        // This test creates topics, using separate cluster to avoid cleanup
        KafkaTestSupport localTestSupport = KafkaTestSupport.create();
        localTestSupport.createKafkaCluster();
        kafkaDataConnection = createKafkaDataConnection(localTestSupport);
        try {
            localTestSupport.createTopic("my-topic", 2);

            Collection<DataConnectionResource> resources = kafkaDataConnection.listResources();
            List<DataConnectionResource> withoutConfluent =
                    resources.stream().filter(r -> !Arrays.toString(r.name()).contains("__confluent")).collect(toList());
            assertThat(withoutConfluent)
                    .containsExactly(new DataConnectionResource("topic", "my-topic"));
        } finally {
            kafkaDataConnection.destroy();
            localTestSupport.shutdownKafkaCluster();
        }
    }

    @Test
    public void releasing_data_connection_does_not_close_shared_producer() {
        kafkaDataConnection = createKafkaDataConnection(kafkaTestSupport);

        Producer<Object, Object> producer = kafkaDataConnection.getProducer(null);
        kafkaDataConnection.release();

        try {
            producer.partitionsFor("my-topic");
        } catch (Exception e) {
            fail("Should not throw exception", e);
        }
    }

    @Test
    public void shared_producer_should_not_be_closed_after_one_close() {
        kafkaDataConnection = createKafkaDataConnection(kafkaTestSupport);

        Producer<Object, Object> p1 = kafkaDataConnection.getProducer(null);
        Producer<Object, Object> p2 = kafkaDataConnection.getProducer(null);
        kafkaDataConnection.release();

        p1.close();

        try {
            p1.partitionsFor("my-topic");
        } catch (Exception e) {
            fail("Should not throw exception", e);
        }
    }

    @Test
    public void shared_producer_should_be_closed_after_all_close() {
        kafkaDataConnection = createKafkaDataConnection(kafkaTestSupport);

        Producer<Object, Object> p1 = kafkaDataConnection.getProducer(null);
        Producer<Object, Object> p2 = kafkaDataConnection.getProducer(null);
        kafkaDataConnection.release();

        p1.close();
        p2.close();

        assertThatThrownBy(() -> p1.partitionsFor("my-topic"))
                .isInstanceOf(KafkaException.class)
                .hasMessage("Requested metadata update after close");
    }

    @Test
    public void should_list_resource_types() {
        // given
        kafkaDataConnection = createKafkaDataConnection(kafkaTestSupport);

        // when
        Collection<String> resourcedTypes = kafkaDataConnection.resourceTypes();

        //then
        assertThat(resourcedTypes)
                .map(r -> r.toLowerCase(Locale.ROOT))
                .containsExactlyInAnyOrder("topic");
    }

    @Test
    public void shared_producer_should_not_be_created_with_additional_props() {
        kafkaDataConnection = createKafkaDataConnection(kafkaTestSupport);
        Properties properties = new Properties();
        properties.put("A", "B");

        assertThatThrownBy(() -> kafkaDataConnection.getProducer(null, properties))
                .isInstanceOf(HazelcastException.class)
                .hasMessageContaining("For shared Kafka producer, please provide all serialization options");

        kafkaDataConnection.release();
    }

    @Test
    public void shared_producer_is_allowed_to_be_created_with_empty_props() {
        kafkaDataConnection = createKafkaDataConnection(kafkaTestSupport);

        Producer<Object, Object> kafkaProducer = kafkaDataConnection.getProducer(null, new Properties());
        assertThat(kafkaProducer).isNotNull();

        kafkaProducer.close();
        kafkaDataConnection.release();
    }

    private KafkaDataConnection createKafkaDataConnection(KafkaTestSupport kafkaTestSupport) {
        DataConnectionConfig config = new DataConnectionConfig("kafka-data-connection")
                .setType("Kafka")
                .setShared(true)
                .setProperties(properties(kafkaTestSupport));

        return new KafkaDataConnection(config);
    }

    private KafkaDataConnection createNonSharedKafkaDataConnection() {
        DataConnectionConfig config = new DataConnectionConfig("kafka-data-connection")
                .setType("Kafka")
                .setShared(false)
                .setProperties(properties(kafkaTestSupport));

        return new KafkaDataConnection(config);
    }

    public Properties properties(KafkaTestSupport kafkaTestSupport) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaTestSupport.getBrokerConnectionString());
        properties.setProperty("key.deserializer", IntegerDeserializer.class.getCanonicalName());
        properties.setProperty("key.serializer", IntegerSerializer.class.getCanonicalName());
        properties.setProperty("value.serializer", StringSerializer.class.getCanonicalName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());
        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }

}
