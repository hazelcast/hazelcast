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

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.datalink.DataLinkResource;
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
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;


public class KafkaDataLinkTest {

    private static final KafkaTestSupport kafkaTestSupport = KafkaTestSupport.create();
    private KafkaDataLink kafkaDataLink;

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
        if (kafkaDataLink != null) {
            kafkaDataLink.destroy();
        }
    }

    @Test
    public void should_create_new_consumer_for_each_call() {
        kafkaDataLink = createKafkaDataLink(kafkaTestSupport);

        try (Consumer<Object, Object> c1 = kafkaDataLink.newConsumer();
             Consumer<Object, Object> c2 = kafkaDataLink.newConsumer()) {
            assertThat(c1).isNotSameAs(c2);
        }
    }

    @Test
    public void shared_data_link_should_return_same_producer() {
        kafkaDataLink = createKafkaDataLink(kafkaTestSupport);

        try (Producer<Object, Object> p1 = kafkaDataLink.getProducer(null);
             Producer<Object, Object> p2 = kafkaDataLink.getProducer(null)) {
            assertThat(p1).isSameAs(p2);
        }
    }

    @Test
    public void non_shared_data_link_should_return_new_producer() {
        kafkaDataLink = createNonSharedKafkaDataLink();

        try (Producer<Object, Object> p1 = kafkaDataLink.getProducer(null);
             Producer<Object, Object> p2 = kafkaDataLink.getProducer(null)) {
            assertThat(p1).isNotSameAs(p2);
        }
    }

    @Test
    public void list_resources_should_return_empty_list_for_no_topics() {
        kafkaDataLink = createKafkaDataLink(kafkaTestSupport);

        Collection<DataLinkResource> resources = kafkaDataLink.listResources();
        List<DataLinkResource> withoutConfluent =
                resources.stream().filter(r -> !r.name().contains("__confluent")).collect(toList());
        assertThat(withoutConfluent).isEmpty();
    }

    @Test
    public void list_resources_should_return_topics() throws IOException {
        // This test creates topics, using separate cluster to avoid cleanup
        KafkaTestSupport localTestSupport = KafkaTestSupport.create();
        localTestSupport.createKafkaCluster();
        kafkaDataLink = createKafkaDataLink(localTestSupport);
        try {
            localTestSupport.createTopic("my-topic", 2);

            Collection<DataLinkResource> resources = kafkaDataLink.listResources();
            List<DataLinkResource> withoutConfluent =
                    resources.stream().filter(r -> !r.name().contains("__confluent")).collect(toList());
            assertThat(withoutConfluent)
                    .containsExactly(new DataLinkResource("topic", "my-topic"));
        } finally {
            kafkaDataLink.destroy();
            localTestSupport.shutdownKafkaCluster();
        }
    }

    @Test
    public void releasing_data_link_does_not_close_shared_producer() {
        kafkaDataLink = createKafkaDataLink(kafkaTestSupport);

        Producer<Object, Object> producer = kafkaDataLink.getProducer(null);
        kafkaDataLink.release();

        try {
            producer.partitionsFor("my-topic");
        } catch (Exception e) {
            fail("Should not throw exception", e);
        }
    }

    @Test
    public void shared_producer_should_not_be_closed_after_one_close() {
        kafkaDataLink = createKafkaDataLink(kafkaTestSupport);

        Producer<Object, Object> p1 = kafkaDataLink.getProducer(null);
        Producer<Object, Object> p2 = kafkaDataLink.getProducer(null);
        kafkaDataLink.release();

        p1.close();

        try {
            p1.partitionsFor("my-topic");
        } catch (Exception e) {
            fail("Should not throw exception", e);
        }
    }

    @Test
    public void shared_producer_should_be_closed_after_all_close() {
        kafkaDataLink = createKafkaDataLink(kafkaTestSupport);

        Producer<Object, Object> p1 = kafkaDataLink.getProducer(null);
        Producer<Object, Object> p2 = kafkaDataLink.getProducer(null);
        kafkaDataLink.release();

        p1.close();
        p2.close();

        assertThatThrownBy(() -> p1.partitionsFor("my-topic"))
                .isInstanceOf(KafkaException.class)
                .hasMessage("Requested metadata update after close");
    }

    private KafkaDataLink createKafkaDataLink(KafkaTestSupport kafkaTestSupport) {
        DataLinkConfig config = new DataLinkConfig("kafka-data-link")
                .setType("Kafka")
                .setShared(true)
                .setProperties(properties(kafkaTestSupport));

        return new KafkaDataLink(config);
    }

    private KafkaDataLink createNonSharedKafkaDataLink() {
        DataLinkConfig config = new DataLinkConfig("kafka-data-link")
                .setType("Kafka")
                .setShared(false)
                .setProperties(properties(kafkaTestSupport));

        return new KafkaDataLink(config);
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
