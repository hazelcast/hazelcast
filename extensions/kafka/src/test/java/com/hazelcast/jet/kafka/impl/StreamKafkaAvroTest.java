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

import com.google.common.collect.ImmutableMap;
import com.hazelcast.collection.IList;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.kafka.HazelcastKafkaAvroDeserializer;
import com.hazelcast.jet.kafka.HazelcastKafkaAvroSerializer;
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.kafka.impl.AbstractHazelcastAvroSerde.OPTION_VALUE_AVRO_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

@Category({QuickTest.class, ParallelJVMTest.class})
public class StreamKafkaAvroTest extends SimpleTestInClusterSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;

    private static final Schema VALUE_SCHEMA = SchemaBuilder.record("schema.value").fields()
            .optionalString("value")
            .endRecord();

    private static final Map<String, String> AVRO_SCHEMA_PROPERTIES = ImmutableMap.of(
            OPTION_VALUE_AVRO_SCHEMA, VALUE_SCHEMA.toString()
    );

    private static KafkaTestSupport kafkaTestSupport;

    private String TOPIC_NAME;

    @BeforeClass
    public static void beforeClass() throws IOException {
        kafkaTestSupport = KafkaTestSupport.create();
        kafkaTestSupport.createKafkaCluster();
        initialize(2, null);
    }

    @Before
    public void before() {
        TOPIC_NAME = randomString();
        kafkaTestSupport.setProducerProperties(
                TOPIC_NAME,
                AVRO_SCHEMA_PROPERTIES
        );
        kafkaTestSupport.createTopic(TOPIC_NAME, INITIAL_PARTITION_COUNT);
    }

    @AfterClass
    public static void afterClass() {
        kafkaTestSupport.shutdownKafkaCluster();
        kafkaTestSupport = null;
    }

    @Test
    public void readGenericRecord() {
        IList<Object> sinkList = instance().getList("output");
        Pipeline p = Pipeline.create();
        p.readFrom(
                        KafkaSources.kafka(
                                createProperties(HazelcastKafkaAvroSerializer.class, HazelcastKafkaAvroDeserializer.class),
                                TOPIC_NAME
                        )
                )
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkList));

        instance().getJet().newJob(p);

        produceSafe(TOPIC_NAME, 1, toGenericRecord("read_value", VALUE_SCHEMA));

        assertTrueEventually(() -> assertThat(sinkList).contains(entry(1, toGenericRecord("read_value", VALUE_SCHEMA))));
    }

    @Test
    public void writeGenericRecord() {
        var list = instance().getList("input");
        list.add(1);

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list(list))
                .map(v -> entry(v, toGenericRecord("write_value", VALUE_SCHEMA)))
                .writeTo(
                        KafkaSinks.kafka(
                                createProperties(
                                        HazelcastKafkaAvroSerializer.class,
                                        HazelcastKafkaAvroDeserializer.class
                                ),
                                TOPIC_NAME
                        )
                );
        instance().getJet().newJob(p).join();

        kafkaTestSupport.assertTopicContentsEventually(
                TOPIC_NAME,
                Map.of(1, toGenericRecord("write_value", VALUE_SCHEMA)),
                IntegerDeserializer.class,
                HazelcastKafkaAvroDeserializer.class,
                AVRO_SCHEMA_PROPERTIES
        );
    }


    @Test
    @Ignore
    public void readSpecificRecord() {
        // todo new ser\deser for specific record
        IList<Object> sinkList = instance().getList("actual");
        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.kafka(createProperties(null, null), TOPIC_NAME))
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkList));

        instance().getJet().newJob(p);

        produceSafe(TOPIC_NAME, 1, toTestSpecificRecord("value"));

        assertTrueEventually(() -> assertThat(sinkList).contains(entry(1, toTestSpecificRecord("value"))));

    }

    @Test
    @Ignore
    public void writeSpecificRecord() {
        // todo new ser\deser for specific record
        var list = instance().getList("input");
        list.add(1);

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list(list))
                .map(v -> Map.entry(1, toTestSpecificRecord("specific_value")))
                .writeTo(KafkaSinks.kafka(createProperties(null, null), TOPIC_NAME));

        instance().getJet().newJob(p).join();

        kafkaTestSupport.assertTopicContentsEventually(
                TOPIC_NAME,
                Map.of(1, toTestSpecificRecord("specific_value")),
                IntegerDeserializer.class,
                null, // todo new deserializer
                AVRO_SCHEMA_PROPERTIES
        );
    }

    @Test
    @Ignore
    public void readReflection() {
        // todo new ser\deser for reflection
        IList<Object> sinkList = instance().getList("actual");
        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.kafka(createProperties(null, null), TOPIC_NAME))
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkList));

        instance().getJet().newJob(p);

        produceSafe(TOPIC_NAME, 1, new TestRecord("value"));

        assertTrueEventually(() -> assertThat(sinkList).contains(entry(1, new TestRecord("value"))));
    }

    @Test
    @Ignore
    public void writeReflection() {
        // todo new ser\deser for reflection
        var list = instance().getList("input");
        list.add(1);

        Pipeline p = Pipeline.create();
        p.readFrom(Sources.list(list))
                .map(v -> Map.entry(1, new TestRecord("value")))
                // todo add class name somewhere. may be in properties?
                .writeTo(KafkaSinks.kafka(createProperties(null, null), TOPIC_NAME));

        instance().getJet().newJob(p).join();

        kafkaTestSupport.assertTopicContentsEventually(
                TOPIC_NAME,
                Map.of(1, new TestRecord("value")),
                IntegerDeserializer.class,
                null, // todo new deserializer
                AVRO_SCHEMA_PROPERTIES
        );
    }


    private void produceSafe(String topic, Object key, Object value) {
        try {
            kafkaTestSupport.produce(topic, key, value).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Properties createProperties(Class<?> serializer, Class<?> deserializer) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaTestSupport.getBrokerConnectionString());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getCanonicalName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer.getCanonicalName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getCanonicalName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer.getCanonicalName());
        properties.setProperty("auto.offset.reset", "earliest");
        properties.putAll(AVRO_SCHEMA_PROPERTIES);

        return properties;
    }

    private static GenericData.Record toGenericRecord(Object value, Schema schema) {
        var record = new GenericData.Record(schema);
        record.put(0, value);
        return record;
    }

    private static SpecificRecord toTestSpecificRecord(String value) {
        return new TestSpecificRecord(value);
    }


    public static class TestSpecificRecord implements SpecificRecord {

        private final Schema schema = VALUE_SCHEMA;

        private String value;

        public TestSpecificRecord(String value) {
            this.value = value;
        }

        @Override
        public void put(int i, Object v) {
            this.value = (String) v;
        }

        @Override
        public Object get(int i) {
            return value;
        }

        @Override
        public Schema getSchema() {
            return schema;
        }
    }


    public static class TestRecord {

        public String value;

        public TestRecord(String value) {
            this.value = value;
        }

    }
}
