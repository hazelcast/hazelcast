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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.kafka.impl.AbstractHazelcastAvroSerde.OPTION_KEY_AVRO_SCHEMA;
import static com.hazelcast.jet.kafka.impl.AbstractHazelcastAvroSerde.OPTION_VALUE_AVRO_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

@Category({QuickTest.class, ParallelJVMTest.class})
public class StreamKafkaAvroTest extends SimpleTestInClusterSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;

    static final Schema KEY_SCHEMA = SchemaBuilder.record("schema.key")
            .fields()
            .optionalInt("key")
            .endRecord();

    static final Schema VALUE_SCHEMA = SchemaBuilder.record("schema.value").fields()
            .optionalString("value")
            .endRecord();

    private static KafkaTestSupport kafkaTestSupport;

    private String TOPIC_NAME;

    private static final Map<String, String> AVRO_SCHEMA_PROPERTIES = ImmutableMap.of(
            OPTION_KEY_AVRO_SCHEMA, KEY_SCHEMA.toString(),
            OPTION_VALUE_AVRO_SCHEMA, VALUE_SCHEMA.toString()
    );

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
        IList<Object> sinkList = instance().getList("actual");
        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.kafka(createProperties(), TOPIC_NAME))
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkList));

        instance().getJet().newJob(p);

        produceSafe(TOPIC_NAME, genericRecord(1, KEY_SCHEMA), genericRecord("value", VALUE_SCHEMA));

        assertTrueEventually(() -> assertThat(sinkList).contains(entry(genericRecord(1, KEY_SCHEMA), genericRecord("value", VALUE_SCHEMA))));
    }

    @Test
    public void writeGenericRecord() {
        var mapName = "source_map";
        var map = instance().getMap(mapName);
        map.put(genericRecord(2, KEY_SCHEMA), genericRecord("value_2", VALUE_SCHEMA));
        Pipeline p = Pipeline.create();

        p.readFrom(Sources.map(mapName))
                .writeTo(KafkaSinks.kafka(createProperties(), TOPIC_NAME));
        instance().getJet().newJob(p).join();

        kafkaTestSupport.assertTopicContentsEventually(
                TOPIC_NAME,
                Map.of(genericRecord(2, KEY_SCHEMA), genericRecord("value_2", VALUE_SCHEMA)),
                HazelcastKafkaAvroDeserializer.class,
                HazelcastKafkaAvroDeserializer.class,
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

    private Properties createProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaTestSupport.getBrokerConnectionString());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, HazelcastKafkaAvroDeserializer.class.getCanonicalName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HazelcastKafkaAvroDeserializer.class.getCanonicalName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, HazelcastKafkaAvroSerializer.class.getCanonicalName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, HazelcastKafkaAvroSerializer.class.getCanonicalName());
        properties.setProperty("auto.offset.reset", "earliest");
        properties.putAll(AVRO_SCHEMA_PROPERTIES);

        return properties;
    }

    private GenericData.Record genericRecord(Object value, Schema schema) {
        var record = new GenericData.Record(schema);
        record.put(0, value);
        return record;
    }
}
