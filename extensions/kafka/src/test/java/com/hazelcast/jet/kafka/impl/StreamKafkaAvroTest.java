package com.hazelcast.jet.kafka.impl;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.collection.IList;
import com.hazelcast.jet.Job;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.kafka.impl.AbstractHazelcastAvroSerde.OPTION_KEY_AVRO_SCHEMA;
import static com.hazelcast.jet.kafka.impl.AbstractHazelcastAvroSerde.OPTION_VALUE_AVRO_SCHEMA;
import static java.lang.String.format;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

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

    private static String TOPIC_NAME = "topic";

    private static final Map<String, String> AVRO_SCHEMA_PROPERTIES = ImmutableMap.of(
            OPTION_KEY_AVRO_SCHEMA, KEY_SCHEMA.toString(),
            OPTION_VALUE_AVRO_SCHEMA, VALUE_SCHEMA.toString()
    );

    @BeforeClass
    public static void beforeClass() throws IOException {
        kafkaTestSupport = KafkaTestSupport.create();
        kafkaTestSupport.createKafkaCluster();
        kafkaTestSupport.setProducerProperties(
                TOPIC_NAME,
                AVRO_SCHEMA_PROPERTIES
        );
        initialize(2, null);
    }

    @Before
    public void before() {
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
        p.readFrom(KafkaSources.kafka(createProperties(KEY_SCHEMA, VALUE_SCHEMA), TOPIC_NAME))
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkList));

        instance().getJet().newJob(p);

        produceSafe(TOPIC_NAME, keyData(1), valueData("value"));

        assertTrueEventually(() -> assertThat(sinkList).contains(entry(keyData(1), valueData("value"))));
    }

    @Test
    public void writeGenericRecord() {
        var mapName = "source_map";
        var map = instance().getMap(mapName);
        map.put(keyData(1), valueData("value"));
        Pipeline p = Pipeline.create();

        p.readFrom(Sources.map(mapName))
                .writeTo(KafkaSinks.kafka(createProperties(KEY_SCHEMA, VALUE_SCHEMA), TOPIC_NAME));
        instance().getJet().newJob(p).join();

        kafkaTestSupport.assertTopicContentsEventually(
                TOPIC_NAME,
                Map.of(keyData(1), valueData("value")),
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

    private Properties createProperties(Schema keySchema, Schema valueSchema) {

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", kafkaTestSupport.getBrokerConnectionString());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, HazelcastKafkaAvroDeserializer.class.getCanonicalName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HazelcastKafkaAvroDeserializer.class.getCanonicalName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, HazelcastKafkaAvroSerializer.class.getCanonicalName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, HazelcastKafkaAvroSerializer.class.getCanonicalName());
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty(OPTION_KEY_AVRO_SCHEMA, keySchema.toString());
        properties.setProperty(OPTION_VALUE_AVRO_SCHEMA, valueSchema.toString());

        return properties;
    }

    private Properties createStringProperties() {
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", kafkaTestSupport.getBrokerConnectionString());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }

    private GenericData.Record keyData(Integer i) {
        var key = new GenericData.Record(KEY_SCHEMA);
        key.put("key", i);
        return key;
    }

    private GenericData.Record valueData(String value) {
        var record = new GenericData.Record(VALUE_SCHEMA);
        record.put("value", value);
        return record;
    }

    private GenericData.Record valueDataAnotherSchema(String name, Integer value) {
        var record = new GenericData.Record(RECORD_SCHEMA);
        record.put("another_id", null);
        record.put("another_name", null);
        record.put("common_value", value);
        return record;
    }

    static final Schema RECORD_SCHEMA = SchemaBuilder.record("schema.value2").fields()
            .optionalInt("another_id")
            .optionalString("another_name")
            .optionalInt("common_value")
            .endRecord();
}
