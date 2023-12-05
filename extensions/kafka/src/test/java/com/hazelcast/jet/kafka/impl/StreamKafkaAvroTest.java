package com.hazelcast.jet.kafka.impl;

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
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Properties;

import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.kafka.impl.AbstractHazelcastAvroSerde.OPTION_KEY_AVRO_SCHEMA;
import static com.hazelcast.jet.kafka.impl.AbstractHazelcastAvroSerde.OPTION_VALUE_AVRO_SCHEMA;
import static java.lang.String.format;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
public class StreamKafkaAvroTest extends SimpleTestInClusterSupport {

    private static final int INITIAL_PARTITION_COUNT = 4;

    static final Schema KEY_SCHEMA = SchemaBuilder.record("schema.id")
            .fields()
            .optionalInt("id")
            .endRecord();

    static final Schema VALUE_SCHEMA = SchemaBuilder.record("SomeSchema").fields()
            .optionalInt("id")
            .optionalString("name")
            .optionalInt("value")
            .endRecord();
    private static KafkaTestSupport kafkaTestSupport;

    private String TOPIC_NAME = "topic";

    @BeforeClass
    public static void beforeClass() throws IOException {
        kafkaTestSupport = KafkaTestSupport.create();
        kafkaTestSupport.createKafkaCluster();
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


    public void read() {
        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.kafka(createProperties(KEY_SCHEMA, VALUE_SCHEMA), TOPIC_NAME))
                .withoutTimestamps()
                .writeTo(Sinks.list("output"));

        Job job = instance().getJet().newJob(p);

        assertJobStatusEventually(job, RUNNING);
        assertTrueAllTheTime(() -> assertEquals(RUNNING, job.getStatus()), 3);
    }

    @Test
    public void write() {
        var mapName = "source_map";
        var map = instance().getMap(mapName);
        range(0, 10).forEach(i -> map.put(keyData(i), valueData("llolololo", i)));
        Pipeline p = Pipeline.create();

        p.readFrom(Sources.map(mapName))
                .writeTo(KafkaSinks.kafka(createProperties(KEY_SCHEMA, VALUE_SCHEMA), TOPIC_NAME));
        instance().getJet().newJob(p).join();

        read();

        instance().getList("output").forEach(entry -> System.out.println("res: " + entry));
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

    private GenericData.Record keyData(Integer i) {
        var key = new GenericData.Record(KEY_SCHEMA);
        key.put("id", i);
        return key;
    }

    private GenericData.Record valueData(String name, Integer value) {
        var record = new GenericData.Record(VALUE_SCHEMA);
        record.put("name", name);
        record.put("value", value);
        return record;
    }
}
