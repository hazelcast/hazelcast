package com.hazelcast.jet.connector.kafka;

import com.hazelcast.jet.JetTestSupport;
import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;

import static kafka.admin.RackAwareMode.Disabled$.MODULE$;

public class KafkaTestSupport extends JetTestSupport {

    private static final String ZK_HOST = "127.0.0.1";
    private static final String BROKER_HOST = "127.0.0.1";
    private static final int BROKER_PORT = 9092;
    private static final int SESSION_TIMEOUT = 30000;
    private static final int CONNECTION_TIMEOUT = 30000;

    private EmbeddedZookeeper zkServer;
    private ZkUtils zkUtils;
    private KafkaServer kafkaServer;
    private KafkaProducer<Integer, String> producer;

    @After
    public void shutdownKafkaCluster() {
        if (kafkaServer != null) {
            kafkaServer.shutdown();
            zkUtils.close();
            zkServer.shutdown();
            if (producer != null) {
                producer.close();
            }
        }
    }

    protected final String createKafkaCluster() throws IOException {
        zkServer = new EmbeddedZookeeper();
        String zkConnect = ZK_HOST + ":" + zkServer.port();
        ZkClient zkClient = new ZkClient(zkConnect, SESSION_TIMEOUT, CONNECTION_TIMEOUT, ZKStringSerializer$.MODULE$);
        zkUtils = ZkUtils.apply(zkClient, false);

        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKER_HOST + ":" + BROKER_PORT);
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);

        return BROKER_HOST + ":" + BROKER_PORT;
    }

    protected void createTopic(String topicId, int partitions, int replicationFactor) {
        AdminUtils.createTopic(zkUtils, topicId, partitions, replicationFactor, new Properties(), MODULE$);
    }

    protected Future<RecordMetadata> produce(String topic, Integer key, String value) {
        return getProducer().send(new ProducerRecord<Integer, String>(topic, key, value));
    }

    protected KafkaProducer<Integer, String> getProducer() {
        if (producer == null) {
            Properties producerProps = new Properties();
            producerProps.setProperty("bootstrap.servers", BROKER_HOST + ":" + BROKER_PORT);
            producerProps.setProperty("key.serializer", IntegerSerializer.class.getCanonicalName());
            producerProps.setProperty("value.serializer", StringSerializer.class.getCanonicalName());
            producer = new KafkaProducer<>(producerProps);
        }
        return producer;
    }

    protected KafkaConsumer<String, String> createConsumer(String brokerConnectionString, String topic) {
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", brokerConnectionString);
        consumerProps.setProperty("group.id", "group0");
        consumerProps.setProperty("client.id", "consumer0");
        consumerProps.setProperty("key.deserializer", StringDeserializer.class.getCanonicalName());
        consumerProps.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());
        consumerProps.put("auto.offset.reset", "earliest");  // to make sure the consumer starts from the beginning of the topic
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }
}
