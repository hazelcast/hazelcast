/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.examples.kafka;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.map.IMap;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static kafka.admin.AdminUtils.createTopic;

/**
 * A sample which consumes two Kafka topics and writes
 * the received items to an {@code IMap}.
 **/
public class KafkaSource {

    private static final int MESSAGE_COUNT_PER_TOPIC = 1_000_000;
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String AUTO_OFFSET_RESET = "earliest";

    private static final String SINK_NAME = "sink";

    private EmbeddedZookeeper zkServer;
    private ZkUtils zkUtils;
    private KafkaServer kafkaServer;

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.kafka(props(
                "bootstrap.servers", BOOTSTRAP_SERVERS,
                "key.deserializer", StringDeserializer.class.getCanonicalName(),
                "value.deserializer", IntegerDeserializer.class.getCanonicalName(),
                "auto.offset.reset", AUTO_OFFSET_RESET)
                , "t1", "t2"))
         .withoutTimestamps()
         .writeTo(Sinks.map(SINK_NAME));
        return p;
    }

    public static void main(String[] args) throws Exception {
        new KafkaSource().run();
    }

    private void run() throws Exception {
        try {
            createKafkaCluster();
            fillTopics();

            JetInstance jet = Jet.bootstrappedInstance();
            IMap<String, Integer> sinkMap = jet.getMap(SINK_NAME);

            Pipeline p = buildPipeline();

            long start = System.nanoTime();
            Job job = jet.newJob(p);
            while (true) {
                int mapSize = sinkMap.size();
                System.out.format("Received %d entries in %d milliseconds.%n",
                        mapSize, NANOSECONDS.toMillis(System.nanoTime() - start));
                if (mapSize == MESSAGE_COUNT_PER_TOPIC * 2) {
                    job.cancel();
                    break;
                }
                Thread.sleep(100);
            }
        } finally {
            Jet.shutdownAll();
            shutdownKafkaCluster();
        }
    }

    // Creates an embedded zookeeper server and a kafka broker
    private void createKafkaCluster() throws IOException {
        zkServer = new EmbeddedZookeeper();
        String zkConnect = "localhost:" + zkServer.port();
        ZkClient zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        zkUtils = ZkUtils.apply(zkClient, false);

        KafkaConfig config = new KafkaConfig(props(
                "zookeeper.connect", zkConnect,
                "broker.id", "0",
                "log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString(),
                "offsets.topic.replication.factor", "1",
                "listeners", "PLAINTEXT://localhost:9092"));
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
    }

    // Creates 2 topics (t1, t2) with different partition counts (32, 64) and fills them with items
    private void fillTopics() {
        createTopic(zkUtils, "t1", 32, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        createTopic(zkUtils, "t2", 64, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

        System.out.println("Filling Topics");
        Properties props = props(
                "bootstrap.servers", "localhost:9092",
                "key.serializer", StringSerializer.class.getName(),
                "value.serializer", IntegerSerializer.class.getName());
        try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= MESSAGE_COUNT_PER_TOPIC; i++) {
                producer.send(new ProducerRecord<>("t1", "t1-" + i, i));
                producer.send(new ProducerRecord<>("t2", "t2-" + i, i));
            }
            System.out.println("Published " + MESSAGE_COUNT_PER_TOPIC + " messages to topic t1");
            System.out.println("Published " + MESSAGE_COUNT_PER_TOPIC + " messages to topic t2");
        }
    }

    private void shutdownKafkaCluster() {
        kafkaServer.shutdown();
        zkUtils.close();
        zkServer.shutdown();
    }

    private static Properties props(String... kvs) {
        final Properties props = new Properties();
        for (int i = 0; i < kvs.length;) {
            props.setProperty(kvs[i++], kvs[i++]);
        }
        return props;
    }
}
