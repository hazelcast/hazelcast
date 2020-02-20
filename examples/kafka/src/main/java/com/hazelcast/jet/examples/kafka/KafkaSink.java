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
import com.hazelcast.jet.kafka.KafkaSinks;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Properties;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A sample which consumes an {@code IMap} and writes
 * the received items to a Kafka Topic.
 **/
public class KafkaSink {

    private static final int MESSAGE_COUNT = 50_000;
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String AUTO_OFFSET_RESET = "earliest";

    private static final String SOURCE_NAME = "source";
    private static final String SINK_TOPIC_NAME = "t1";

    private EmbeddedZookeeper zkServer;
    private ZkUtils zkUtils;
    private KafkaServer kafkaServer;
    private KafkaConsumer kafkaConsumer;

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.map(SOURCE_NAME))
         .writeTo(KafkaSinks.kafka(props(
                 "bootstrap.servers", BOOTSTRAP_SERVERS,
                 "key.serializer", StringSerializer.class.getCanonicalName(),
                 "value.serializer", IntegerSerializer.class.getCanonicalName()),
                 SINK_TOPIC_NAME));
        return p;
    }

    public static void main(String[] args) throws Exception {
        new KafkaSink().run();
    }

    private void run() throws Exception {
        try {
            createKafkaCluster();

            JetInstance jet = Jet.bootstrappedInstance();

            IMap<String, Integer> sourceMap = jet.getMap(SOURCE_NAME);
            fillIMap(sourceMap);


            Pipeline p = buildPipeline();

            long start = System.nanoTime();
            Job job = jet.newJob(p);

            System.out.println("Consuming Topics");
            kafkaConsumer = TestUtils.createConsumer(
                    BOOTSTRAP_SERVERS,
                    "verification-consumer",
                    AUTO_OFFSET_RESET,
                    true,
                    true,
                    4096,
                    SecurityProtocol.PLAINTEXT,
                    Option.<File>empty(),
                    Option.<Properties>empty(),
                    new StringDeserializer(),
                    new IntegerDeserializer());
            kafkaConsumer.subscribe(Collections.singleton(SINK_TOPIC_NAME));

            int totalMessagesSeen = 0;
            while (true) {
                ConsumerRecords records = kafkaConsumer.poll(10000);
                totalMessagesSeen += records.count();
                System.out.format("Received %d entries in %d milliseconds.%n",
                        totalMessagesSeen, NANOSECONDS.toMillis(System.nanoTime() - start));
                if (totalMessagesSeen == MESSAGE_COUNT) {
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

    private void fillIMap(IMap<String, Integer> sourceMap) {
        System.out.println("Filling IMap");
        for (int i = 1; i <= MESSAGE_COUNT; i++) {
            sourceMap.put("t1-" + i, i);
        }
        System.out.println("Published " + MESSAGE_COUNT + " messages to IMap -> " + SOURCE_NAME);
    }

    private void shutdownKafkaCluster() {
        kafkaServer.shutdown();
        kafkaConsumer.close();
        zkUtils.close();
        zkServer.shutdown();
    }

    private static Properties props(String... kvs) {
        final Properties props = new Properties();
        for (int i = 0; i < kvs.length; ) {
            props.setProperty(kvs[i++], kvs[i++]);
        }
        return props;
    }
}
