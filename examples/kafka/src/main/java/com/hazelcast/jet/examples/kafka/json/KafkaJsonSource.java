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

package com.hazelcast.jet.examples.kafka.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.Properties;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static kafka.admin.AdminUtils.createTopic;

/**
 * A sample which demonstrates how to consume items using custom JSON
 * serialization.
 */
public class KafkaJsonSource {

    private static final String ZK_HOST = "127.0.0.1";
    private static final String BROKER_HOST = "127.0.0.1";
    private static final int SESSION_TIMEOUT = 30000;
    private static final int CONNECTION_TIMEOUT = 30000;
    private static final String AUTO_OFFSET_RESET = "earliest";
    private static final String TOPIC = "topic";
    private static final String SINK_MAP_NAME = "users";

    private EmbeddedZookeeper zkServer;
    private ZkUtils zkUtils;
    private KafkaServer kafkaServer;
    private int brokerPort;

    private Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.<Integer, JsonNode>kafka(props(
                "bootstrap.servers", BROKER_HOST + ':' + brokerPort,
                "key.deserializer", IntegerDeserializer.class.getName(),
                "value.deserializer", JsonDeserializer.class.getName(),
                "auto.offset.reset", AUTO_OFFSET_RESET), TOPIC))
         .withoutTimestamps()
         .peek()
         .map(e -> entry(e.getKey(), e.getValue().toString()))
         .writeTo(Sinks.map(SINK_MAP_NAME));
        return p;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        new KafkaJsonSource().go();
    }

    private void go() throws Exception {
        try {
            createKafkaCluster();
            createAndFillTopic();

            JetInstance jet = Jet.bootstrappedInstance();

            long start = System.nanoTime();

            Job job = jet.newJob(buildPipeline());

            IMap<Integer, String> sinkMap = jet.getMap(SINK_MAP_NAME);

            while (true) {
                int mapSize = sinkMap.size();
                System.out.format("Received %d entries in %d milliseconds.%n",
                        mapSize, NANOSECONDS.toMillis(System.nanoTime() - start));
                if (mapSize == 20) {
                    SECONDS.sleep(1);
                    cancel(job);
                    break;
                }
                Thread.sleep(100);
            }
        } finally {
            Jet.shutdownAll();
            shutdownKafkaCluster();
        }
    }

    private void createAndFillTopic() {
        createTopic(zkUtils, TOPIC, 4, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        Properties props = props(
                "bootstrap.servers", BROKER_HOST + ':' + brokerPort,
                "key.serializer", IntegerSerializer.class.getName(),
                "value.serializer", JsonSerializer.class.getName());

        ObjectMapper mapper = new ObjectMapper();
        try (KafkaProducer<Integer, JsonNode> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 20; i++) {
                User user = new User("name" + i, "pass" + i, i, i % 2 == 0);
                producer.send(new ProducerRecord<>(TOPIC, i, mapper.valueToTree(user)));
            }
        }
    }

    private void createKafkaCluster() throws IOException {
        System.setProperty("zookeeper.preAllocSize", Integer.toString(128));
        zkServer = new EmbeddedZookeeper();
        String zkConnect = ZK_HOST + ':' + zkServer.port();
        ZkClient zkClient = new ZkClient(zkConnect, SESSION_TIMEOUT, CONNECTION_TIMEOUT, ZKStringSerializer$.MODULE$);
        zkUtils = ZkUtils.apply(zkClient, false);
        brokerPort = randomPort();

        KafkaConfig config = new KafkaConfig(props(
                "zookeeper.connect", zkConnect,
                "broker.id", "0",
                "log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString(),
                "listeners", "PLAINTEXT://" + BROKER_HOST + ':' + brokerPort,
                "offsets.topic.replication.factor", "1",
                "offsets.topic.num.partitions", "1"));
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
    }


    private void shutdownKafkaCluster() {
        kafkaServer.shutdown();
        zkUtils.close();
        zkServer.shutdown();
    }

    private static void cancel(Job job) {
        job.cancel();
        while (job.getStatus() != JobStatus.FAILED) {
            uncheckRun(() -> SECONDS.sleep(1));
        }
    }

    private static int randomPort() throws IOException {
        ServerSocket server = null;
        try {
            server = new ServerSocket(0);
            return server.getLocalPort();
        } finally {
            if (server != null) {
                server.close();
            }
        }
    }

    private static Properties props(String... kvs) {
        final Properties props = new Properties();
        for (int i = 0; i < kvs.length; ) {
            props.setProperty(kvs[i++], kvs[i++]);
        }
        return props;
    }

}
