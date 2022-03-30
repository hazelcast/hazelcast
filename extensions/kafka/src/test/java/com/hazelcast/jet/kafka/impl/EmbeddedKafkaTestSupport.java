/*
 * Copyright 2021 Hazelcast Inc.
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

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SystemTime;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

import static com.hazelcast.internal.util.OsHelper.isWindows;

class EmbeddedKafkaTestSupport extends KafkaTestSupport {
    private static final String ZK_HOST = "127.0.0.1";
    private static final String BROKER_HOST = "127.0.0.1";
    private EmbeddedZookeeper zkServer;
    private String zkConnect;
    private KafkaServer kafkaServer;
    private int brokerPort = -1;

    @Override
    public void createKafkaCluster() throws IOException {
        System.setProperty("zookeeper.preAllocSize", Integer.toString(128));
        zkServer = new EmbeddedZookeeper();
        zkConnect = ZK_HOST + ':' + zkServer.port();

        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKER_HOST + ":0");
        brokerProps.setProperty("offsets.topic.replication.factor", "1");
        brokerProps.setProperty("offsets.topic.num.partitions", "1");
        // we need this due to avoid OOME while running tests, see https://issues.apache.org/jira/browse/KAFKA-3872
        brokerProps.setProperty("log.cleaner.dedupe.buffer.size", Long.toString(2 * 1024 * 1024L));
        brokerProps.setProperty("transaction.state.log.replication.factor", "1");
        brokerProps.setProperty("transaction.state.log.num.partitions", "1");
        brokerProps.setProperty("transaction.state.log.min.isr", "1");
        brokerProps.setProperty("transaction.abort.timed.out.transaction.cleanup.interval.ms", "200");
        brokerProps.setProperty("group.initial.rebalance.delay.ms", "0");
        KafkaConfig config = new KafkaConfig(brokerProps);
        kafkaServer = TestUtils.createServer(config, new SystemTime());
        brokerPort = TestUtils.boundPort(kafkaServer, SecurityProtocol.PLAINTEXT);

        brokerConnectionString = BROKER_HOST + ':' + brokerPort;
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerConnectionString);
        admin = Admin.create(props);
    }

    @Override
    public void shutdownKafkaCluster() {
        if (kafkaServer != null) {
            kafkaServer.shutdown();
            if (admin != null) {
                admin.close();
            }
            if (producer != null) {
                producer.close();
            }
            try {
                zkServer.shutdown();
            } catch (Exception e) {
                // ignore error on Windows, it fails there, see https://issues.apache.org/jira/browse/KAFKA-6291
                if (!isWindows()) {
                    throw e;
                }
            }
            producer = null;
            kafkaServer = null;
            admin = null;
            zkServer = null;
        }
    }
}
