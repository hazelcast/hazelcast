/*
 * Copyright 2026 Hazelcast Inc.
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

import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.nio.IOUtil.closeResource;

class EmbeddedKafkaTestSupport extends KafkaTestSupport {
    private static final String BROKER_HOST = "127.0.0.1";
    private KafkaClusterTestKit cluster;

    @Override
    protected String createKafkaCluster0(Map<String, String> properties) throws IOException {
        Map<String, String> brokerProps = new HashMap<>();
        brokerProps.put("broker.id", "0");
        brokerProps.put("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.put("listeners", "PLAINTEXT://" + BROKER_HOST + ":0");
        brokerProps.put("offsets.topic.replication.factor", "1");
        brokerProps.put("offsets.topic.num.partitions", "1");
        // we need this due to avoid OOME while running tests, see https://issues.apache.org/jira/browse/KAFKA-3872
        brokerProps.put("log.cleaner.dedupe.buffer.size", Long.toString(2 * 1024 * 1024L));
        brokerProps.put("transaction.state.log.replication.factor", "1");
        brokerProps.put("transaction.state.log.num.partitions", "1");
        brokerProps.put("transaction.state.log.min.isr", "1");
        brokerProps.put("transaction.abort.timed.out.transaction.cleanup.interval.ms", "200");
        brokerProps.put("group.initial.rebalance.delay.ms", "0");

        brokerProps.putAll(properties);

        TestKitNodes nodes = new TestKitNodes.Builder()
            .setCombined(true)
            .setNumBrokerNodes(1)
            .setNumControllerNodes(1)
            .setPerServerProperties(Map.of(0, brokerProps))
            .build();

        try {
            this.cluster = new KafkaClusterTestKit.Builder(nodes).build();
            cluster.startup();
            cluster.waitForReadyBrokers();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return cluster.bootstrapServers();
    }

    @Override
    protected void shutdownKafkaCluster0() {
        if (cluster != null) {
            closeResource(cluster);
            cluster = null;
        }
    }
}
