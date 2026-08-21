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

import com.hazelcast.internal.util.concurrent.ConcurrentMemoizingSupplier;
import com.hazelcast.test.starter.MavenInterface;
import org.jspecify.annotations.NonNull;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.Memoizers.memoizeConcurrent;

class DockerizedKafkaTestSupport extends KafkaTestSupport {
    private static final ConcurrentMemoizingSupplier<String> MAVEN_CONFLUENT_VERSION
        = memoizeConcurrent(() -> MavenInterface.evaluateExpression("confluent.version"));
    private static final Logger LOGGER = LoggerFactory.getLogger(DockerizedKafkaTestSupport.class);
    private GenericContainer<?> kafkaContainer;

    @Override
    protected String createKafkaCluster0(Map<String, String> properties) {
        String kafkaVersion = System.getProperty("test.kafka.version");
        if (kafkaVersion == null) {
            kafkaVersion = MAVEN_CONFLUENT_VERSION.get();
        }
        DockerImageName imageName = DockerImageName.parse("confluentinc/cp-kafka").withTag(kafkaVersion);
        if (new ComparableVersion(kafkaVersion).compareTo(new ComparableVersion("7.4.0")) < 0) {
            // intentionally we want to test with old Kafka and Zookeeper
            //noinspection deprecation
            KafkaContainer legacyKafkaContainer = new KafkaContainer(imageName)
                .withEmbeddedZookeeper()
                .withEnv(toKafkaEnvironments(properties))
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withListener(() -> "kafka:19092");
            kafkaContainer = legacyKafkaContainer;
            kafkaContainer.start();
            return legacyKafkaContainer.getBootstrapServers();
        }

        ConfluentKafkaContainer confluentKafkaContainer = new ConfluentKafkaContainer(imageName)
            .withEnv(toKafkaEnvironments(properties))
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withListener("kafka:19092");
        kafkaContainer = confluentKafkaContainer;
        kafkaContainer.start();
        return confluentKafkaContainer.getBootstrapServers();
    }

    static @NonNull String getKafkaVersion() {
        String kafkaVersion = System.getProperty("test.kafka.version");
        if (kafkaVersion == null) {
            kafkaVersion = MAVEN_CONFLUENT_VERSION.get();
        }
        return kafkaVersion;
    }

    private String toKafkaEnvironment(String property) {
        return "KAFKA_" + property.toUpperCase(Locale.ROOT).replace('.', '_');
    }

    private Map<String, String> toKafkaEnvironments(Map<String, String> properties) {
        return properties
            .entrySet()
            .stream()
            .map(entry -> Map.entry(toKafkaEnvironment(entry.getKey()), entry.getValue()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    protected void shutdownKafkaCluster0() {
        if (kafkaContainer != null) {
            kafkaContainer.stop();
            kafkaContainer = null;
        }
    }
}
