/*
 * Copyright 2024 Hazelcast Inc.
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

import com.hazelcast.jet.impl.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.Objects;

class DockerizedKafkaTestSupport extends KafkaTestSupport {
    // TODO This should lookup "confluent.version" Maven property
    private static final String TEST_KAFKA_VERSION = System.getProperty("test.kafka.version", "7.7.1");
    private static final Logger LOGGER = LoggerFactory.getLogger(DockerizedKafkaTestSupport.class);

    private KafkaContainer kafkaContainer;

    @Override
    protected String createKafkaCluster0() throws IOException {
        String kafkaContainerStarterScript = (String) Objects
                .requireNonNull(ReflectionUtils.readStaticFieldOrNull(KafkaContainer.class.getName(), "STARTER_SCRIPT"));

        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka").withTag(TEST_KAFKA_VERSION))
                .withEmbeddedZookeeper()
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                // Workaround for https://github.com/testcontainers/testcontainers-java/issues/3288
                // It adds 0.5s sleep before running the script copied from the host to the container.
                .withCommand("-c",
                    "while [ ! -f %1$s ]; do sleep 0.1; done; sleep 0.5; %1$s".formatted(kafkaContainerStarterScript));
        kafkaContainer.start();

        return kafkaContainer.getBootstrapServers();
    }

    @Override
    protected void shutdownKafkaCluster0() {
        if (kafkaContainer != null) {
            kafkaContainer.stop();
            kafkaContainer = null;
        }
    }
}
