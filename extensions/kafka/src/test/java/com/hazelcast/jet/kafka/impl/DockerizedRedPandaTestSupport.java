/*
 * Copyright 2023 Hazelcast Inc.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.testcontainers.utility.DockerImageName.parse;

class DockerizedRedPandaTestSupport extends KafkaTestSupport {
    private static final String TEST_REDPANDA_VERSION = System.getProperty("test.redpanda.version", "v22.3.20");
    private static final Logger LOGGER = LoggerFactory.getLogger(DockerizedRedPandaTestSupport.class);

    private RedpandaContainer redpandaContainer;

    @Override
    protected String createKafkaCluster0() {
        DockerImageName imageName = parse("docker.redpanda.com/redpandadata/redpanda:" + TEST_REDPANDA_VERSION);
        redpandaContainer = new RedpandaContainer(imageName)
                .withLogConsumer(new Slf4jLogConsumer(LOGGER));
        redpandaContainer.start();

        return redpandaContainer.getBootstrapServers();
    }

    @Override
    protected void shutdownKafkaCluster0() {
        if (redpandaContainer != null) {
            redpandaContainer.stop();
            redpandaContainer = null;
        }
    }
}
