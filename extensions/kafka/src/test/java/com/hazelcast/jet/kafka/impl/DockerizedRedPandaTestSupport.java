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

import org.apache.kafka.clients.admin.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.Properties;

import static org.testcontainers.utility.DockerImageName.parse;

class DockerizedRedPandaTestSupport extends KafkaTestSupport {

    private static final String TEST_REDPANDA_VERSION = System.getProperty("test.redpanda.version", "v22.3.20");
    private static final Logger LOGGER = LoggerFactory.getLogger(DockerizedRedPandaTestSupport.class);

    private RedpandaContainer redpandaContainer;

    public void createKafkaCluster() throws IOException {
        DockerImageName imageName = parse("docker.redpanda.com/redpandadata/redpanda:" + TEST_REDPANDA_VERSION);
        redpandaContainer = new RedpandaContainer(imageName)
                .withLogConsumer(new Slf4jLogConsumer(LOGGER));
        redpandaContainer.start();

        brokerConnectionString = redpandaContainer.getBootstrapServers();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokerConnectionString);
        admin = Admin.create(props);
    }

    public void shutdownKafkaCluster() {
        if (redpandaContainer != null) {
            redpandaContainer.stop();
            if (admin != null) {
                admin.close();
            }
            if (producer != null) {
                producer.close();
            }
            producer = null;
            admin = null;
            redpandaContainer = null;
            brokerConnectionString = null;
        }
    }

}
