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

package com.hazelcast.jet.kafka.connect;

import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.jet.retry.IntervalFunction;
import com.hazelcast.jet.retry.RetryStrategies;
import com.hazelcast.jet.retry.RetryStrategy;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionException;

import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.kafka.connect.TestUtil.getConnectorURL;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static com.hazelcast.test.OverridePropertyRule.set;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LONG;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class KafkaConnectNeo4jIT extends JetTestSupport {
    @ClassRule
    public static final OverridePropertyRule enableLogging = set("hazelcast.logging.type", "log4j2");

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectNeo4jIT.class);

    @SuppressWarnings("resource")
    @ClassRule
    public static final Neo4jContainer<?> container = new Neo4jContainer<>(DockerImageName.parse("neo4j:5.5.0"))
            .withoutAuthentication()
            .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix("Docker"));

    private static final int ITEM_COUNT = 1_000;


    @BeforeClass
    public static void setUpDocker() {
        assumeDockerEnabled();
    }

    @Test
    public void testReadFromNeo4jConnector() {
        String sourceName = generateRandomString(5);
        Properties connectorProperties = getConnectorProperties(sourceName);
        insertNodes(sourceName, "items-1");

        Pipeline pipeline = Pipeline.create();
        StreamStage<String> streamStage = pipeline.readFrom(KafkaConnectSources.connect(connectorProperties,
                        TestUtil::convertToString))
                .withoutTimestamps()
                .setLocalParallelism(2);
        streamStage
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> assertThat(list).hasSize(2 * ITEM_COUNT)));

        JobConfig jobConfig = new JobConfig();
        URL connectorURL = getConnectorURL("neo4j-kafka-connect-neo4j-2.0.1.zip");
        jobConfig.addJarsInZip(connectorURL);

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        LOGGER.info("Creating testReadFromNeo4jConnector job");
        Job job = createHazelcastInstance(config).getJet().newJob(pipeline, jobConfig);
        assertThat(job).eventuallyHasStatus(RUNNING);

        insertNodes(sourceName, "items-2");

        try {
            job.join();
            fail("Job should have completed with an AssertionCompletedException, but completed normally");
        } catch (CompletionException e) {

            String errorMsg = e.getCause().getMessage();
            assertTrue("Job was expected to complete with AssertionCompletedException, but completed with: "
                       + e.getCause(), errorMsg.contains(AssertionCompletedException.class.getName()));
        }
    }

    @Test
    public void testDbNotStarted() {
        String sourceName = generateRandomString(5);
        Properties connectorProperties = getConnectorProperties(sourceName);
        // Change the uri so that connector fails to connect
        connectorProperties.setProperty("neo4j.server.uri", "bolt://localhost:52403");
        connectorProperties.setProperty("neo4j.retry.backoff.msecs", "5");
        connectorProperties.setProperty("neo4j.retry.max.attemps", "1");

        Pipeline pipeline = Pipeline.create();
        RetryStrategy strategy = RetryStrategies.custom()
                .maxAttempts(2)
                .intervalFunction(IntervalFunction.constant(500))
                .build();
        StreamStage<String> streamStage = pipeline.readFrom(KafkaConnectSources.connect(connectorProperties,
                        TestUtil::convertToString,
                        strategy))
                .withoutTimestamps()
                .setLocalParallelism(2);
        streamStage.writeTo(Sinks.logger());

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getConnectorURL("neo4j-kafka-connect-neo4j-2.0.1.zip"));

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        LOGGER.info("Creating testDbNotStarted job");
        Job job = createHazelcastInstance(config).getJet().newJob(pipeline, jobConfig);

        assertThat(job).eventuallyHasStatus(FAILED);
    }

    @Test
    public void testDistinct() {
        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        final HazelcastInstance instance = createHazelcastInstance(config);

        String testName = "testDistinct";
        String sourceName = generateRandomString(5);
        Properties connectorProperties = getConnectorProperties(sourceName);

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(KafkaConnectSources.connect(connectorProperties,
                        TestUtil::convertToString))
                .withIngestionTimestamps()
                .peek(s -> ">input " + s)
                .window(WindowDefinition.tumbling(3))
                .distinct()
                .peek(s -> ">distinct " + s)
                .rollingAggregate(AggregateOperations.counting())
                .peek(s -> ">aggreg " + s)
                .writeTo(Sinks.list(testName));
        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getConnectorURL("neo4j-kafka-connect-neo4j-2.0.1.zip"));

        LOGGER.info("Creating a job");
        Job testJob = instance.getJet().newJob(pipeline, jobConfig);
        assertThat(testJob).eventuallyHasStatus(RUNNING);

        String boltUrl = container.getBoltUrl();
        final int expectedSize = 9;

        try (Driver driver = GraphDatabase.driver(boltUrl, AuthTokens.none());
             Session session = driver.session()) {

            List<Map<String, Object>> nodes = new ArrayList<>();
            for (int index = 0; index < expectedSize; index++) {
                Map<String, Object> node = new HashMap<>();
                node.put("name", testName);
                node.put("value", testName + "-value-" + index);
                node.put("timestamp", System.currentTimeMillis());
                nodes.add(node);
            }

            // Use the UNWIND clause to iterate over a parameter named $nodes.
            String sb = "UNWIND $nodes AS node " +
                        // Create a node of type source using the properties from the current element in the list.
                        "CREATE (:" + sourceName + " {name: node.name, value: node.value, timestamp: node.timestamp});";

            // Execute the query with the list of nodes
            session.run(sb, Collections.singletonMap("nodes", nodes));
        }

        final IList<Long> testList = instance.getList(testName);

        assertTrueEventually(() -> assertThat(testList)
                .hasSizeGreaterThan(3)
                .last()
                .asInstanceOf(LONG)
                .isEqualTo(expectedSize)
        );
    }

    private static Properties getConnectorProperties(String sourceName) {
        Properties connectorProperties = new Properties();
        connectorProperties.setProperty("name", "neo4j");
        connectorProperties.setProperty("tasks.max", "1");
        connectorProperties.setProperty("connector.class", "streams.kafka.connect.source.Neo4jSourceConnector");
        connectorProperties.setProperty("topic", "some-topic");
        connectorProperties.setProperty("neo4j.server.uri", container.getBoltUrl());
        connectorProperties.setProperty("neo4j.authentication.basic.username", "neo4j");
        connectorProperties.setProperty("neo4j.authentication.basic.password", "password");
        connectorProperties.setProperty("neo4j.streaming.poll.interval.msecs", "1000");
        connectorProperties.setProperty("neo4j.streaming.property", "timestamp");
        connectorProperties.setProperty("neo4j.streaming.from", "ALL");
        connectorProperties.setProperty("neo4j.enforce.schema", "true");
        connectorProperties.setProperty("neo4j.source.query",
                "MATCH (ts:" + sourceName + ") WHERE ts.timestamp > $lastCheck " +
                "RETURN ts.name AS name, ts.value AS value, ts.timestamp AS timestamp");
        return connectorProperties;
    }

    private static void insertNodes(String sourceName, String prefix) {
        String boltUrl = container.getBoltUrl();
        List<Map<String, Object>> nodes = new ArrayList<>();

        try (Driver driver = GraphDatabase.driver(boltUrl, AuthTokens.none()); Session session = driver.session()) {
            for (int i = 0; i < ITEM_COUNT; i++) {
                Map<String, Object> node = new HashMap<>();
                node.put("name", prefix + "-name-" + i);
                node.put("value", prefix + "-value-" + i);
                node.put("timestamp", System.currentTimeMillis());
                nodes.add(node);
            }

            // Use the UNWIND clause to iterate over a parameter named $nodes.
            String sb = "UNWIND $nodes AS node " +
                        // Create a node of type TestSource using the properties from the current element in the list.
                        "CREATE (:" + sourceName + " {name: node.name, value: node.value, timestamp: node.timestamp});";

            // Execute the query with the list of nodes
            session.run(sb, Collections.singletonMap("nodes", nodes));
        }
    }
}
