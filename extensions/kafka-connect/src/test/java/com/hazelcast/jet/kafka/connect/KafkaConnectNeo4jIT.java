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

import javax.annotation.Nonnull;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

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
        Properties connectorProperties = getConnectorProperties();

        insertNodes("items-1");

        Pipeline pipeline = Pipeline.create();
        StreamStage<String> streamStage = pipeline.readFrom(KafkaConnectSources.connect(connectorProperties,
                                                          TestUtil::convertToString))
                                                  .withoutTimestamps()
                                                  .setLocalParallelism(2);
        streamStage
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> assertThat(list).hasSize(2 * ITEM_COUNT)));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getConnectorURL("neo4j-kafka-connect-neo4j-2.0.1.zip"));

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        LOGGER.info("Creating a job");
        Job job = createHazelcastInstance(config).getJet().newJob(pipeline, jobConfig);
        assertJobStatusEventually(job, RUNNING);

        insertNodes("items-2");

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
        Properties connectorProperties = getConnectorProperties();
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
        LOGGER.info("Creating a job");
        Job job = createHazelcastInstance(config).getJet().newJob(pipeline, jobConfig);

        assertJobStatusEventually(job, FAILED);
    }

    @Nonnull
    private static Properties getConnectorProperties() {
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
                "MATCH (ts:TestSource) WHERE ts.timestamp > $lastCheck " +
                        "RETURN ts.name AS name, ts.value AS value, ts.timestamp AS timestamp");
        return connectorProperties;
    }

    @Test
    public void testDistinct() {
        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        final HazelcastInstance instance = createHazelcastInstance(config);

        final String testName = "testDistinct";
        Properties connectorProperties = getConnectorProperties();

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
        assertJobStatusEventually(testJob, RUNNING);
        final AtomicLong recordsCreatedCounter = new AtomicLong();
        String boltUrl = container.getBoltUrl();
        Driver driver = GraphDatabase.driver(boltUrl, AuthTokens.none());
        Session session = driver.session();

        final int expectedSize = 9;
        IntStream.range(0, expectedSize)
                 .forEach(value -> {
                     session.run("CREATE (:TestSource {name: '" + testName + "', value: '"
                             + testName + "-value-" + value + "', timestamp: datetime().epochMillis});");
                     recordsCreatedCounter.incrementAndGet();
                 });

        final IList<Long> testList = instance.getList(testName);

        assertTrueEventually(() -> assertThat(testList)
                .hasSizeGreaterThan(3)
                .last()
                .asInstanceOf(LONG)
                .isEqualTo(expectedSize)
        );

        session.close();
        driver.close();
    }

    private static void insertNodes(String prefix) {
        String boltUrl = container.getBoltUrl();
        try (Driver driver = GraphDatabase.driver(boltUrl, AuthTokens.none()); Session session = driver.session()) {
            for (int i = 0; i < ITEM_COUNT; i++) {
                session.run("CREATE (:TestSource {name: '" + prefix + "-name-" + i + "', value: '"
                        + prefix + "-value-" + i + "', timestamp: datetime().epochMillis});");
            }
        }
    }

}
