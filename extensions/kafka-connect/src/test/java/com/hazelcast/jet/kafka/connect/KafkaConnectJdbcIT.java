/*
 * Copyright 2025 Hazelcast Inc.
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
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.pipeline.test.AssertionCompletedException;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.jdbc.MySQLDatabaseProvider;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.jet.TestedVersions.TOXIPROXY_IMAGE;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.FAILED;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.kafka.connect.KafkaConnectSources.connect;
import static com.hazelcast.jet.kafka.connect.TestUtil.getConnectorURL;
import static com.hazelcast.jet.pipeline.Sinks.map;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static com.hazelcast.test.OverridePropertyRule.set;
import static eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM;
import static eu.rekawek.toxiproxy.model.ToxicDirection.UPSTREAM;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class KafkaConnectJdbcIT extends JetTestSupport {
    @ClassRule
    public static final OverridePropertyRule enableLogging = set("hazelcast.logging.type", "log4j2");

    public static final String USERNAME = "mysql";
    public static final String PASSWORD = "mysql";
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectJdbcIT.class);

    private static final Network network = Network.newNetwork();

    @SuppressWarnings("resource")
    @ClassRule
    public static final MySQLContainer<?> mysql = MySQLDatabaseProvider.createContainer()
            .withUsername(USERNAME)
            .withPassword(PASSWORD)
            .withNetwork(network)
            .withNetworkAliases("mysql")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix("Docker"));

    private static final int ITEM_COUNT = 1_000;
    private static final String FILE_NAME = "confluentinc-kafka-connect-jdbc-10.6.3.zip";

    @BeforeClass
    public static void setUpDocker() {
        assumeDockerEnabled();
    }


    @Test
    public void testReading() throws Exception {
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "confluentinc-kafka-connect-jdbc");
        randomProperties.setProperty("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
        randomProperties.setProperty("mode", "incrementing");
        String connectionUrl = mysql.getJdbcUrl();
        randomProperties.setProperty("connection.url", connectionUrl);
        randomProperties.setProperty("connection.user", USERNAME);
        randomProperties.setProperty("connection.password", PASSWORD);
        randomProperties.setProperty("incrementing.column.name", "id");
        randomProperties.setProperty("table.whitelist", "items1");
        randomProperties.setProperty("table.poll.interval.ms", "5000");

        createTableAndFill(connectionUrl, "items1");


        Pipeline pipeline = Pipeline.create();
        StreamStage<String> streamStage = pipeline.readFrom(KafkaConnectSources.connect(randomProperties,
                        TestUtil::convertToString))
                .withoutTimestamps();
        streamStage
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> assertEquals(ITEM_COUNT, list.size())));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getConnectorURL(FILE_NAME));

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        Job job = createHazelcastInstance(config).getJet().newJob(pipeline, jobConfig);

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
    public void testScaling() throws Exception {
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "confluentinc-kafka-connect-jdbc");
        randomProperties.setProperty("tasks.max", "2");
        randomProperties.setProperty("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
        randomProperties.setProperty("mode", "incrementing");
        String connectionUrl = mysql.getJdbcUrl();
        randomProperties.setProperty("connection.url", connectionUrl);
        randomProperties.setProperty("connection.user", USERNAME);
        randomProperties.setProperty("connection.password", PASSWORD);
        randomProperties.setProperty("incrementing.column.name", "id");
        randomProperties.setProperty("table.whitelist", "parallel_items_1,parallel_items_2");
        randomProperties.setProperty("table.poll.interval.ms", "5000");

        createTableAndFill(connectionUrl, "parallel_items_1");
        createTableAndFill(connectionUrl, "parallel_items_2");


        Pipeline pipeline = Pipeline.create();
        StreamStage<String> streamStage = pipeline.readFrom(KafkaConnectSources.connect(randomProperties,
                        TestUtil::convertToString))
                .withoutTimestamps();
        streamStage
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> assertEquals(2 * ITEM_COUNT, list.size())));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getConnectorURL(FILE_NAME));

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        Job job = createHazelcastInstances(config, 3)[0].getJet().newJob(pipeline, jobConfig);

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
    public void testScaling_with_new_member() throws Exception {
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "confluentinc-kafka-connect-jdbc");
        randomProperties.setProperty("tasks.max", "2");
        randomProperties.setProperty("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
        randomProperties.setProperty("mode", "incrementing");
        String connectionUrl = mysql.getJdbcUrl();
        randomProperties.setProperty("connection.url", connectionUrl);
        randomProperties.setProperty("connection.user", USERNAME);
        randomProperties.setProperty("connection.password", PASSWORD);
        randomProperties.setProperty("incrementing.column.name", "id");
        randomProperties.setProperty("table.whitelist", "newmember_parallel_items_1,newmember_parallel_items_2");
        randomProperties.setProperty("table.poll.interval.ms", "5000");
        randomProperties.setProperty("batch.max.rows", "1");

        createTableAndFill(connectionUrl, "newmember_parallel_items_1");
        createTableAndFill(connectionUrl, "newmember_parallel_items_2");

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        HazelcastInstance hazelcastInstance1 = createHazelcastInstances(config, 1) [0];

        String listName = "destinationList";
        IList<Object> sinkList = hazelcastInstance1.getList(listName);

        // Create job
        Pipeline pipeline = Pipeline.create();
        StreamStage<String> streamStage = pipeline.readFrom(KafkaConnectSources.connect(randomProperties,
                        TestUtil::convertToString))
                .withoutTimestamps();
        streamStage.writeTo(Sinks.list(sinkList));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        jobConfig.addJarsInZip(getConnectorURL(FILE_NAME));

        Job job = hazelcastInstance1.getJet().newJob(pipeline, jobConfig);

        // There are some items in the list. The job is running
        assertTrueEventually(() -> assertTrue(sinkList.size() > 10));

        // Add one more member to  cluster. Job is going to auto-scale
        HazelcastInstance hazelcastInstance2 = createHazelcastInstances(config, 1)[0];
        assertClusterSizeEventually(2, hazelcastInstance1, hazelcastInstance2);

        // We should see more items in the list
        assertTrueEventually(() -> assertTrue(sinkList.size() >= 2 * ITEM_COUNT));
    }

    @Test
    public void testDynamicReconfiguration() throws Exception {
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "confluentinc-kafka-connect-jdbc");
        randomProperties.setProperty("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
        randomProperties.setProperty("mode", "incrementing");
        String connectionUrl = mysql.getJdbcUrl();
        randomProperties.setProperty("connection.url", connectionUrl);
        randomProperties.setProperty("connection.user", USERNAME);
        randomProperties.setProperty("connection.password", PASSWORD);
        randomProperties.setProperty("incrementing.column.name", "id");
        randomProperties.setProperty("table.whitelist", "dynamic_test_items1,dynamic_test_items2,dynamic_test_items3");
        randomProperties.setProperty("table.poll.interval.ms", "1000");

        createTableAndFill(connectionUrl, "dynamic_test_items1");

        Pipeline pipeline = Pipeline.create();
        StreamStage<String> streamStage = pipeline.readFrom(KafkaConnectSources.connect(randomProperties,
                        TestUtil::convertToString))
                .withoutTimestamps()
                .setLocalParallelism(1);
        streamStage
                .writeTo(AssertionSinks.assertCollectedEventually(60,
                        list -> assertEquals(3 * ITEM_COUNT, list.size())));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getConnectorURL(FILE_NAME));

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        Job job = hazelcastInstance.getJet().newJob(pipeline, jobConfig);

        createTableAndFill(connectionUrl, "dynamic_test_items2");
        createTableAndFill(connectionUrl, "dynamic_test_items3");

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
    public void reconnectsOnNetworkProblem() throws Exception {
        final String testName = "reconnectsOnNetworkProblem";
        try (var toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("toxiproxy")) {
            toxiproxy.start();

            var toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
            var proxy = toxiproxyClient.createProxy("mysql", "0.0.0.0:8666", "mysql:3306");

            String connectionUrl = mysql.getJdbcUrl();

            String host = toxiproxy.getHost();
            int port = toxiproxy.getMappedPort(8666);
            String toxiUrl = connectionUrl.replaceFirst("localhost:\\d+", host + ":" + port);

            createTable(mysql.getJdbcUrl(), testName);

            Properties connectorProperties = new Properties();
            connectorProperties.setProperty("name", "confluentinc-kafka-connect-jdbc");
            connectorProperties.setProperty("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
            connectorProperties.setProperty("mode", "incrementing");
            connectorProperties.setProperty("connection.url", toxiUrl);
            connectorProperties.setProperty("connection.user", USERNAME);
            connectorProperties.setProperty("connection.password", PASSWORD);
            connectorProperties.setProperty("incrementing.column.name", "id");
            connectorProperties.setProperty("table.whitelist", testName);
            connectorProperties.setProperty("table.poll.interval.ms", "1000");
            connectorProperties.setProperty("connections.max.idle.ms", "2000"); // 2 seconds
            connectorProperties.setProperty("session.timeout.ms", "2000"); // 2 seconds
            connectorProperties.setProperty("request.timeout.ms", "2000"); // 2 seconds

            Config config = smallInstanceConfig();
            config.getJetConfig().setResourceUploadEnabled(true);
            HazelcastInstance[] hazelcastInstances = createHazelcastInstances(config, 3);
            var hazelcastInstance = hazelcastInstances[0];

            IMap<String, String> itemsMap = hazelcastInstance.getMap("itemsMap" + System.currentTimeMillis());
            Map<String, String> map = itemsMap;
            var pipeline = Pipeline.create();
            pipeline.readFrom(connect(connectorProperties,
                            TestUtil::convertToStringWithJustIndexForMongo))
                    .withIngestionTimestamps()
                    .setLocalParallelism(1)
                    .map(e -> Map.entry(e, e))
                    .writeTo(map(itemsMap));

            logger.info("Creating a job");
            JobConfig jobConfig = new JobConfig();
            jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
            jobConfig.setSnapshotIntervalMillis(500);
            jobConfig.addJarsInZip(getConnectorURL(FILE_NAME));

            Job job = hazelcastInstance.getJet().newJob(pipeline, jobConfig);
            assertThat(job).eventuallyHasStatus(RUNNING);

            var jobRepository = new JobRepository(hazelcastInstance);
            waitForNextSnapshot(jobRepository, job.getId(), 30, false);

            AtomicLong recordsCreatedCounter = new AtomicLong();
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            ScheduledFuture<?> scheduledFuture = scheduler.scheduleWithFixedDelay(
                    () -> createOneRecord("reconnectsOnNetworkProblem",
                    recordsCreatedCounter.incrementAndGet()), 0, 10, MILLISECONDS);

            Thread.sleep(1_000);
            final int currentElements = map.size();

            assertTrueEventually(() -> {
                Job j = hazelcastInstance.getJet().getJob(job.getId());
                assert j != null;
                assertThat(j.getStatus()).isEqualTo(JobStatus.RUNNING);
            });

            proxy.toxics().bandwidth(DOWNSTREAM.name(), DOWNSTREAM, 0);
            proxy.toxics().bandwidth(UPSTREAM.name(), UPSTREAM, 0);

            Thread.sleep(7_000);

            proxy.toxics().get(DOWNSTREAM.name()).remove();
            proxy.toxics().get(UPSTREAM.name()).remove();

            // Wait for new elements
            assertTrueEventually(() -> assertThat(map).hasSizeGreaterThan(currentElements));
            scheduledFuture.cancel(false);

            assertTrueEventually(() -> assertThat(map).hasSize(recordsCreatedCounter.intValue()));
        }
    }

    private void createTableAndFill(String connectionUrl, String tableName) throws SQLException {
        createTable(connectionUrl, tableName);
        insertItems(connectionUrl, tableName);
    }

    public void createOneRecord(String testName, long index) {
        try (Connection conn = DriverManager.getConnection(mysql.getJdbcUrl(), USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()
        ) {
            String sql = String.format("INSERT INTO %s VALUES (%d , %d)", testName, index, index);
            stmt.addBatch(sql);
            stmt.executeBatch();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static void insertItems(String connectionUrl, String tableName) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()
        ) {
            for (int i = 0; i < ITEM_COUNT; i++) {
                String sql = String.format("INSERT INTO %s VALUES(%d, '%s-%d')", tableName, i, tableName, i);
                stmt.addBatch(sql);
            }
            int[] ints = stmt.executeBatch();
            boolean allMatch = Arrays.stream(ints).allMatch(i -> i == 1);
            assertTrue(allMatch);
        }
    }

    private static void createTable(String connectionUrl, String tableName) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute("CREATE TABLE " + tableName + " (id INT PRIMARY KEY, name VARCHAR(128))");
        }
    }

    @Test
    public void windowing_withIngestionTimestamps_should_work() throws Exception {
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "confluentinc-kafka-connect-jdbc");
        randomProperties.setProperty("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
        randomProperties.setProperty("mode", "incrementing");
        String connectionUrl = mysql.getJdbcUrl();
        randomProperties.setProperty("connection.url", connectionUrl);
        randomProperties.setProperty("connection.user", USERNAME);
        randomProperties.setProperty("connection.password", PASSWORD);
        randomProperties.setProperty("incrementing.column.name", "id");
        randomProperties.setProperty("table.whitelist", "windowing_test");
        randomProperties.setProperty("table.poll.interval.ms", "1000");

        createTable(connectionUrl, "windowing_test");

        final Pipeline pipeline = Pipeline.create();
        StreamStage<Long> streamStage = pipeline.readFrom(KafkaConnectSources.connect(randomProperties,
                        TestUtil::convertToString))
                .withIngestionTimestamps()
                .setLocalParallelism(1)
                .window(WindowDefinition.tumbling(10))
                .distinct()
                .rollingAggregate(AggregateOperations.counting());
        streamStage.writeTo(Sinks.list("windowing_test_results"));
        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(getConnectorURL(FILE_NAME));
        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        Job job = hazelcastInstance.getJet().newJob(pipeline, jobConfig);
        assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);

        insertItems(connectionUrl, "windowing_test");

        IList<Long> list = hazelcastInstance.getList("windowing_test_results");
        assertTrueEventually(() -> Assertions.assertThat(list)
                .isNotEmpty()
                .last().isEqualTo((long) ITEM_COUNT));

        job.cancel();
        assertThat(job).eventuallyHasStatus(FAILED);
    }

}
