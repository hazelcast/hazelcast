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

package com.hazelcast.jet.kafka.connect;

import com.hazelcast.collection.IList;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.pipeline.ServiceFactories.nonSharedService;
import static com.hazelcast.jet.pipeline.Sinks.list;
import static com.hazelcast.jet.pipeline.Sinks.map;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static com.hazelcast.test.OverridePropertyRule.set;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.rangeClosed;
import static org.assertj.core.api.Assertions.assertThat;

@Ignore
@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class})
public class KafkaConnectScalingIT extends JetTestSupport {
    @ClassRule
    public static final OverridePropertyRule enableLogging = set("hazelcast.logging.type", "log4j2");

    public static final String USERNAME = "mysql";
    public static final String PASSWORD = "mysql";
    private static final int TABLE_COUNT = 8;
    private static final String TESTED_TABLES = rangeClosed(1, TABLE_COUNT)
            .mapToObj(i -> "parallel_items_" + i)
            .collect(joining(","));
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectScalingIT.class);
    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    @SuppressWarnings("resource")
    private static final MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0.33")
            .withUsername(USERNAME).withPassword(PASSWORD)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix("Docker"));

    private static final int ITEM_COUNT = 1_000;

    private static final String CONNECTOR_URL = "https://repository.hazelcast.com/download"
            + "/tests/confluentinc-kafka-connect-jdbc-10.6.3.zip";

    @BeforeClass
    public static void setUpDocker() {
        assumeDockerEnabled();
        mysql.start();
    }

    @AfterClass
    public static void afterAll() {
        if (mysql != null) {
            mysql.stop();
        }
    }

    @After
    public void removeTables() {
        for (int i = 1; i <= TABLE_COUNT; i++) {
            try (Connection conn = DriverManager.getConnection(mysql.getJdbcUrl(), USERNAME, PASSWORD);
                 Statement stmt = conn.createStatement()
            ) {
                stmt.execute("DROP TABLE IF EXISTS parallel_items_" + i);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testScaling() throws Exception {
        final int localParallelism = 2;
        final int nodeCount = 3;
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "confluentinc-kafka-connect-jdbc");
        randomProperties.setProperty("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
        randomProperties.setProperty("mode", "incrementing");
        String connectionUrl = mysql.getJdbcUrl();
        randomProperties.setProperty("connection.url", connectionUrl);
        randomProperties.setProperty("connection.user", USERNAME);
        randomProperties.setProperty("connection.password", PASSWORD);
        randomProperties.setProperty("incrementing.column.name", "id");
        randomProperties.setProperty("table.whitelist", TESTED_TABLES);
        randomProperties.setProperty("table.poll.interval.ms", "5000");
        randomProperties.setProperty("tasks.max", String.valueOf(localParallelism * nodeCount));

        rangeClosed(1, TABLE_COUNT).forEach(i -> createTableAndFill(connectionUrl, "parallel_items_" + i));

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        HazelcastInstance[] instances = createHazelcastInstances(config, nodeCount);
        HazelcastInstance instance = instances[0];

        IMap<String, Integer> processors = instance.getMap("processors_" + randomName());
        IMap<String, String> processorInstances = instance.getMap("processorInstances_" + randomName());
        IList<String> values = instance.getList("values" + randomName());
        Pipeline pipeline = Pipeline.create();
        StreamStage<String> mappedValueStage = pipeline
                .readFrom(KafkaConnectSources.connect(randomProperties,
                        SourceRecordUtil::convertToString))
                .withoutTimestamps()
                .setLocalParallelism(localParallelism);

        mappedValueStage
                .writeTo(list(values));

        mappedValueStage
                .mapUsingService(nonSharedService(Context::globalProcessorIndex),
                        (ctx, item) -> Map.entry(item, ctx))
                .setLocalParallelism(localParallelism)
                .writeTo(map(processors));

        mappedValueStage
                .mapUsingService(nonSharedService(ctx -> ctx.hazelcastInstance().getName()),
                        (ctx, item) -> Map.entry(item, ctx))
                .setLocalParallelism(localParallelism)
                .writeTo(map(processorInstances));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        jobConfig.addJarsInZip(new URL(CONNECTOR_URL));

        Job job = instance.getJet().newJob(pipeline, jobConfig);

        // 6 not 8, because we are bounded by total parallelism of the job
        var expectedProcessorIndexArray = IntStream.range(0, nodeCount * localParallelism).boxed().toArray(Integer[]::new);
        var allInstancesNames = Arrays.stream(instances).map(HazelcastInstance::getName).toArray(String[]::new);
        assertTrueEventually(() -> {
            Set<Integer> array = new TreeSet<>(processors.values());
            assertThat(array).containsExactlyInAnyOrder(expectedProcessorIndexArray);

            Set<String> instanceNames = new TreeSet<>(processorInstances.values());
            assertThat(instanceNames).containsExactlyInAnyOrder(allInstancesNames);

            assertThat(values).hasSize(TABLE_COUNT * ITEM_COUNT);
        });
        try {
            job.cancel();
        } catch (Exception ignored) {
        }
        assertTrueEventually(() -> assertThat(job.getStatus()).isEqualTo(JobStatus.FAILED));
        for (HazelcastInstance hi : instances) {
            hi.getLifecycleService().terminate();
        }
    }

    @Test
    public void testScalingOfHazelcastUp() throws Exception {
        int localParallelism = 1;
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "confluentinc-kafka-connect-jdbc");
        randomProperties.setProperty("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
        randomProperties.setProperty("mode", "incrementing");
        String connectionUrl = mysql.getJdbcUrl();
        randomProperties.setProperty("connection.url", connectionUrl);
        randomProperties.setProperty("connection.user", USERNAME);
        randomProperties.setProperty("connection.password", PASSWORD);
        randomProperties.setProperty("incrementing.column.name", "id");
        randomProperties.setProperty("table.whitelist", TESTED_TABLES);
        randomProperties.setProperty("table.poll.interval.ms", "5000");

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        HazelcastInstance instance = createHazelcastInstance(config);
        rangeClosed(1, TABLE_COUNT).forEach(i -> createTableAndFill(connectionUrl, "parallel_items_" + i));

        IMap<String, Integer> processors = instance.getMap("processors_" + randomName());
        IMap<String, String> processorInstances = instance.getMap("processorInstances_" + randomName());
        IMap<String, String> values = instance.getMap("values" + randomName());
        Pipeline pipeline = Pipeline.create();
        StreamStage<String> mappedValueStage = pipeline
                .readFrom(KafkaConnectSources.connect(randomProperties,
                        SourceRecordUtil::convertToString))
                .withoutTimestamps()
                .setLocalParallelism(localParallelism);

        mappedValueStage
                .map(e -> tuple2(e, e))
                .writeTo(map(values));

        mappedValueStage
                .mapUsingService(nonSharedService(Context::globalProcessorIndex),
                        (ctx, item) -> Map.entry(item, ctx))
                .setLocalParallelism(localParallelism)
                .writeTo(map(processors));

        mappedValueStage
                .mapUsingService(nonSharedService(ctx -> ctx.hazelcastInstance().getName()),
                        (ctx, item) -> Map.entry(item, ctx))
                .setLocalParallelism(localParallelism)
                .writeTo(map(processorInstances));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        jobConfig.addJarsInZip(new URL(CONNECTOR_URL));

        instance.getJet().newJob(pipeline, jobConfig);

        assertTrueEventually(() -> {
            assertThat(new HashSet<>(processors.values())).containsExactlyInAnyOrder(0);

            assertThat(values.values()).hasSize(TABLE_COUNT * ITEM_COUNT);
        });

        HazelcastInstance second = createHazelcastInstance(config);
        assertTrueEventually(() -> assertThat(instance.getCluster().getMembers()).hasSize(2));
        insertItems(connectionUrl, "parallel_items_1");

        final String[] instances = { instance.getName(), second.getName() };
        var expectedProcessorIndexArray = new Integer[] { 0, 1 };
        assertTrueEventually(() -> {
            Set<Integer> array = new TreeSet<>(processors.values());
            assertThat(array).containsExactlyInAnyOrder(expectedProcessorIndexArray);

            Set<String> instanceNames = new TreeSet<>(processorInstances.values());
            assertThat(instanceNames).containsExactlyInAnyOrder(instances);

            // +1, because we inserted twice as much to parallel_items_1
            assertThat(values.values()).hasSize((TABLE_COUNT + 1) * ITEM_COUNT);
        });
        instance.shutdown();
        second.shutdown();
    }

    @Test
    public void testScalingOfHazelcastDown() throws Exception {
        int localParallelism = 1;
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "confluentinc-kafka-connect-jdbc");
        randomProperties.setProperty("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
        randomProperties.setProperty("mode", "incrementing");
        String connectionUrl = mysql.getJdbcUrl();
        randomProperties.setProperty("connection.url", connectionUrl);
        randomProperties.setProperty("connection.user", USERNAME);
        randomProperties.setProperty("connection.password", PASSWORD);
        randomProperties.setProperty("incrementing.column.name", "id");
        randomProperties.setProperty("table.whitelist", TESTED_TABLES);
        randomProperties.setProperty("table.poll.interval.ms", "5000");

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        HazelcastInstance[] instances = createHazelcastInstances(config, 3);
        HazelcastInstance instance = instances[0];
        rangeClosed(1, TABLE_COUNT).forEach(i -> createTableAndFill(connectionUrl, "parallel_items_" + i));

        IMap<String, Integer> processors = instance.getMap("processors_" + randomName());
        IMap<String, String> processorInstances = instance.getMap("processorInstances_" + randomName());
        IMap<String, String> values = instance.getMap("values" + randomName());
        Pipeline pipeline = Pipeline.create();
        StreamStage<String> mappedValueStage = pipeline
                .readFrom(KafkaConnectSources.connect(randomProperties,
                        SourceRecordUtil::convertToString))
                .withoutTimestamps()
                .setLocalParallelism(localParallelism);

        mappedValueStage
                .map(e -> tuple2(e, e))
                .writeTo(map(values));

        mappedValueStage
                .mapUsingService(nonSharedService(Context::globalProcessorIndex),
                        (ctx, item) -> Map.entry(item, ctx))
                .setLocalParallelism(localParallelism)
                .writeTo(map(processors));

        mappedValueStage
                .mapUsingService(nonSharedService(ctx -> ctx.hazelcastInstance().getName()),
                        (ctx, item) -> Map.entry(item, ctx))
                .setLocalParallelism(localParallelism)
                .writeTo(map(processorInstances));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        jobConfig.addJarsInZip(new URL(CONNECTOR_URL));

        instance.getJet().newJob(pipeline, jobConfig);

        assertTrueEventually(() -> {
            assertThat(new HashSet<>(processors.values())).containsExactlyInAnyOrder(0, 1, 2);

            assertThat(values.values()).hasSize(TABLE_COUNT * ITEM_COUNT);
        });

        instances[2].shutdown();
        assertTrueEventually(() -> assertThat(instance.getCluster().getMembers()).hasSize(2));
        insertItems(connectionUrl, "parallel_items_1");

        final String[] instancesNames = { instance.getName(), instances[1].getName() };
        var expectedProcessorIndexArray = new Integer[] { 0, 1 };
        assertTrueEventually(() -> {
            Set<Integer> array = new TreeSet<>(processors.values());
            assertThat(array).containsExactlyInAnyOrder(expectedProcessorIndexArray);

            Set<String> instanceNames = new TreeSet<>(processorInstances.values());
            assertThat(instanceNames).containsExactlyInAnyOrder(instancesNames);

            // +1, because we inserted twice as much to parallel_items_1
            assertThat(values.values()).hasSize((TABLE_COUNT + 1) * ITEM_COUNT);
        });
        for (HazelcastInstance hi : instances) {
            hi.getLifecycleService().terminate();
        }
    }

    @Test
    public void testDynamicReconfiguration() throws Exception {
        int localParallelism = 2;
        Properties randomProperties = new Properties();
        randomProperties.setProperty("name", "confluentinc-kafka-connect-jdbc");
        randomProperties.setProperty("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
        randomProperties.setProperty("mode", "incrementing");
        String connectionUrl = mysql.getJdbcUrl();
        randomProperties.setProperty("connection.url", connectionUrl);
        randomProperties.setProperty("connection.user", USERNAME);
        randomProperties.setProperty("connection.password", PASSWORD);
        randomProperties.setProperty("incrementing.column.name", "id");
        randomProperties.setProperty("table.whitelist", TESTED_TABLES);
        randomProperties.setProperty("table.poll.interval.ms", "5000");

        createTableAndFill(connectionUrl, "parallel_items_1");
        createTableAndFill(connectionUrl, "parallel_items_2");

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        HazelcastInstance[] instances = createHazelcastInstances(config, 3);
        HazelcastInstance instance = instances[0];

        IMap<String, Integer> processors = instance.getMap("processors_" + randomName());
        IMap<String, String> processorInstances = instance.getMap("processorInstances_" + randomName());
        Pipeline pipeline = Pipeline.create();
        StreamStage<String> mappedValueStage = pipeline
                .readFrom(KafkaConnectSources.connect(randomProperties,
                        SourceRecordUtil::convertToString))
                .withoutTimestamps()
                .setLocalParallelism(localParallelism);

        mappedValueStage
                .mapUsingService(nonSharedService(Context::globalProcessorIndex),
                        (ctx, item) -> tuple2(item, ctx))
                .setLocalParallelism(localParallelism)
                .writeTo(map(processors));


        IMap<String, Integer> itemToHowManyProc = instance.getMap("itemToHowManyProc" + randomName());
        mappedValueStage
                .mapUsingService(nonSharedService(Context::globalProcessorIndex),
                        (ctx, item) ->  tuple2(item, ctx))
                .setLocalParallelism(localParallelism)
                .groupingKey(Entry::getKey)
                .mapStateful(AtomicInteger::new, (state, key, item) -> {
                    state.incrementAndGet();
                    return tuple2(key, state.get());
                })
                .writeTo(map(itemToHowManyProc));

        mappedValueStage
                .mapUsingService(nonSharedService(ctx -> ctx.hazelcastInstance().getName()),
                        (ctx, item) -> Map.entry(item, ctx))
                .setLocalParallelism(localParallelism)
                .writeTo(map(processorInstances));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(new URL(CONNECTOR_URL));

        instance.getJet().newJob(pipeline, jobConfig);

        var expectedProcessorIndexArray = new Integer[] { 0, 1 };
        var allInstancesNames = Arrays.stream(instances).map(HazelcastInstance::getName).toArray(String[]::new);
        assertTrueEventually(() -> {
            Set<Integer> array = new TreeSet<>(processors.values());
            assertThat(array).containsExactlyInAnyOrder(expectedProcessorIndexArray);
        });

        processors.clear();
        processorInstances.clear();
        insertItems(connectionUrl, "parallel_items_1");
        insertItems(connectionUrl, "parallel_items_2");
        createTableAndFill(connectionUrl, "parallel_items_3");
        createTableAndFill(connectionUrl, "parallel_items_4");
        createTableAndFill(connectionUrl, "parallel_items_5");
        createTableAndFill(connectionUrl, "parallel_items_6");

        var newExpectedProcessorIndexArray = IntStream.range(0, 3 * localParallelism).boxed().toArray(Integer[]::new);
        assertTrueEventually(() -> {
            Set<Integer> array = new TreeSet<>(processors.values());
            assertThat(array).containsExactlyInAnyOrder(newExpectedProcessorIndexArray);

            Set<String> instanceNames = new TreeSet<>(processorInstances.values());
            assertThat(instanceNames).containsExactlyInAnyOrder(allInstancesNames);
        });
        for (HazelcastInstance hi : instances) {
            hi.getLifecycleService().terminate();
        }
    }

    private void createTableAndFill(String connectionUrl, String tableName) {
        createTable(connectionUrl, tableName);
        insertItems(connectionUrl, tableName);
    }

    private static void insertItems(String connectionUrl, String tableName) {
        try (Connection conn = DriverManager.getConnection(connectionUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()
        ) {
            for (int i = 0; i < ITEM_COUNT; i++) {
                stmt.execute(String.format("INSERT INTO " + tableName + " VALUES(%d, '" + tableName + "-%d')",
                        COUNTER.incrementAndGet(), i));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void createTable(String connectionUrl, String tableName) {
        try (Connection conn = DriverManager.getConnection(connectionUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute("CREATE TABLE " + tableName + " (id INT PRIMARY KEY, name VARCHAR(128))");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
