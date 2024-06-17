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
import org.junit.Before;
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

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
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

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.pipeline.ServiceFactories.nonSharedService;
import static com.hazelcast.jet.pipeline.Sinks.list;
import static com.hazelcast.jet.pipeline.Sinks.map;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.jdbc.MySQLDatabaseProvider.TEST_MYSQL_VERSION;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.rangeClosed;
import static org.assertj.core.api.Assertions.assertThat;

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

    private static MySQLContainer<?> mysql;

    private static final int ITEM_COUNT = 1_000;

    @BeforeClass
    public static void setUpDocker() {
        assumeDockerEnabled();
    }

    @SuppressWarnings("resource")
    @Before
    public void setUpContainer() {
        mysql = new MySQLContainer<>("mysql:" + TEST_MYSQL_VERSION)
                .withUsername(USERNAME).withPassword(PASSWORD)
                .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix("Docker"))
                .withTmpFs(Map.of(
                        "/var/lib/mysql/", "rw",
                        "/tmp/", "rw"
                ));
        mysql.start();
    }

    @After
    public void after() {
        if (mysql != null) {
            mysql.stop();
            mysql = null;
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
                        TestUtil::convertToString))
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
        jobConfig.addJarsInZip(getJdbcConnectorURL());

        Job job = instance.getJet().newJob(pipeline, jobConfig);

        var allInstancesNames = Arrays.stream(instances).map(HazelcastInstance::getName).toArray(String[]::new);
        assertTrueEventually(() -> {
            Set<Integer> array = new TreeSet<>(processors.values());
            assertThat(array).hasSize(localParallelism * nodeCount);

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
        final int localParallelism = 4;
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
        randomProperties.setProperty("tasks.max", String.valueOf(localParallelism));

        rangeClosed(1, TABLE_COUNT).forEach(i -> createTableAndFill(connectionUrl, "parallel_items_" + i));

        Config config = smallInstanceConfig();
        config.getJetConfig().setResourceUploadEnabled(true);
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<String, Integer> processors = instance.getMap("processors_" + randomName());
        IMap<String, String> processorInstances = instance.getMap("processorInstances_" + randomName());
        IMap<String, String> values = instance.getMap("values" + randomName());
        Pipeline pipeline = Pipeline.create();
        StreamStage<String> mappedValueStage = pipeline
                .readFrom(KafkaConnectSources.connect(randomProperties,
                        TestUtil::convertToString))
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
        jobConfig.addJarsInZip(getJdbcConnectorURL());

        var job = instance.getJet().newJob(pipeline, jobConfig);

        assertTrueEventually(() -> {
            assertThat(new HashSet<>(processors.values())).hasSize(localParallelism);

            assertThat(values.values()).hasSize(TABLE_COUNT * ITEM_COUNT);
        });

        HazelcastInstance second = createHazelcastInstance(config);
        assertTrueEventually(() -> assertThat(instance.getCluster().getMembers()).hasSize(2));

        processors.clear();
        for (int i = 1; i <= TABLE_COUNT; i++) {
            insertItems(connectionUrl, "parallel_items_" + i);
        }
        assertTrueEventually(() -> assertThat(values.values()).hasSize((TABLE_COUNT * 2) * ITEM_COUNT));
        try {
            job.cancel();
        } catch (Exception ignored) {
        }
        assertTrueEventually(() -> assertThat(job.getStatus()).isEqualTo(JobStatus.FAILED));
        instance.getLifecycleService().terminate();
        second.getLifecycleService().terminate();
    }

    @Test
    public void testScalingOfHazelcastDown() throws Exception {
        final int localParallelism = 4;
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
        randomProperties.setProperty("tasks.max", String.valueOf(localParallelism));

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
                        TestUtil::convertToString))
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
        jobConfig.addJarsInZip(getJdbcConnectorURL());

        var job = instance.getJet().newJob(pipeline, jobConfig);

        assertTrueEventually(() -> assertThat(values.values()).hasSize(TABLE_COUNT * ITEM_COUNT));

        instances[2].shutdown();
        assertTrueEventually(() -> assertThat(instance.getCluster().getMembers()).hasSize(2));
        processors.clear();
        for (int i = 1; i <= TABLE_COUNT; i++) {
            insertItems(connectionUrl, "parallel_items_" + i);
        }

        assertTrueEventually(() -> assertThat(values.values()).hasSize(2 * TABLE_COUNT * ITEM_COUNT));
        try {
            job.cancel();
        } catch (Exception ignored) {
        }
        assertTrueEventually(() -> assertThat(job.getStatus()).isEqualTo(JobStatus.FAILED));
        for (HazelcastInstance hi : instances) {
            hi.getLifecycleService().terminate();
        }
    }

    @Ignore
    @Test
    public void testDynamicReconfiguration() throws Exception {
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
                        TestUtil::convertToString))
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
        jobConfig.addJarsInZip(getJdbcConnectorURL());

        instance.getJet().newJob(pipeline, jobConfig);

        var allInstancesNames = Arrays.stream(instances).map(HazelcastInstance::getName).toArray(String[]::new);
        assertTrueEventually(() -> {
            Set<Integer> array = new TreeSet<>(processors.values());
            assertThat(array).hasSize(2);
        });

        processors.clear();
        processorInstances.clear();
        insertItems(connectionUrl, "parallel_items_1");
        insertItems(connectionUrl, "parallel_items_2");
        createTableAndFill(connectionUrl, "parallel_items_3");
        createTableAndFill(connectionUrl, "parallel_items_4");
        createTableAndFill(connectionUrl, "parallel_items_5");
        createTableAndFill(connectionUrl, "parallel_items_6");

        assertTrueEventually(() -> {
            Set<Integer> array = new TreeSet<>(processors.values());
            assertThat(array).hasSize(6);

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
             PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO " + tableName + " VALUES(?, ?)")) {

            int batchCount = 0;

            for (int i = 0; i < ITEM_COUNT; i++) {
                preparedStatement.setInt(1, COUNTER.incrementAndGet());
                preparedStatement.setString(2, tableName + "-" + i);
                preparedStatement.addBatch();

                batchCount++;

                if (batchCount % 100 == 0) {
                    preparedStatement.executeBatch();
                    batchCount = 0;
                }
            }
            // Flush any remaining items
            if (batchCount > 0) {
                preparedStatement.executeBatch();
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

    private URL getJdbcConnectorURL() throws URISyntaxException {
        ClassLoader classLoader = getClass().getClassLoader();
        final String CONNECTOR_FILE_PATH = "confluentinc-kafka-connect-jdbc-10.6.3.zip";
        URL resource = classLoader.getResource(CONNECTOR_FILE_PATH);
        assert resource != null;
        assertThat(new File(resource.toURI())).exists();
        return resource;
    }

}
