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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.Processor.Context;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.hazelcast.jet.pipeline.ServiceFactories.nonSharedService;
import static com.hazelcast.jet.pipeline.Sinks.map;
import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static com.hazelcast.test.OverridePropertyRule.set;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class KafkaConnectScalingIntegrationTest extends JetTestSupport {
    @ClassRule
    public static final OverridePropertyRule enableLogging = set("hazelcast.logging.type", "log4j2");

    public static final String USERNAME = "mysql";
    public static final String PASSWORD = "mysql";
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectScalingIntegrationTest.class);

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

    @Test
    public void testScaling() throws Exception {
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
        randomProperties.setProperty("table.whitelist", "parallel_items_1,parallel_items_2");
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
                        (ctx, item) -> Map.entry(item, ctx))
                .setLocalParallelism(localParallelism)
                .writeTo(map(processors));

        mappedValueStage
                .mapUsingService(nonSharedService(ctx -> ctx.hazelcastInstance().getName()),
                        (ctx, item) -> Map.entry(item, ctx))
                .setLocalParallelism(localParallelism)
                .writeTo(map(processorInstances));

        JobConfig jobConfig = new JobConfig();
        jobConfig.addJarsInZip(new URL(CONNECTOR_URL));

        instance.getJet().newJob(pipeline, jobConfig);

        var expectedProcessorIndexArray = IntStream.range(0, 3 * localParallelism).boxed().toArray(Integer[]::new);
        var allInstancesNames = Arrays.stream(instances).map(HazelcastInstance::getName).toArray(String[]::new);
        assertTrueEventually(() -> {
            Set<Integer> array = new TreeSet<>(processors.values());
            assertThat(array).containsExactlyInAnyOrder(expectedProcessorIndexArray);

            Set<String> instanceNames = new TreeSet<>(processorInstances.values());
            assertThat(instanceNames).containsExactlyInAnyOrder(allInstancesNames);
        });
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
        randomProperties.setProperty("table.whitelist", "parallel_items_1,parallel_items_2,parallel_items_3");
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
                        (ctx, item) -> Tuple2.tuple2(item, ctx))
                .setLocalParallelism(localParallelism)
                .writeTo(map(processors));


        IMap<String, Integer> itemToHowManyProc = instance.getMap("itemToHowManyProc" + randomName());
        mappedValueStage
                .mapUsingService(nonSharedService(Context::globalProcessorIndex),
                        (ctx, item) ->  Tuple2.tuple2(item, ctx))
                .setLocalParallelism(localParallelism)
                .groupingKey(Entry::getKey)
                .mapStateful(AtomicInteger::new, (state, key, item) -> {
                    state.incrementAndGet();
                    return Tuple2.tuple2(key, state.get());
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

        var expectedProcessorIndexArray = IntStream.range(0, 3 * localParallelism).boxed().toArray(Integer[]::new);
        var allInstancesNames = Arrays.stream(instances).map(HazelcastInstance::getName).toArray(String[]::new);
        assertTrueEventually(() -> {
            Set<Integer> array = new TreeSet<>(processors.values());
            assertThat(array).containsExactlyInAnyOrder(expectedProcessorIndexArray);

            Set<String> instanceNames = new TreeSet<>(processorInstances.values());
            assertThat(instanceNames).containsExactlyInAnyOrder(allInstancesNames);
        });

        processors.clear();
        processorInstances.clear();
        createTableAndFill(connectionUrl, "parallel_items_3");

        assertTrueEventually(() -> {
            Set<Integer> array = new TreeSet<>(processors.values());
            assertThat(array).containsExactlyInAnyOrder(expectedProcessorIndexArray);

            Set<String> instanceNames = new TreeSet<>(processorInstances.values());
            assertThat(instanceNames).containsExactlyInAnyOrder(allInstancesNames);
        });
    }

    private void createTableAndFill(String connectionUrl, String tableName) throws SQLException {
        createTable(connectionUrl, tableName);
        insertItems(connectionUrl, tableName);
    }

    private static void insertItems(String connectionUrl, String tableName) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()
        ) {
            for (int i = 0; i < ITEM_COUNT; i++) {
                stmt.execute(String.format("INSERT INTO " + tableName + " VALUES(%d, '" + tableName + "-%d')", i, i));
            }
        }
    }

    private static void createTable(String connectionUrl, String tableName) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionUrl, USERNAME, PASSWORD);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute("CREATE TABLE " + tableName + " (id INT PRIMARY KEY, name VARCHAR(128))");
        }
    }

}
