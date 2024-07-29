/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cdc;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.debezium.connector.mongodb.MongoDbConnector;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.postgresql.PostgresConnector;
import org.bson.Document;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.cdc.MySQLTestUtils.runQuery;
import static com.hazelcast.jet.cdc.PostgresTestUtils.getPostgreSqlConnection;
import static com.hazelcast.jet.core.JobAssertions.assertThat;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

@Category({NightlyTest.class})
@RunWith(HazelcastSerialClassRunner.class)
@SuppressWarnings({"SqlNoDataSourceInspection", "SqlResolve", "Convert2MethodRef", "checkstyle:NeedBraces"})
public class DebeziumCdcIntegrationTest extends AbstractCdcIntegrationTest {
    private static final DockerImageName MYSQL_IMAGE =
            DockerImageName.parse("debezium/example-mysql:2.6.2.Final").asCompatibleSubstituteFor("mysql");
    private static final DockerImageName POSTGRES_IMAGE =
            DockerImageName.parse("debezium/example-postgres:2.6.2.Final").asCompatibleSubstituteFor("postgres");
    private static final DockerImageName MONGODB_IMAGE =
            DockerImageName.parse("mongo:6.0.3").asCompatibleSubstituteFor("mongodb");
    private Job job;

    @Test
    public void mysql() throws Exception {
        try (MySQLContainer<?> container = mySqlContainer()) {
            container.start();

            HazelcastInstance hz = createHazelcastInstances(2)[0];
            IList<CustomerInfo> results = hz.getList(randomName());

            // given
            StreamSource<ChangeRecord> source = mySqlSource(container);
            Pipeline pipeline = getPipeline(source, results);

            // when
            job = hz.getJet().newJob(pipeline);
            assertThat(job).eventuallyHasStatus(RUNNING);

            //then
            assertTrueEventually(() -> assertThat(results).hasSizeGreaterThanOrEqualTo(4));

            //when
            MySQLTestUtils.insertData(container);

            //then
            assertContainsAddedRecords(results);
        }
    }

    @Nonnull
    private StreamSource<ChangeRecord> mySqlSource(MySQLContainer<?> container) {
        return DebeziumCdcSources.debezium("mysql",
                                         MySqlConnector.class)
                                 .setProperty("include.schema.changes", "true")
                                 .setProperty("database.hostname", container.getHost())
                                 .setProperty("database.port", Integer.toString(container.getMappedPort(MYSQL_PORT)))
                                 .setProperty("database.user", "debezium")
                                 .setProperty("database.password", "dbz")
                                 .setProperty("database.server.id", "184054")
                                 .setProperty("database.server.name", "dbserver1")
                                 .setProperty("database.whitelist", "inventory")
                                 .setProperty("table.whitelist", "inventory.customers")
                                 .build();
    }

    @Test
    public void mysql_simpleJson() {
        try (MySQLContainer<?> container = mySqlContainer()) {
            container.start();

            HazelcastInstance hz = createHazelcastInstances(2)[0];
            IList<CustomerInfo> results = hz.getList(randomName());

            // given
            StreamSource<Entry<String, String>> source = DebeziumCdcSources
                    .debeziumJson("mysql", MySqlConnector.class)
                    .setProperty("include.schema.changes", "false")
                    .setProperty("database.hostname", container.getHost())
                    .setProperty("database.port", Integer.toString(container.getMappedPort(MYSQL_PORT)))
                    .setProperty("database.user", "debezium")
                    .setProperty("database.password", "dbz")
                    .setProperty("database.server.id", "184054")
                    .setProperty("database.server.name", "dbserver1")
                    .setProperty("database.whitelist", "inventory")
                    .setProperty("table.whitelist", "inventory.customers")
                    .build();

            Pipeline pipeline = Pipeline.create();
            pipeline.readFrom(source)
                    .withIngestionTimestamps()
                    .map(changeRecord -> {
                        Map<String, Object> value = JsonUtil.mapFrom(changeRecord.getValue());
                        assert value != null;
                        String op = (String) value.get("op");
                        if (op == null) return null;
                        if ("r".equalsIgnoreCase(op)) return new CustomerInfo(Operation.SYNC, new Customer());

                        //noinspection unchecked
                        var customerMap =  value.get("after") != null
                                ? (Map<String, Object>) value.get("after")
                                : (Map<String, Object>) value.get("before");
                        var customer = new Customer((Integer) customerMap.get("id"),
                                (String) customerMap.get("first_name"),
                                (String) customerMap.get("last_name"),
                                (String) customerMap.get("email"));
                        return new CustomerInfo(Operation.get(op), customer);
                    })
                    .writeTo(Sinks.list(results));
            pipeline.setPreserveOrder(true);

            // when
            job = hz.getJet().newJob(pipeline);
            assertThat(job).eventuallyHasStatus(RUNNING);
            assertTrueEventually(() -> assertThat(results).hasSizeGreaterThanOrEqualTo(4));

            MySQLTestUtils.insertData(container);

            //then
            assertContainsAddedRecords(results);
        }
    }

    @SuppressWarnings("resource")
    private MySQLContainer<?> mySqlContainer() {
        return namedTestContainer(
                new MySQLContainer<>(MYSQL_IMAGE)
                        .withUsername("mysqluser")
                        .withPassword("mysqlpw")
        );
    }

    @Test
    public void postgres() throws Exception {
        try (PostgreSQLContainer<?> container = postgresContainer()) {
            container.start();

            HazelcastInstance hz = createHazelcastInstances(2)[0];
            IList<CustomerInfo> results = hz.getList(randomName());

            // given
            StreamSource<ChangeRecord> source = DebeziumCdcSources
                    .debezium("postgres", "io.debezium.connector.postgresql.PostgresConnector")
                    .setProperty("database.server.name", "dbserver1")
                    .setProperty("database.hostname", container.getHost())
                    .setProperty("database.port", Integer.toString(container.getMappedPort(POSTGRESQL_PORT)))
                    .setProperty("database.user", "postgres")
                    .setProperty("database.password", "postgres")
                    .setProperty("database.dbname", "postgres")
                    .setProperty("table.whitelist", "inventory.customers")
                    .setProperty("heartbeat.interval.ms", "1000") // this will add Heartbeat messages to the stream
                    .build();

            Pipeline pipeline = getPipeline(source, results);

            pipeline.setPreserveOrder(true);
            // when
            job = hz.getJet().newJob(pipeline);
            assertThat(job).eventuallyHasStatus(RUNNING);

            //then
            assertEqualsEventually(() -> results.size(), 4);

            //when
            PostgresTestUtils.insertData(container);

            //then
            assertContainsAddedRecords(results);
        }
    }

    @Test
    public void postgres_simpleJson() {
        try (PostgreSQLContainer<?> container = postgresContainer()) {
            container.start();

            HazelcastInstance hz = createHazelcastInstances(2)[0];
            IList<CustomerInfo> results = hz.getList(randomName());

            // given
            StreamSource<Entry<String, String>> source = DebeziumCdcSources
                    .debeziumJson("postgres",
                            "io.debezium.connector.postgresql.PostgresConnector")
                    .setProperty("database.server.name", "dbserver1")
                    .setProperty("database.hostname", container.getHost())
                    .setProperty("database.port", Integer.toString(container.getMappedPort(POSTGRESQL_PORT)))
                    .setProperty("database.user", "postgres")
                    .setProperty("database.password", "postgres")
                    .setProperty("database.dbname", "postgres")
                    .setProperty("table.whitelist", "inventory.customers")
                    .build();

            Pipeline pipeline = Pipeline.create();
            pipeline.readFrom(source)
                    .withIngestionTimestamps()
                    .map(changeRecord -> {
                        Map<String, Object> value = JsonUtil.mapFrom(changeRecord.getValue());
                        assert value != null;
                        String op = (String) value.get("op");
                        if (op == null) return null;
                        if ("r".equalsIgnoreCase(op)) return new CustomerInfo(Operation.get(op), new Customer());

                        //noinspection unchecked
                        var customerMap =  value.get("after") != null
                                ? (Map<String, Object>) value.get("after")
                                : (Map<String, Object>) value.get("before");
                        var customer = new Customer((Integer) customerMap.get("id"),
                                (String) customerMap.get("first_name"),
                                (String) customerMap.get("last_name"),
                                (String) customerMap.get("email"));
                        return new CustomerInfo(Operation.get(op), customer);
                    })
                    .writeTo(Sinks.list(results));
            pipeline.setPreserveOrder(true);

            // when
            job = hz.getJet().newJob(pipeline);
            assertThat(job).eventuallyHasStatus(RUNNING);
            assertTrueEventually(() -> assertThat(results).hasSizeGreaterThanOrEqualTo(4));

            PostgresTestUtils.insertData(container);

            //then
            assertContainsAddedRecords(results);
        }
    }

    @SuppressWarnings("resource")
    private PostgreSQLContainer<?> postgresContainer() {
        return namedTestContainer(
                new PostgreSQLContainer<>(POSTGRES_IMAGE)
                        .withDatabaseName("postgres")
                        .withUsername("postgres")
                        .withPassword("postgres")
        );
    }

    @Test
    public void invalidConnectorClass() {
        StreamSource<Entry<String, String>> source = DebeziumCdcSources
                .debeziumJson("connector", "io.debezium.connector.xxx.BlaBlaBla")
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withNativeTimestamps(0)
                .writeTo(Sinks.noop());

        // when
        HazelcastInstance hz = createHazelcastInstances(2)[0];
        Job job = hz.getJet().newJob(pipeline);

        assertThatThrownBy(job::join)
                .hasRootCauseInstanceOf(JetException.class)
                .hasStackTraceContaining("connector class io.debezium.connector.xxx.BlaBlaBla not found");
    }

    @Test
    public void notFailWhenOldValueNotPresent() {
        try (MySQLContainer<?> container = mySqlContainer()) {
            container.start();
            Pipeline pipeline = Pipeline.create();

            // stateful transform causes hashCode to be used
            StreamSource<ChangeRecord> source = mySqlSource(container);
            pipeline.readFrom(source)
                    .withNativeTimestamps(1)
                    .setLocalParallelism(1)
                    .groupingKey(r -> r)
                    .mapStateful(
                            LongAccumulator::new,
                            (acc, key, changeRecord) -> {
                                acc.add(1);
                                return acc.get();
                            }
                    )
                    .peek()
                    .writeTo(Sinks.list("notFailWhenOldValueNotPresent"));

            HazelcastInstance hz = createHazelcastInstances(1)[0];
            hz.getJet().newJob(pipeline);

            assertTrueEventually(() -> assertThat(hz.getList("notFailWhenOldValueNotPresent")).isNotEmpty());
        }
    }
    @Test
    public void noFailWhenNoPrimaryKey() throws Exception {
        try (PostgreSQLContainer<?> container = postgresContainer()) {
            container.start();
            //when
            try (Connection connection = getPostgreSqlConnection(container.getJdbcUrl(), container.getUsername(),
                    container.getPassword())) {
                connection.setSchema("inventory");
                Statement statement = connection.createStatement();
                statement.addBatch("CREATE TABLE NO_PK (SOME_INT INT);");
                statement.addBatch("INSERT INTO NO_PK VALUES (1)");
                statement.executeBatch();
            }

            Pipeline pipeline = Pipeline.create();

            StreamSource<ChangeRecord> source = DebeziumCdcSources
                    .debezium("postgres", PostgresConnector.class)
                    .setProperty("database.server.name", "dbserver1")
                    .setProperty("database.hostname", container.getHost())
                    .setProperty("database.port", Integer.toString(container.getMappedPort(POSTGRESQL_PORT)))
                    .setProperty("database.user", "postgres")
                    .setProperty("database.password", "postgres")
                    .setProperty("database.dbname", "postgres")
                    .setProperty("table.whitelist", "inventory.no_pk")
                    .build();
            pipeline.readFrom(source)
                    .withNativeTimestamps(1)
                    .writeTo(Sinks.list("no_pk"));

            HazelcastInstance hz = createHazelcastInstances(1)[0];
            hz.getJet().newJob(pipeline);

            assertTrueEventually(() -> assertThat(hz.getList("no_pk")).isNotEmpty());
        }
    }

    @Test
    public void nullIsNotValidOperationId() {
        try (MySQLContainer<?> container = mySqlContainer()) {
            container.start();

            HazelcastInstance hz = createHazelcastInstance();
            IList<ChangeRecord> changeRecordList = hz.getList("nullIsNotValidOperationId");

            StreamSource<ChangeRecord> source = mySqlSource(container);
            Pipeline p = Pipeline.create();
            p
                    .readFrom(source)
                    .withIngestionTimestamps()
                    .setLocalParallelism(1)
                    .writeTo(Sinks.list(changeRecordList));

            Job job = hz.getJet().newJob(p);

            assertThat(job).eventuallyHasStatus(RUNNING);

            assertTrueEventually(() -> {
                logger.info(String.format("List size: %s", changeRecordList.size()));
                assertThat(changeRecordList).as("nullIsNotValidOperationId").isNotEmpty();
                logger.info(changeRecordList.get(0).toString()); // <-- 'null' is not a valid operation id
            });
        }
    }

    @Test
    public void tableForSchemaChangesArePassed() {
        final String tableName = "schemaChangesArePassed";
        try (MySQLContainer<?> container = mySqlContainer()) {
            container.start();

            HazelcastInstance hz = createHazelcastInstance();
            IList<ChangeRecord> changeRecordList = hz.getList(tableName);

            StreamSource<ChangeRecord> source = mySqlSource(container);
            Pipeline p = Pipeline.create();
            p
                    .readFrom(source)
                    .withIngestionTimestamps()
                    .setLocalParallelism(1)
                    .writeTo(Sinks.list(changeRecordList));

            Job job = hz.getJet().newJob(p);
            assertThat(job).eventuallyHasStatus(RUNNING);
            assertTrueEventually(() -> assertThat(changeRecordList).as(tableName).isNotEmpty());

            String query = "CREATE TABLE `inventory`.`%s` (id INT)".formatted(tableName);
            runQuery(container, query);

            try {
                assertTrueEventually(() -> {
                    logger.info(String.format("List size: %s", changeRecordList.size()));
                    assertThat(changeRecordList).as(tableName).isNotEmpty();

                    assertThat(changeRecordList).anyMatch(r -> {
                        String json = r.value().toJson();
                        return json.contains("CREATE TABLE") && json.contains(tableName);
                    });
                });
            } finally {
                runQuery(container, "DROP TABLE IF EXISTS inventory.%s".formatted(tableName));
            }
        }
    }

    /**
     * {@code before} field in MongoDB CDC is not present at all
     */
    @Test
    public void noFailWhenBeforeIsNotPresent() {
        try (MongoDBContainer container = new MongoDBContainer(MONGODB_IMAGE).withExposedPorts(27017)
                                                                             .withNetworkAliases("mongo")) {
            container.start();
            String connectionString = container.getConnectionString();

            try (MongoClient mc = MongoClients.create(connectionString)) {

                HazelcastInstance hz = createHazelcastInstance();
                IList<ChangeRecord> changeRecordList = hz.getList("noFailWhenBeforeIsNotPresent");
                mc.getDatabase("test").getCollection("test").insertOne(new Document("test", "test"));

                StreamSource<ChangeRecord> source = DebeziumCdcSources
                        .debezium("mongo", MongoDbConnector.class)
                        .setProperty("mongodb.hosts", container.getHost() + ":" + container.getMappedPort(27017))
                        .setProperty("mongodb.members.auto.discover", "false")
                        .setProperty("mongodb.name", "test")
                        .setProperty("topic.prefix", "customer")
                        .setProperty("snapshot.mode", "initial")
                        .setProperty("connect.keep.alive", "true")
                        .setProperty("connect.keep.alive.interval.ms", "1000")
                        .setProperty("capture.mode", "change_streams_update_full")
                        .build();

                Pipeline p = Pipeline.create();
                p
                        .readFrom(source)
                        .withIngestionTimestamps()
                        .setLocalParallelism(1)
                        .writeTo(Sinks.list(changeRecordList));

                Job job = hz.getJet().newJob(p);

                assertThat(job).eventuallyHasStatus(RUNNING);
                mc.getDatabase("test").getCollection("test").insertOne(new Document("test", "test"));

                assertTrueEventually(() ->
                        assertThat(changeRecordList).as("Should receive record without exception").isNotEmpty());
            }
        }
    }
}
