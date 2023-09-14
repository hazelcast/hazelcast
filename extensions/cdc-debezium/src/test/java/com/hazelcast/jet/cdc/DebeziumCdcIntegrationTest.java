/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.core.JobStatus;
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
import org.junit.Assume;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

import static com.hazelcast.jet.cdc.Operation.UNSPECIFIED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

@SuppressWarnings("SqlNoDataSourceInspection")
@Category({NightlyTest.class})
@RunWith(HazelcastSerialClassRunner.class)
public class DebeziumCdcIntegrationTest extends AbstractCdcIntegrationTest {
    private static final DockerImageName MYSQL_IMAGE =
            DockerImageName.parse("debezium/example-mysql:2.3.0.Final").asCompatibleSubstituteFor("mysql");
    private static final DockerImageName POSTGRES_IMAGE =
            DockerImageName.parse("debezium/example-postgres:2.3.0.Final").asCompatibleSubstituteFor("postgres");
    private static final DockerImageName MONGODB_IMAGE =
            DockerImageName.parse("mongo:6.0.3").asCompatibleSubstituteFor("mongodb");

    @Test
    public void mysql() throws Exception {
        Assume.assumeFalse("https://github.com/hazelcast/hazelcast-jet/issues/2623, " +
                        "https://github.com/hazelcast/hazelcast/issues/18800",
                System.getProperty("java.version").matches("^1[56].*"));

        try (MySQLContainer<?> container = mySqlContainer()) {
            container.start();

            // given
            List<String> expectedRecords = Arrays.asList(
                    "SYNC:Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}",
                    "SYNC:Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}",
                    "SYNC:Customer {id=1003, firstName=Edward, lastName=Walker, email=ed@walker.com}",
                    "SYNC:Customer {id=1004, firstName=Anne, lastName=Kretchmar, email=annek@noanswer.org}",
                    "UPDATE:Customer {id=1004, firstName=Anne Marie, lastName=Kretchmar, email=annek@noanswer.org}",
                    "INSERT:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}",
                    "DELETE:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}"
            );

            StreamSource<ChangeRecord> source = mySqlSource(container);

            Pipeline pipeline = Pipeline.create();
            pipeline.readFrom(source)
                    .withNativeTimestamps(0)
                    .<ChangeRecord>customTransform("filter_timestamps", filterTimestampsProcessorSupplier())
                    .filter(record -> record.operation() != UNSPECIFIED)
                    .map(record -> {
                        Operation operation = record.operation();
                        RecordPart value = record.value();
                        Customer customer = value.toObject(Customer.class);
                        return operation + ":" + customer;
                    })
                    .writeTo(Sinks.list("results"));
            pipeline.setPreserveOrder(true);

            // when
            HazelcastInstance hz = createHazelcastInstances(2)[0];
            Job job = hz.getJet().newJob(pipeline);

            //then
            assertEqualsEventually(() -> hz.getList("results").size(), 4);

            //when
            try (Connection connection = getMySqlConnection(container.withDatabaseName("inventory").getJdbcUrl(),
                    container.getUsername(), container.getPassword());
                 Statement statement = connection.createStatement()
            ) {
                statement.addBatch("UPDATE customers SET first_name='Anne Marie' WHERE id=1004");
                statement.addBatch("INSERT INTO customers VALUES (1005, 'Jason', 'Bourne', 'jason@bourne.org')");
                statement.addBatch("DELETE FROM customers WHERE id=1005");
                statement.executeBatch();
            }

            //then
            String expected = String.join("\n", expectedRecords);
            try {
                assertEqualsEventually(() -> String.join("\n", hz.getList("results")), expected);
            } finally {
                job.cancel();
                assertJobStatusEventually(job, JobStatus.FAILED);
            }
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
        Assume.assumeFalse("https://github.com/hazelcast/hazelcast-jet/issues/2623, " +
                        "https://github.com/hazelcast/hazelcast/issues/18800",
                System.getProperty("java.version").matches("^1[56].*"));

        try (MySQLContainer<?> container = mySqlContainer()) {
            container.start();

            // given
            List<String> expectedRecords = Arrays.asList(
                    "\\{\"id\":1001}:\\{\"before\":null," +
                            "\"after\":\\{\"id\":1001,\"first_name\":\"Sally\",\"last_name\":\"Thomas\"," +
                                "\"email\":\"sally.thomas@acme.com\"}," +
                            "\"source\":\\{\"version\":\"[\\w\\d\\.]*\",\"connector\":\"mysql\",\"name\":\"dbserver1\"," +
                                "\"ts_ms\":[0-9]*,\"snapshot\":\"true\",\"db\":\"inventory\",\"sequence\":null," +
                            "\"table\":\"customers\"," +
                                "\"server_id\":0,\"gtid\":null,\"file\":\"mysql-bin.000003\",\"pos\":157,\"row\":0," +
                                "\"thread\":null,\"query\":null}," +
                            "\"op\":\"r\",\"ts_ms\":[0-9]*,\"transaction\":null}",
                    "\\{\"id\":1002}:\\{\"before\":null," +
                            "\"after\":\\{\"id\":1002,\"first_name\":\"George\",\"last_name\":\"Bailey\"," +
                                "\"email\":\"gbailey@foobar.com\"}," +
                            "\"source\":\\{\"version\":\"[\\w\\d\\.]*\",\"connector\":\"mysql\",\"name\":\"dbserver1\"," +
                                "\"ts_ms\":[0-§9]*,\"snapshot\":\"true\",\"db\":\"inventory\",\"sequence\":null," +
                            "\"table\":\"customers\"," +
                                "\"server_id\":0,\"gtid\":null,\"file\":\"mysql-bin.000003\",\"pos\":157,\"row\":0," +
                                "\"thread\":null,\"query\":null}," +
                            "\"op\":\"r\",\"ts_ms\":[0-9]*,\"transaction\":null}",
                    "\\{\"id\":1003}:\\{\"before\":null," +
                            "\"after\":\\{\"id\":1003,\"first_name\":\"Edward\",\"last_name\":\"Walker\"," +
                                "\"email\":\"ed@walker.com\"}," +
                            "\"source\":\\{\"version\":\"[\\w\\d\\.]*\",\"connector\":\"mysql\",\"name\":\"dbserver1\"," +
                                "\"ts_ms\":[0-9]*,\"snapshot\":\"true\",\"db\":\"inventory\",\"sequence\":null," +
                            "\"table\":\"customers\"," +
                                "\"server_id\":0,\"gtid\":null,\"file\":\"mysql-bin.000003\",\"pos\":157,\"row\":0," +
                                "\"thread\":null,\"query\":null}," +
                            "\"op\":\"r\",\"ts_ms\":[0-9]*,\"transaction\":null}",
                    "\\{\"id\":1004}:\\{\"before\":null," +
                            "\"after\":\\{\"id\":1004,\"first_name\":\"Anne\",\"last_name\":\"Kretchmar\"," +
                                "\"email\":\"annek@noanswer.org\"}," +
                            "\"source\":\\{\"version\":\"[\\w\\d\\.]*\",\"connector\":\"mysql\",\"name\":\"dbserver1\"," +
                            "\"ts_ms\":[0-9]*,\"snapshot\":\"last\",\"db\":\"inventory\",\"sequence\":null," +
                            "\"table\":\"customers\"," +
                                "\"server_id\":0,\"gtid\":null,\"file\":\"mysql-bin.000003\",\"pos\":157,\"row\":0," +
                                "\"thread\":null,\"query\":null}," +
                            "\"op\":\"r\",\"ts_ms\":[0-9]*,\"transaction\":null}"
            );

            StreamSource<Entry<String, String>> source = DebeziumCdcSources.debeziumJson("mysql",
                    MySqlConnector.class)
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
                    .withNativeTimestamps(0)
                    .writeTo(Sinks.map("results"));

            // when
            HazelcastInstance hz = createHazelcastInstances(2)[0];
            Job job = hz.getJet().newJob(pipeline);

            //then
            try {
                assertTrueEventually(() -> assertMatch(expectedRecords, mapResultsToSortedList(hz.getMap("results"))));
            } finally {
                job.cancel();
                assertJobStatusEventually(job, JobStatus.FAILED);
            }
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

            // given
            List<String> expectedRecords = Arrays.asList(
                    "SYNC:Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}",
                    "SYNC:Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}",
                    "SYNC:Customer {id=1003, firstName=Edward, lastName=Walker, email=ed@walker.com}",
                    "SYNC:Customer {id=1004, firstName=Anne, lastName=Kretchmar, email=annek@noanswer.org}",
                    "UPDATE:Customer {id=1004, firstName=Anne Marie, lastName=Kretchmar, email=annek@noanswer.org}",
                    "INSERT:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}",
                    "DELETE:Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}"
            );

            StreamSource<ChangeRecord> source = DebeziumCdcSources.debezium("postgres",
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
                    .withNativeTimestamps(0)
                    .filter(record -> record.operation() != UNSPECIFIED)
                    .<ChangeRecord>customTransform("filter_timestamps", filterTimestampsProcessorSupplier())
                    .map(record -> {
                        Operation operation = record.operation();
                        RecordPart value = record.value();
                        Customer customer = value.toObject(Customer.class);
                        return operation + ":" + customer;
                    })
                    .setLocalParallelism(1)
                    .writeTo(Sinks.list("results"));

            pipeline.setPreserveOrder(true);
            // when
            HazelcastInstance hz = createHazelcastInstances(2)[0];
            Job job = hz.getJet().newJob(pipeline);

            //then
            assertEqualsEventually(() -> hz.getList("results").size(), 4);

            //when
            try (Connection connection = getPostgreSqlConnection(container.getJdbcUrl(), container.getUsername(),
                    container.getPassword())) {
                connection.setSchema("inventory");
                try (Statement statement = connection.createStatement()) {
                    statement.addBatch("UPDATE customers SET first_name='Anne Marie' WHERE id=1004");
                    statement.addBatch("INSERT INTO customers VALUES (1005, 'Jason', 'Bourne', 'jason@bourne.org')");
                    statement.addBatch("DELETE FROM customers WHERE id=1005");
                    statement.executeBatch();
                }
            }

            //then
            String expected = String.join("\n", expectedRecords);
            try {
                assertEqualsEventually(() -> String.join("\n", hz.getList("results")), expected);
            } finally {
                job.cancel();
                assertJobStatusEventually(job, JobStatus.FAILED);
            }
        }
    }

    @Test
    public void postgres_simpleJson() {
       try (PostgreSQLContainer<?> container = postgresContainer()) {
            container.start();

            // given
            List<String> expectedRecords = Arrays.asList(
                    "\\{\"id\":1001}:\\{\"before\":null," +
                            "\"after\":\\{\"id\":1001,\"first_name\":\"Sally\",\"last_name\":\"Thomas\"," +
                                "\"email\":\"sally.thomas@acme.com\"}," +
                            "\"source\":\\{\"version\":\"[\\w\\d\\.]*\",\"connector\":\"postgresql\"," +
                                "\"name\":\"dbserver1\",\"ts_ms\":[0-9]*,\"snapshot\":\"true\",\"db\":\"postgres\"," +
                            "\"sequence\":\"\\[.*]\"," +
                                "\"schema\":\"inventory\",\"table\":\"customers\",\"txId\":[0-9]*,\"lsn\":[0-9]*," +
                                "\"xmin\":null},\"op\":\"r\",\"ts_ms\":[0-9]*,\"transaction\":null}",
                    "\\{\"id\":1002}:\\{\"before\":null," +
                            "\"after\":\\{\"id\":1002,\"first_name\":\"George\",\"last_name\":\"Bailey\"," +
                                "\"email\":\"gbailey@foobar.com\"}," +
                            "\"source\":\\{\"version\":\"[\\w\\d\\.]*\",\"connector\":\"postgresql\"," +
                                "\"name\":\"dbserver1\",\"ts_ms\":[0-9]*,\"snapshot\":\"true\",\"db\":\"postgres\"," +
                            "\"sequence\":\"\\[.*]\"," +
                                "\"schema\":\"inventory\",\"table\":\"customers\",\"txId\":[0-9]*,\"lsn\":[0-9]*," +
                                "\"xmin\":null},\"op\":\"r\",\"ts_ms\":[0-9]*,\"transaction\":null}",
                    "\\{\"id\":1003}:\\{\"before\":null," +
                            "\"after\":\\{\"id\":1003,\"first_name\":\"Edward\",\"last_name\":\"Walker\"," +
                                "\"email\":\"ed@walker.com\"}," +
                            "\"source\":\\{\"version\":\"[\\w\\d\\.]*\",\"connector\":\"postgresql\"," +
                                "\"name\":\"dbserver1\",\"ts_ms\":[0-9]*,\"snapshot\":\"true\",\"db\":\"postgres\"," +
                            "\"sequence\":\"\\[.*]\"," +
                                "\"schema\":\"inventory\",\"table\":\"customers\",\"txId\":[0-9]*,\"lsn\":[0-9]*," +
                                "\"xmin\":null},\"op\":\"r\",\"ts_ms\":[0-9]*,\"transaction\":null}",
                    "\\{\"id\":1004}:\\{\"before\":null," +
                            "\"after\":\\{\"id\":1004,\"first_name\":\"Anne\",\"last_name\":\"Kretchmar\"," +
                                "\"email\":\"annek@noanswer.org\"}," +
                            "\"source\":\\{\"version\":\"[\\w\\d\\.]*\",\"connector\":\"postgresql\"," +
                                "\"name\":\"dbserver1\",\"ts_ms\":[0-9]*,\"snapshot\":\"last\",\"db\":\"postgres\"," +
                            "\"sequence\":\"\\[.*]\"," +
                                "\"schema\":\"inventory\",\"table\":\"customers\",\"txId\":[0-9]*,\"lsn\":[0-9]*," +
                                "\"xmin\":null},\"op\":\"r\",\"ts_ms\":[0-9]*,\"transaction\":null}"
            );

            StreamSource<Entry<String, String>> source = DebeziumCdcSources.debeziumJson("postgres",
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
                    .withNativeTimestamps(0)
                    .writeTo(Sinks.map("results"));

            // when
            HazelcastInstance hz = createHazelcastInstances(2)[0];
            Job job = hz.getJet().newJob(pipeline);

            //then
            try {
                assertTrueEventually(() -> assertMatch(expectedRecords, mapResultsToSortedList(hz.getMap("results"))));
            } finally {
                job.cancel();
                assertJobStatusEventually(job, JobStatus.FAILED);
            }
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
        StreamSource<Entry<String, String>> source = DebeziumCdcSources.debeziumJson("connector",
                "io.debezium.connector.xxx.BlaBlaBla")
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
                            (acc, key, record) -> {
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

            assertJobStatusEventually(job, JobStatus.RUNNING);

            assertTrueEventually(() -> {
                logger.info(String.format("List size: %s", changeRecordList.size()));
                assertThat(changeRecordList).as("nullIsNotValidOperationId").isNotEmpty();
                logger.info(changeRecordList.get(0).toString()); // <-- 'null' is not a valid operation id
            });
        }
    }

    /**
     * {@code before} field in MongoDB CDC is not present at all
     */
    @Test
    public void noFailWhenBeforeIsNotPresent() {
        try (MongoDBContainer container =  new MongoDBContainer(MONGODB_IMAGE).withExposedPorts(27017)
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

                assertJobStatusEventually(job, JobStatus.RUNNING);
                mc.getDatabase("test").getCollection("test").insertOne(new Document("test", "test"));

                assertTrueEventually(() ->
                        assertThat(changeRecordList).as("Should receive record without exception").isNotEmpty());
            }
        }
    }

    private static class Customer {

        @JsonProperty("id")
        public int id;

        @JsonProperty("first_name")
        public String firstName;

        @JsonProperty("last_name")
        public String lastName;

        @JsonProperty("email")
        public String email;

        Customer() {
        }

        @Override
        public int hashCode() {
            return Objects.hash(email, firstName, id, lastName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Customer other = (Customer) obj;
            return id == other.id
                    && Objects.equals(firstName, other.firstName)
                    && Objects.equals(lastName, other.lastName)
                    && Objects.equals(email, other.email);
        }

        @Override
        public String toString() {
            return "Customer {id=" + id + ", firstName=" + firstName + ", lastName=" + lastName + ", email=" + email + '}';
        }

    }
}
