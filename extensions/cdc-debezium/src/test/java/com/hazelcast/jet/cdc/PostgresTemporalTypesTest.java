/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.cdc;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import io.debezium.connector.postgresql.PostgresConnector;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.annotation.Nonnull;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;

import static com.hazelcast.jet.cdc.TestUtils.executeSql;
import static com.hazelcast.jet.TestedVersions.DEBEZIUM_POSTGRES_IMAGE;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

@Category({NightlyTest.class})
@RunWith(HazelcastSerialClassRunner.class)
@SuppressWarnings("SqlResolve")
public class PostgresTemporalTypesTest extends AbstractCdcDataTypesTest<PostgreSQLContainer<?>> {
    @Nonnull
    @Override
    public PostgreSQLContainer<?> getContainer() {
        //noinspection resource
        return new PostgreSQLContainer<>(DEBEZIUM_POSTGRES_IMAGE)
                .withDatabaseName("postgres")
                .withUsername("postgres")
                .withPassword("postgres");
    }

    @Override
    public void prepareDatabase(PostgreSQLContainer<?> container) {
        executeSql(container.getJdbcUrl(), container.getUsername(), container.getPassword(),
                "ALTER DATABASE postgres SET timezone TO 'UTC';");
        executeSql(container.getJdbcUrl(), container.getUsername(), container.getPassword(),
                "SELECT pg_reload_conf();");
        try (var connection = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword());
             Statement statement = connection.createStatement()) {
            statement.addBatch("""
            CREATE TABLE inventory.TX_TEMPORAL_USAGE(
              ID INTEGER NOT NULL PRIMARY KEY,
              TXN_DATETIME TIMESTAMP(3) NOT NULL,
              DATETIME_WITH_PRECISION TIMESTAMP NOT NULL,
              DATE date NOT NULL,
              DATE_LOCAL date NOT NULL,
              TIME time NOT NULL,
              CREATE_DATETIME timestamp NOT NULL DEFAULT NOW()
              );
            """);
            statement.addBatch("""
            INSERT INTO inventory.TX_TEMPORAL_USAGE (id, txn_datetime, datetime_with_precision, date, date_local, time)
              VALUES (1001, '2024-08-06 23:30:00.000', '2024-08-06 23:30:00.111222333', '2024-08-06', '2024-08-06', '23:30:00');
            """);
            statement.addBatch("""
            INSERT INTO inventory.TX_TEMPORAL_USAGE (id, txn_datetime, datetime_with_precision, date, date_local, time)
              VALUES (1002, '2024-08-05 00:30:00.777', '2024-08-05 00:30:00.444555666', '2024-08-05', '2024-08-05', '00:30:00');
            """);
            statement.addBatch("""
            INSERT INTO inventory.TX_TEMPORAL_USAGE (id, txn_datetime, datetime_with_precision, date, date_local, time)
              VALUES (1003,'2024-08-04 01:30:00.888', '2024-08-04 01:30:00.777888999', '2024-08-04', '2024-08-04', '01:30:00');
            """);
            statement.addBatch("""
            INSERT INTO inventory.TX_TEMPORAL_USAGE (id, txn_datetime, datetime_with_precision, date, date_local, time)
              VALUES (1004, '2024-08-03 02:30:00.999', '2024-08-03 02:30:00.000111222', '2024-08-03', '2024-08-03', '02:30:00');
            """);
            statement.executeBatch();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        executeSql(container.getJdbcUrl(), container.getUsername(), container.getPassword(),
                "ALTER TABLE inventory.TX_TEMPORAL_USAGE REPLICA IDENTITY FULL;");
    }

    @Override
    protected @Nonnull DebeziumCdcSources.Builder<ChangeRecord> conf(PostgreSQLContainer<?> container,
                                                                     String table, Map<String, String> options) {
        return DebeziumCdcSources
                .debezium("postgres", PostgresConnector.class)
                .withErrorMaxRetries(1)
                .setProperty("database.server.name", "server1")
                .setProperty("database.hostname", container.getHost())
                .setProperty("database.port", container.getMappedPort(POSTGRESQL_PORT))
                .setProperty("database.user", "postgres")
                .setProperty("database.password", "postgres")
                .setProperty("database.dbname", "postgres")
                .setProperty("topic.prefix", "TESTS")
                .setProperty("table.include.list", "inventory.TX_TEMPORAL_USAGE")
                .setProperty("heartbeat.interval.ms", 1000)
                .setProperty("time.precision.mode", options.get("precisionMode"))
                .setProperty("tombstones.on.delete", false)
                .setProperty("plugin.name", "pgoutput")

                .setProperty("notification.enabled.channels", "TestNotificationChannel")
                .setProperty("notification.TestNotificationChannel.uuid", uuidForNotifications)
                .changeRecord();
    }
}
