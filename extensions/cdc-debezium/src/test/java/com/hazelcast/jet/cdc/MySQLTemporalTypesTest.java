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
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.containers.MySQLContainer;

import javax.annotation.Nonnull;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Map;

import static com.hazelcast.jet.TestedVersions.DEBEZIUM_MYSQL_IMAGE;
import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

@Category({NightlyTest.class})
@RunWith(HazelcastSerialClassRunner.class)
@SuppressWarnings("SqlResolve")
public class MySQLTemporalTypesTest extends AbstractCdcDataTypesTest<MySQLContainer<?>> {
    @Nonnull
    @Override
    public MySQLContainer<?> getContainer() {
        //noinspection resource
        return new MySQLContainer<>(DEBEZIUM_MYSQL_IMAGE)
                .withDatabaseName("mysql")
                .withUsername("mysqluser")
                .withPassword("mysqlpw");
    }

    @Override
    public void prepareDatabase(MySQLContainer<?> container) {
        try (var connection = DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
                container.getPassword()); Statement statement = connection.createStatement()) {
            statement.addBatch("""
                    CREATE TABLE inventory.TX_TEMPORAL_USAGE(
                      ID INTEGER NOT NULL PRIMARY KEY,
                      TXN_DATETIME DATETIME(3) NOT NULL,
                      DATETIME_WITH_PRECISION DATETIME(6) NOT NULL,
                      DATE date NOT NULL,
                      DATE_LOCAL date NOT NULL,
                      TIME time NOT NULL,
                      CREATE_DATETIME DATETIME NOT NULL DEFAULT NOW()
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
    }

    @Override
    protected @Nonnull DebeziumCdcSources.Builder<ChangeRecord> conf(MySQLContainer<?> container, String table,
                                                                     Map<String, String> options) {
        return DebeziumCdcSources.debezium("mysql", "io.debezium.connector.mysql.MySqlConnector")
                                 .withErrorMaxRetries(1)
                                 .setProperty("database.server.name", "server1")
                                 .setProperty("database.server.id", "184054")
                                 .setProperty("database.hostname", container.getHost())
                                 .setProperty("database.port", container.getMappedPort(MYSQL_PORT))
                                 .setProperty("database.user", "debezium")
                                 .setProperty("database.password", "dbz")
                                 .setProperty("topic.prefix", "TESTS")
                                 .setProperty("table.include.list", table)
                                 .setProperty("heartbeat.interval.ms", 1000)
                                 .setProperty("time.precision.mode", options.get("precisionMode"))
                                 .setProperty("tombstones.on.delete", false)

                                 .setProperty("notification.enabled.channels", "TestNotificationChannel")
                                 .setProperty("notification.TestNotificationChannel.uuid", uuidForNotifications)
                                 .changeRecord();
    }

    @Override
    protected String getAdaptivePrecisionMode() {
        return "adaptive_time_microseconds";
    }

    @After
    public void cleanup() {
        revertSetOfTemporalChanges(container());
    }
}
