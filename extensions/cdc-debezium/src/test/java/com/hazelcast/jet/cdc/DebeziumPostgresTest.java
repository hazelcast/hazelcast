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

import static com.hazelcast.jet.TestedVersions.DEBEZIUM_POSTGRES_IMAGE;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

@Category({NightlyTest.class})
@RunWith(HazelcastSerialClassRunner.class)
public class DebeziumPostgresTest extends AbstractBasicCdcIntegrationTest<PostgreSQLContainer<?>> {

    @Override
    @SuppressWarnings("resource")
    public @Nonnull PostgreSQLContainer<?> getContainer() {
        return new PostgreSQLContainer<>(DEBEZIUM_POSTGRES_IMAGE)
                .withDatabaseName("postgres")
                .withUsername("postgres")
                .withPassword("postgres");
    }

    @Override
    protected @Nonnull DebeziumCdcSources.Builder<ChangeRecord> basicConf(PostgreSQLContainer<?> container) {
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
                .setProperty("table.include.list", "inventory.customers")
                .setProperty("heartbeat.interval.ms", 1000)
                .setProperty("plugin.name", "pgoutput")

                .setProperty("notification.enabled.channels", "TestNotificationChannel")
                .setProperty("notification.TestNotificationChannel.uuid", uuidForNotifications)
                .changeRecord();
    }
}
