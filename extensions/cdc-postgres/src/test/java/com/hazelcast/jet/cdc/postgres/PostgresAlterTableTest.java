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
package com.hazelcast.jet.cdc.postgres;

import com.hazelcast.jet.cdc.AbstractCdcAlterTableTest;
import com.hazelcast.jet.cdc.ChangeRecord;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.annotation.Nonnull;

import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.jet.cdc.DebeziumSnapshotMode.ALWAYS;
import static com.hazelcast.jet.TestedVersions.DEBEZIUM_POSTGRES_IMAGE;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

public class PostgresAlterTableTest extends AbstractCdcAlterTableTest<PostgreSQLContainer<?>> {

    @Override
    @SuppressWarnings("resource")
    public @Nonnull PostgreSQLContainer<?> getContainer() {
        return new PostgreSQLContainer<>(DEBEZIUM_POSTGRES_IMAGE)
                .withDatabaseName("postgres")
                .withUsername("postgres")
                .withPassword("postgres");
    }

    @Override
    public @Nonnull PostgresCdcSources.Builder<ChangeRecord> basicConf(PostgreSQLContainer<?> container) {
        return PostgresCdcSources
                .postgres("postgres source")
                .setDatabaseAddress(container.getHost(), container.getMappedPort(POSTGRESQL_PORT))
                .setDatabaseCredentials("postgres", "postgres")
                .setDatabaseName("postgres")
                .setTableIncludeList("inventory.customers")
                .setSnapshotMode(ALWAYS)
                .setProperty("topic.prefix", "TESTS")
                .setProperty("heartbeat.interval.ms", 1000)
                .setProperty("plugin.name", "pgoutput")

                .setProperty("notification.enabled.channels", "TestNotificationChannel")
                .setProperty("notification.TestNotificationChannel.uuid", uuidForNotifications)
                .setProperty("publication.name", "dbz_publication" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE))
                .changeRecord();
    }
}
