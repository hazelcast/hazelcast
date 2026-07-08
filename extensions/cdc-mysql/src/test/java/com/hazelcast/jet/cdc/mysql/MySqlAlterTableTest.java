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
package com.hazelcast.jet.cdc.mysql;

import com.hazelcast.jet.cdc.AbstractCdcAlterTableTest;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.DebeziumCdcSources;
import org.testcontainers.containers.MySQLContainer;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.TestedVersions.DEBEZIUM_MYSQL_IMAGE;
import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

public class MySqlAlterTableTest extends AbstractCdcAlterTableTest<MySQLContainer<?>> {

    @Override
    @SuppressWarnings("resource")
    public @Nonnull MySQLContainer<?> getContainer() {
        return new MySQLContainer<>(DEBEZIUM_MYSQL_IMAGE)
                .withDatabaseName("mysql")
                .withUsername("mysqluser")
                .withPassword("mysqlpw");
    }

    @Override
    protected @Nonnull DebeziumCdcSources.Builder<ChangeRecord> basicConf(MySQLContainer<?> container) {
        return MySqlCdcSources
                .mysql("mysql source")
                .withErrorMaxRetries(1)
                .setProperty("database.server.name", "server1")
                .setProperty("database.server.id", "184054")
                .setProperty("database.hostname", container.getHost())
                .setProperty("database.port", container.getMappedPort(MYSQL_PORT))
                .setProperty("database.user", "debezium")
                .setProperty("database.password", "dbz")
                .setProperty("table.include.list", "inventory.customers")
                .setProperty("include.schema.changes", true)
                .setProperty("topic.prefix", "TESTS")

                .setProperty("notification.enabled.channels", "TestNotificationChannel")
                .setProperty("notification.TestNotificationChannel.uuid", uuidForNotifications)
                .setProperty("heartbeat.interval.ms", 1000); // this will add Heartbeat messages to the stream
    }

}
