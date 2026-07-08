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

import com.hazelcast.jet.cdc.AbstractCdcMoreJobsTogetherTest;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.DebeziumCdcSources;
import org.testcontainers.containers.MySQLContainer;

import javax.annotation.Nonnull;
import java.util.Map;

import static com.hazelcast.jet.cdc.DebeziumSnapshotMode.ALWAYS;
import static com.hazelcast.jet.TestedVersions.DEBEZIUM_MYSQL_IMAGE;
import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

public class MySqlCdcMoreJobsTogetherTest extends AbstractCdcMoreJobsTogetherTest<MySQLContainer<?>> {

    @Override
    @SuppressWarnings("resource")
    public @Nonnull MySQLContainer<?> getContainer() {
        return new MySQLContainer<>(DEBEZIUM_MYSQL_IMAGE)
                .withDatabaseName("mysql")
                .withUsername("mysqluser")
                .withPassword("mysqlpw");
    }

    @Override
    protected @Nonnull DebeziumCdcSources.Builder<ChangeRecord> conf(MySQLContainer<?> container,
                                                                     String table, Map<String, String> options) {
        return MySqlCdcSources
                .mysql("mysql source")
                .withErrorMaxRetries(1)
                .setDatabaseClientId(Integer.parseInt(options.get("clientId")))
                .setDatabaseAddress(container.getHost(), container.getMappedPort(MYSQL_PORT))
                .setDatabaseCredentials("debezium", "dbz")
                .setDatabaseName("mysql")
                .setTableIncludeList(table)
                .setSnapshotMode(ALWAYS)
                .setProperty("heartbeat.interval.ms", 1000)
                .setProperty("database.server.id", Integer.parseInt(options.get("serverId")))
                .setProperty("topic.prefix", options.get("serverId"))

                .setProperty("notification.enabled.channels", "TestNotificationChannel")
                .setProperty("notification.TestNotificationChannel.uuid", options.get("uuid"))
                .changeRecord();
    }

}
