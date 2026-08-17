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

import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.DebeziumMySqlTest;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.MySQLContainer;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.cdc.DebeziumSnapshotMode.ALWAYS;
import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

@Category({NightlyTest.class})
public class DebeziumMySqlSpecificTest extends DebeziumMySqlTest {

    @Override
    protected @Nonnull MySqlCdcSources.Builder<ChangeRecord> basicConf(MySQLContainer<?> container) {
        return MySqlCdcSources
                .mysql("mysql source")
                .withErrorMaxRetries(1)
                .setDatabaseClientId(1037)
                .setDatabaseAddress(container.getHost(), container.getMappedPort(MYSQL_PORT))
                .setDatabaseCredentials("debezium", "dbz")
                .setDatabaseName("mysql")
                .setTableIncludeList("inventory.customers")
                .setSnapshotMode(ALWAYS)
                .setProperty("heartbeat.interval.ms", 1000)
                .setProperty("topic.prefix", "TESTS")

                .setProperty("notification.enabled.channels", "TestNotificationChannel")
                .setProperty("notification.TestNotificationChannel.uuid", uuidForNotifications)
                .changeRecord();
    }

}
