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
import com.hazelcast.jet.cdc.NetworkIssuesTest;
import com.hazelcast.jet.pipeline.StreamSource;
import org.testcontainers.containers.MySQLContainer;

import static com.hazelcast.jet.TestedVersions.DEBEZIUM_MYSQL_IMAGE;
import static org.testcontainers.containers.MySQLContainer.MYSQL_PORT;

public class MySqlNetworkIssuesTest extends NetworkIssuesTest<MySQLContainer<?>> {

    @Override
    protected int databasePort() {
        return 3306;
    }

    @Override
    @SuppressWarnings("resource")
    protected void startDbContainer() {
        if (dbContainer == null) {
            dbContainer = new MySQLContainer<>(DEBEZIUM_MYSQL_IMAGE)
                    .withDatabaseName("mysql")
                    .withUsername("mysqluser")
                    .withPassword("mysqlpw")
                    .withNetwork(network)
                    .withNetworkAliases("database")
                    .withExposedPorts(3306);
            dbContainer.start();
        }
    }

    @Override
    protected StreamSource<ChangeRecord> prepareDefaultSource(int maxRetries) {
        return MySqlCdcSources
                .mysql("mysql")
                .setProperty("database.server.name", "server1")
                .setDatabaseAddress(toxiproxy.getHost(), toxiproxy.getMappedPort(8666))
                .setDatabaseCredentials("debezium", "dbz")
                .setProperty("table.include.list", "inventory.customers")
                .setProperty("include.schema.changes", true)
                .setProperty("heartbeat.interval.ms", 1000)

                .withErrorMaxRetries(maxRetries)
                .setProperty("snapshot.mode", "initial")
                .setProperty("connect.timeout.ms", 1000)
                .setProperty("topic.prefix", "TESTS")

                .setProperty("notification.enabled.channels", "TestNotificationChannel")
                .setProperty("notification.TestNotificationChannel.uuid", uuidForNotifications)
                .build();
    }

    @Override
    protected String jdbcUrlWithAuth() {
        return "jdbc:mysql://mysqluser:mysqlpw@" + dbContainer.getHost() + ":" + dbContainer.getMappedPort(MYSQL_PORT)
                + "/mysql?connectTimeout=1000&socketTimeout=1000";
    }
}
