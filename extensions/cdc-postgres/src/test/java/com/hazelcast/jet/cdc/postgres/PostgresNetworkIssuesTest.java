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

import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.NetworkIssuesTest;
import com.hazelcast.jet.pipeline.StreamSource;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.jet.TestedVersions.DEBEZIUM_POSTGRES_IMAGE;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

public class PostgresNetworkIssuesTest extends NetworkIssuesTest<PostgreSQLContainer<?>> {

    @Override
    protected int databasePort() {
        return 5432;
    }

    @Override
    @SuppressWarnings("resource")
    protected void startDbContainer() {
        if (dbContainer == null) {
            dbContainer = new PostgreSQLContainer<>(DEBEZIUM_POSTGRES_IMAGE)
                    .withDatabaseName("postgres")
                    .withUsername("postgres")
                    .withPassword("postgres")
                    .withNetwork(network)
                    .withNetworkAliases("database")
                    .withExposedPorts(5432);
            dbContainer.start();
        }
    }

    @Override
    protected StreamSource<ChangeRecord> prepareDefaultSource(int maxRetries) {
        return PostgresCdcSources
                .postgres("postgres")
                .setDatabaseAddress(toxiproxy.getHost(), toxiproxy.getMappedPort(8666))
                .setDatabaseCredentials("postgres", "postgres")
                .setProperty("database.server.name", "server1")
                .setProperty("database.dbname", "postgres")
                .setProperty("table.include.list", "inventory.customers")
                .setProperty("heartbeat.interval.ms", 1000)
                .setProperty("plugin.name", "pgoutput")

                .withErrorMaxRetries(maxRetries)
                .setProperty("snapshot.mode", "initial")
                .setProperty("connect.timeout.ms", 1000)

                .setProperty("notification.enabled.channels", "TestNotificationChannel")
                .setProperty("notification.TestNotificationChannel.uuid", uuidForNotifications)
                .setProperty("publication.name", "dbz_publication" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE))
                .build();
    }

    @Override
    protected String jdbcUrlWithAuth() {
        return "jdbc:postgresql://" + dbContainer.getHost()
                + ":" + dbContainer.getMappedPort(POSTGRESQL_PORT)
                + "/postgres?connectTimeout=1000&socketTimeout=1000"
                + "&user=postgres&password=postgres";
    }
}
