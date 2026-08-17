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

import com.hazelcast.jet.cdc.AbstractCdcMoreJobsTogetherTest;
import com.hazelcast.jet.cdc.ChangeRecord;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.annotation.Nonnull;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.jet.cdc.DebeziumSnapshotMode.ALWAYS;
import static com.hazelcast.jet.TestedVersions.DEBEZIUM_POSTGRES_IMAGE;
import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

public class PostgresCdcMoreJobsTogetherTest extends AbstractCdcMoreJobsTogetherTest<PostgreSQLContainer<?>> {

    @Override
    @SuppressWarnings("resource")
    public @Nonnull PostgreSQLContainer<?> getContainer() {
        return new PostgreSQLContainer<>(DEBEZIUM_POSTGRES_IMAGE)
                .withDatabaseName("postgres")
                .withUsername("postgres")
                .withPassword("postgres");
    }

    @Override
    @SuppressWarnings("SqlResolve")
    protected void prepareDatabase(PostgreSQLContainer<?> container) {
        try (var conn = DriverManager.getConnection(container.getJdbcUrl(), "postgres", "postgres");
             var stmt = conn.createStatement()) {
            stmt.execute("""
            CREATE TABLE inventory.addresses (
                id INTEGER NOT NULL PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                street VARCHAR(255) NOT NULL,
                city VARCHAR(255) NOT NULL,
                state VARCHAR(255) NOT NULL,
                zip VARCHAR(255) NOT NULL,
                type VARCHAR(255) NOT NULL
            );
            """);
            stmt.execute("ALTER TABLE inventory.addresses REPLICA IDENTITY FULL;");

            stmt.execute("""
            INSERT INTO inventory.addresses
                VALUES (10, 1001,'3183 Moore Avenue','Euless','Texas','76036','SHIPPING'),
                    (11, 1001,'2389 Hidden Valley Road','Harrisburg','Pennsylvania','17116','BILLING'),
                    (12, 1002,'281 Riverside Drive','Augusta','Georgia','30901','BILLING'),
                    (13, 1003,'3787 Brownton Road','Columbus','Mississippi','39701','SHIPPING'),
                    (14, 1003,'2458 Lost Creek Road','Bethlehem','Pennsylvania','18018','SHIPPING'),
                    (15, 1003,'4800 Simpson Square','Hillsdale','Oklahoma','73743','BILLING'),
                    (16, 1004,'1289 University Hill Road','Canehill','Arkansas','72717','LIVING')
            """);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    @Override
    public PostgresCdcSources.Builder<ChangeRecord> conf(PostgreSQLContainer<?> container,
                                                         String table, Map<String, String> options) {
        return PostgresCdcSources
                .postgres("postgres source")
                .setDatabaseAddress(container.getHost(), container.getMappedPort(POSTGRESQL_PORT))
                .setDatabaseCredentials("postgres", "postgres")
                .setDatabaseName("postgres")
                .setTableIncludeList(table)
                .setSnapshotMode(ALWAYS)
                .setProperty("heartbeat.interval.ms", 1000)
                .setReplicationSlotName("debezium" + options.getOrDefault("serverId", "default"))
                .setProperty("topic.prefix", options.get("serverId"))
                .setProperty("plugin.name", "pgoutput")
                .setProperty("publication.name", "dbz_publication" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE))

                .setProperty("notification.enabled.channels", "TestNotificationChannel")
                .setProperty("notification.TestNotificationChannel.uuid", options.get("uuid"))
                .changeRecord();
    }
}
