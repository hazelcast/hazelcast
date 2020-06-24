/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.cdc.postgres;

import com.hazelcast.jet.cdc.AbstractCdcIntegrationTest;
import org.junit.Rule;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

public abstract class AbstractPostgresCdcIntegrationTest extends AbstractCdcIntegrationTest {

    @Rule
    public PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("debezium/example-postgres:1.2")
            .withDatabaseName("postgres")
            .withUsername("postgres")
            .withPassword("postgres");

    protected PostgresCdcSources.Builder sourceBuilder(String name) {
        return PostgresCdcSources.postgres(name)
                .setDatabaseAddress(postgres.getContainerIpAddress())
                .setDatabasePort(postgres.getMappedPort(POSTGRESQL_PORT))
                .setDatabaseUser("postgres")
                .setDatabasePassword("postgres")
                .setDatabaseName("postgres");
    }

    protected void createSchema(String schema) throws SQLException {
        try (Connection connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
                postgres.getPassword())) {
            connection.createStatement().execute("CREATE SCHEMA " + schema);
        }
    }

}
