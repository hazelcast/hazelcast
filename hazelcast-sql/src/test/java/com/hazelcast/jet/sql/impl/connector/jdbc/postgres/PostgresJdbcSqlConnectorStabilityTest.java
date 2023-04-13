/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.jdbc.postgres;

import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnectorStabilityTest;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.PostgreSQLContainer;

@Category(NightlyTest.class)
public class PostgresJdbcSqlConnectorStabilityTest extends JdbcSqlConnectorStabilityTest {

    // Shadow the parent's @BeforeClass method by using the same method name
    @BeforeClass
    public static void beforeClass() {
        jdbcDatabaseContainer = new PostgreSQLContainer<>("postgres:12.1");
        jdbcDatabaseContainer.start();
        dbConnectionUrl = jdbcDatabaseContainer.getJdbcUrl() + "&user=" + jdbcDatabaseContainer.getUsername() +
                          "&password=" + jdbcDatabaseContainer.getPassword();
        initialize();
    }
}
