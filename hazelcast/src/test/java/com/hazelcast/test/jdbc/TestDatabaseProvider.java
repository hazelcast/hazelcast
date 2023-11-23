/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import static com.hazelcast.internal.util.Preconditions.checkState;
import static java.util.stream.Collectors.joining;

/**
 * Database provider allows changing database used in a test by providing
 * a different implementation.
 * For sample use see JdbcSqlTestSupport.
 */
public interface TestDatabaseProvider {

    /**
     * Creates database with given name and returns jdbc url that can
     * be used to connect to the database
     */
    String createDatabase(String dbName);

    /**
     * Return jdbc url without authentication parameters, so they need to be provided separately in properties
     */
    default String noAuthJdbcUrl() {
        throw new RuntimeException("Not supported");
    }

    /**
     * A username to authenticate
     */
    default String user() {
        throw new RuntimeException("Not supported");
    }

    /**
     * Password to authenticate
     */
    default String password() {
        throw new RuntimeException("Not supported");
    }

    /**
     * Waits for a connection to the database.
     * @param jdbcUrl JDBC url returned by {@link #createDatabase(String)}.
     * @param timeout wait timeout in seconds
     */
    default void waitForDb(String jdbcUrl, int timeout) {
        DriverManager.setLoginTimeout(timeout);
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            checkState(!conn.isClosed(), "at this point the connection should be open");
        } catch (SQLException e) {
            throw new RuntimeException("error while starting database", e);
        }
    }

    /**
     * Stops the database
     */
    void shutdown();

    /**
     * Quote individual parts of a compound identifier and concat with `.` delimiter
     */
    default String quote(String... parts) {
        return Arrays.stream(parts)
                     .map(part -> '\"' + part.replaceAll("\"", "\"\"") + '\"')
                     .collect(joining("."));
    };

    default String createSchemaQuery(String schemaName) {
        return "CREATE SCHEMA IF NOT EXISTS " + quote(schemaName);
    }
}
