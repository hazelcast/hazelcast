/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import org.testcontainers.jdbc.ContainerDatabaseDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static com.hazelcast.internal.util.Preconditions.checkState;

public class MySQLDatabaseProvider implements TestDatabaseProvider {

    private static final int LOGIN_TIMEOUT = 5;

    private String jdbcUrl;

    @Override
    public String createDatabase(String dbName) {
        jdbcUrl = "jdbc:tc:mysql:8.0.29:///" + dbName + "?TC_DAEMON=true&sessionVariables=sql_mode=ANSI";
        waitForDb();
        return jdbcUrl;
    }

    private void waitForDb() {
        DriverManager.setLoginTimeout(LOGIN_TIMEOUT);
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            checkState(!conn.isClosed(), "at this point the connection should be open");
        } catch (SQLException e) {
            throw new RuntimeException("error while starting database", e);
        }
    }

    @Override
    public void shutdown() {
        if (jdbcUrl != null) {
            ContainerDatabaseDriver.killContainer(jdbcUrl);
            jdbcUrl = null;
        }
    }
}
