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

import org.testcontainers.containers.OracleContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static com.hazelcast.internal.util.Preconditions.checkState;

public class OracleDatabaseProvider implements TestDatabaseProvider {

    private static final int LOGIN_TIMEOUT = 120;

    private OracleContainer container;

    @Override
    public String createDatabase(String dbName) {
        container = new OracleContainer("gvenzl/oracle-xe:21-slim-faststart")
                .withUsername("test")
                .withPassword("test");

        container.start();
        String jdbcUrl = "jdbc:oracle:thin:test/test@" + container.getHost() + ":" + container.getOraclePort() + "/" + container.getDatabaseName();
        waitForDb(jdbcUrl, LOGIN_TIMEOUT);
        return jdbcUrl;
    }

    /*
    private void waitForDb(String jdbcUrl, String username, String password, int timeout) {
        DriverManager.setLoginTimeout(timeout);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            checkState(!conn.isClosed(), "at this point the connection should be open");
        } catch (SQLException e) {
            throw new RuntimeException("error while starting database", e);
        }
    }
    */
    @Override
    public String noAuthJdbcUrl() {
        return container.getJdbcUrl()
                .replaceAll("&?user=test", "")
                .replaceAll("&?password=test", "");
    }

    @Override
    public String user() {
        return "test";
    }

    @Override
    public String password() {
        return "test";
    }

    @Override
    public void shutdown() {
        if (container != null) {
            container.stop();
            container = null;
        }
    }
}
