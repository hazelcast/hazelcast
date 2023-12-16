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

import org.h2.jdbcx.JdbcDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@SuppressWarnings("rawtypes")
public class H2DatabaseProvider extends JdbcDatabaseProvider {

    private static final int LOGIN_TIMEOUT = 60;

    private String dbName;

    private String jdbcUrl;

    @Override
    JdbcDatabaseContainer<?> createContainer(String dbName) {
        throw new IllegalStateException("should not be called");
    }

    @Override
    public String createDatabase(String dbName) {
        this.dbName = dbName;
        this.jdbcUrl = "jdbc:h2:mem:" + dbName + ";DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1";
        waitForDb(jdbcUrl, LOGIN_TIMEOUT);
        return jdbcUrl;
    }

    @Override
    public String url() {
        return jdbcUrl;
    }

    @Override
    public String noAuthJdbcUrl() {
        return jdbcUrl;
    }

    @Override
    public DataSource createDataSource(boolean xa) {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setUrl(url());
        dataSource.setUser(user());
        dataSource.setPassword(password());

        return dataSource;
    }

    @Override
    public String user() {
        return "";
    }

    @Override
    public String password() {
        return "";
    }

    @Override
    public String getDatabaseName() {
        return dbName;
    }

    @Override
    public void shutdown() {
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            conn.createStatement().execute("shutdown");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
