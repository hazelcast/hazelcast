/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.xa.PGXADataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import javax.annotation.Nonnull;
import javax.sql.CommonDataSource;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class PostgresDatabaseProvider extends JdbcDatabaseProvider<PostgreSQLContainer<?>> {

    public static final String TEST_POSTGRES_VERSION = System.getProperty("test.postgres.version", "11.19-bullseye");

    private String command;

    public PostgresDatabaseProvider withCommand(String command) {
        this.command = command;
        return this;
    }

    @Override
    public CommonDataSource createDataSource(boolean xa) {
        if (xa) {
            return createXADataSource();
        } else {
            return createDataSource();
        }
    }

    @Nonnull
    private PGSimpleDataSource createDataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(url());
        dataSource.setUser(user());
        dataSource.setPassword(password());
        dataSource.setDatabaseName(getDatabaseName());
        return dataSource;
    }

    @Nonnull
    private PGXADataSource createXADataSource() {
        PGXADataSource dataSource = new PGXADataSource();
        dataSource.setUrl(url());
        dataSource.setUser(user());
        dataSource.setPassword(password());
        dataSource.setDatabaseName(getDatabaseName());
        return dataSource;
    }

    private static final WaitStrategy PG_DEFAULT_WAIT = new LogMessageWaitStrategy()
                .withRegEx(".*database system is ready to accept connections.*\\s")
                .withTimes(2)
                .withStartupTimeout(Duration.of(60, ChronoUnit.SECONDS));

    @SuppressWarnings("resource")
    @Override
    PostgreSQLContainer<?> createContainer(String dbName) {
        PostgreSQLContainer<?> postgreSQLContainer = new PostgreSQLContainer<>("postgres:" + TEST_POSTGRES_VERSION)
                .withDatabaseName(dbName)
                // On MacOS there seems to be some delay before the port is available for connections (maybe only with colima?).
                // As a result, container is reported started based only on logs earlier that it can be connected to.
                // If we wait for both conditions (port and log) we should get robust behavior.
                .waitingFor(new WaitAllStrategy().withStrategy(PG_DEFAULT_WAIT).withStrategy(Wait.defaultWaitStrategy()))
                .withUrlParam("user", user())
                .withUrlParam("password", password());
        if (command != null) {
            postgreSQLContainer.withCommand(command);
        }
        return postgreSQLContainer;
    }

    @Override
    public String noAuthJdbcUrl() {
        return container.getJdbcUrl()
                .replaceAll("&?user=" + user(), "")
                .replaceAll("&?password=" + password(), "");
    }


    @Override
    public String user() {
        return "test";
    }

}
