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

import javax.annotation.Nonnull;
import javax.sql.CommonDataSource;

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

    @SuppressWarnings("resource")
    @Override
    PostgreSQLContainer<?> createContainer(String dbName) {
        container = new PostgreSQLContainer<>("postgres:" + TEST_POSTGRES_VERSION)
                .withDatabaseName(dbName)
                .withUrlParam("user", user())
                .withUrlParam("password", password());
        if (command != null) {
            container.withCommand(command);
        }
        return container;
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
