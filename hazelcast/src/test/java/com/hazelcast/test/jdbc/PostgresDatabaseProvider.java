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

import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresDatabaseProvider implements TestDatabaseProvider {

    public static final String TEST_POSTGRES_VERSION = System.getProperty("test.postgres.version", "11.19-bullseye");
    private static final int LOGIN_TIMEOUT = 120;

    private PostgreSQLContainer<?> container;

    @Override
    public String createDatabase(String dbName) {
        //noinspection resource
        container = new PostgreSQLContainer<>("postgres:" + TEST_POSTGRES_VERSION)
                .withDatabaseName(dbName)
                .withUrlParam("user", "test")
                .withUrlParam("password", "test");
        container.start();
        String jdbcUrl = container.getJdbcUrl();
        waitForDb(jdbcUrl, LOGIN_TIMEOUT);
        return jdbcUrl;
    }

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
