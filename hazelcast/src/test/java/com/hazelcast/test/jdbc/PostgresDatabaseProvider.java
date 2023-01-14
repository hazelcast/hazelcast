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

import org.testcontainers.jdbc.ContainerDatabaseDriver;

public class PostgresDatabaseProvider implements TestDatabaseProvider {

    private static final int LOGIN_TIMEOUT = 120;
    private String jdbcUrl;

    @Override
    public String createDatabase(String dbName) {
        jdbcUrl = "jdbc:tc:postgresql:10.21:///" + dbName + "?TC_DAEMON=true";
        waitForDb(jdbcUrl, LOGIN_TIMEOUT);
        return jdbcUrl;
    }

    @Override
    public void shutdown() {
        if (jdbcUrl != null) {
            ContainerDatabaseDriver.killContainer(jdbcUrl);
            jdbcUrl = null;
        }
    }
}
