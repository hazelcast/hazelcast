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

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;

public class MSSQLDatabaseProvider implements TestDatabaseProvider {
    public static final String TEST_MSSQL_VERSION = System.getProperty("test.mssql.version", "2017-CU12");

    private static final int LOGIN_TIMEOUT = 120;

    private JdbcDatabaseContainer<?> container;

    @Override
    public String createDatabase(String dbName) {
        //noinspection resource
        container = new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:" + TEST_MSSQL_VERSION)
                .acceptLicense()
                .withPassword("A_Str0ng_Required_Password")
                .withUrlParam("user", "SA")
                .withUrlParam("password", "A_Str0ng_Required_Password");
        container.start();
        String jdbcUrl = container.getJdbcUrl();
        waitForDb(jdbcUrl, LOGIN_TIMEOUT);
        return jdbcUrl;
    }

    @Override
    public void shutdown() {
        if (container != null) {
            container.stop();
            container = null;
        }
    }
}
