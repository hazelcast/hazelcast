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

import org.testcontainers.containers.MSSQLServerContainer;

import static com.hazelcast.test.HazelcastTestSupport.assumeNoArm64Architecture;

public class MSSQLDatabaseProvider implements TestDatabaseProvider {

    public static final String TEST_MSSQLSERVER_VERSION = System.getProperty("test.mssqlserver.version", "2017-CU12");

    private static final int LOGIN_TIMEOUT = 120;

    private MSSQLServerContainer<?> container;

    @Override
    public String createDatabase(String dbName) {
        assumeNoArm64Architecture();
        container = new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:" + TEST_MSSQLSERVER_VERSION);
        container.acceptLicense()
                 // See https://learn.microsoft.com/en-us/sql/connect/jdbc/using-basic-data-types?view=sql-server-ver16
                 // "To use java.sql.Time with the time SQL Server type, you must set the sendTimeAsDatetime
                 // connection property to false."
                .withUrlParam("sendTimeAsDateTime", "false")
                .withUrlParam("user", container.getUsername())
                .withUrlParam("password", container.getPassword());
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

    @Override
    public String noAuthJdbcUrl() {
        return container.getJdbcUrl()
                        .replaceAll(";user=" + user(), "")
                        .replaceAll(";password=" + password(), "");
    }

    @Override
    public String user() {
        return container.getUsername();
    }

    @Override
    public String password() {
        return container.getPassword();
    }

    @Override
    public String createSchemaQuery(String schemaName) {
        return "IF NOT EXISTS ("
                + " SELECT 0 FROM information_schema.schemata WHERE schema_name = '" + schemaName + "')"
                + " BEGIN"
                + " EXEC sp_executesql N'CREATE SCHEMA " + quote(schemaName) + "';"
                + " END";
    }
}
