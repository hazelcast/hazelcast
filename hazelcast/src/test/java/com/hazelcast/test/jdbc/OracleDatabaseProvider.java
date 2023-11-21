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
import org.testcontainers.utility.MountableFile;

public class OracleDatabaseProvider implements TestDatabaseProvider {

    private static final int LOGIN_TIMEOUT = 120;

    private OracleContainer container;

    @Override
    public String createDatabase(String dbName) {
        container = new OracleContainer("gvenzl/oracle-xe:21-slim-faststart")
                .withCopyFileToContainer(MountableFile.forClasspathResource("init.sql"), "/container-entrypoint-startdb.d/init.sql");


        container.start();
        String jdbcUrl = "jdbc:oracle:thin:test1/password@" + container.getHost() + ":" + container.getOraclePort() + "/" + container.getDatabaseName();
        waitForDb(jdbcUrl, LOGIN_TIMEOUT);
        return jdbcUrl;
    }


    @Override
    public String noAuthJdbcUrl() {
        return container.getJdbcUrl()
                .replaceAll("&?user=test1", "")
                .replaceAll("&?password=password", "");
    }

    @Override
    public String user() {
        return "test1";
    }

    @Override
    public String password() {
        return "password";
    }

    @Override
    public void shutdown() {
        if (container != null) {
            container.stop();
            container = null;
        }
    }
    @Override
    public String createSchemaQuery(String schemaName) {
        return "CREATE USER " + quote(schemaName) + " IDENTIFIED BY \"password\"\n\n"
                + "GRANT UNLIMITED TABLESPACE TO " + quote(schemaName);
    }
}
