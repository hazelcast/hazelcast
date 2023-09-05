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

package com.hazelcast.dataconnection.databasediscovery.impl;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.dataconnection.DataConnectionResource;
import com.hazelcast.dataconnection.impl.JdbcDataConnection;
import com.hazelcast.dataconnection.impl.jdbcproperties.DataConnectionProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.sql.SQLException;
import java.util.List;

public class DiscoverDatabase {

    private static final ILogger LOGGER = Logger.getLogger(DiscoverDatabase.class);

    private DiscoverDatabase() {
    }

    public static List<DataConnectionResource> listResources(JdbcDataConnection jdbcDataConnection) throws SQLException {
        DataConnectionConfig dataConnectionConfig = jdbcDataConnection.getConfig();
        String jdbcUrl = dataConnectionConfig.getProperty(DataConnectionProperties.JDBC_URL);

        List<DataConnectionResource> list;

        LOGGER.info("jdbcUrl is : " + jdbcUrl);

        if (isPostgres(jdbcUrl)) {
            LOGGER.info("Detected database type is Postgres");
            PostgresDatabaseDiscovery databaseDiscovery = new PostgresDatabaseDiscovery();
            list = databaseDiscovery.listResources(jdbcDataConnection);
        } else if (isMySql(jdbcUrl)) {
            LOGGER.info("Detected database is MySql");
            MySQLDatabaseDiscovery databaseDiscovery = new MySQLDatabaseDiscovery();
            list = databaseDiscovery.listResources(jdbcDataConnection);
        } else if (isSqlServer(jdbcUrl)) {
            LOGGER.info("Detected database is SQL Server");
            MSSQLDatabaseDiscovery databaseDiscovery = new MSSQLDatabaseDiscovery();
            list = databaseDiscovery.listResources(jdbcDataConnection);
        } else {
            LOGGER.info("Could not detect database type. Using the DefaultDatabaseDiscovery");
            DefaultDatabaseDiscovery databaseDiscovery = new DefaultDatabaseDiscovery();
            list = databaseDiscovery.listResources(jdbcDataConnection);
        }
        return list;
    }

    static boolean isPostgres(String jdbcUrl) {
        return contains(jdbcUrl, ":postgresql:");
    }

    static boolean isMySql(String jdbcUrl) {
        return contains(jdbcUrl, ":mysql:");
    }

    static boolean isSqlServer(String jdbcUrl) {
        return contains(jdbcUrl, ":sqlserver:");
    }

    private static boolean contains(String jdbcUrl, String databaseType) {
        return jdbcUrl.contains(databaseType);
    }
}
