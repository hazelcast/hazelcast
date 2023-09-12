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

import com.hazelcast.dataconnection.DataConnectionResource;
import com.hazelcast.dataconnection.impl.JdbcDataConnection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;

public class DiscoverDatabase {

    private static final ILogger LOGGER = Logger.getLogger(DiscoverDatabase.class);

    private DiscoverDatabase() {
    }

    public static List<DataConnectionResource> listResources(JdbcDataConnection jdbcDataConnection) throws SQLException {
        try (Connection connection = jdbcDataConnection.getConnection()) {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            DatabaseType databaseType = getDatabaseType(databaseMetaData);

            List<DataConnectionResource> list;

            if (databaseType == DatabaseType.POSTGRESQL) {
                LOGGER.info("Detected database type is Postgres");
                PostgresDatabaseDiscovery databaseDiscovery = new PostgresDatabaseDiscovery();
                list = databaseDiscovery.listResources(connection);
            } else if (databaseType == DatabaseType.MYSQL) {
                LOGGER.info("Detected database is MySql");
                MySQLDatabaseDiscovery databaseDiscovery = new MySQLDatabaseDiscovery();
                list = databaseDiscovery.listResources(connection);
            } else if (databaseType == DatabaseType.SQLSERVER) {
                LOGGER.info("Detected database is SQL Server");
                MSSQLDatabaseDiscovery databaseDiscovery = new MSSQLDatabaseDiscovery();
                list = databaseDiscovery.listResources(connection);
            } else {
                LOGGER.info("Could not detect database type. Using the DefaultDatabaseDiscovery");
                DefaultDatabaseDiscovery databaseDiscovery = new DefaultDatabaseDiscovery();
                list = databaseDiscovery.listResources(connection);
            }
            return list;

        }
    }

    public static DatabaseType getDatabaseType(DatabaseMetaData databaseMetaData) throws SQLException {
        String databaseProductName = databaseMetaData.getDatabaseProductName().toUpperCase(Locale.ROOT).trim();
        DatabaseType result;
        if (isPostgres(databaseProductName)) {
            return DatabaseType.POSTGRESQL;
        } else if (isMySql(databaseProductName)) {
            return DatabaseType.MYSQL;
        } else if (isSqlServer(databaseProductName)) {
            return DatabaseType.SQLSERVER;
        } else {
            result = DatabaseType.OTHER;
        }
        return result;
    }

    private static boolean isPostgres(String databaseProductName) {
        return "POSTGRESQL".equals(databaseProductName);
    }


    private static boolean isMySql(String databaseProductName) {
        return "MYSQL".equals(databaseProductName);
    }

    private static boolean isSqlServer(String databaseProductName) {
        return "MICROSOFT SQL SERVER".equals(databaseProductName);
    }
}
