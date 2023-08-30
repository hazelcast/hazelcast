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

package com.hazelcast.dataconnection.databasediscovery;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.dataconnection.DataConnectionResource;
import com.hazelcast.dataconnection.impl.JdbcDataConnection;
import com.hazelcast.dataconnection.impl.jdbcproperties.DataConnectionProperties;

import java.sql.SQLException;
import java.util.List;

public class DiscoverDatabase {

    public List<DataConnectionResource> listResources(JdbcDataConnection jdbcDataConnection) throws SQLException {
        DataConnectionConfig dataConnectionConfig = jdbcDataConnection.getConfig();
        String jdbcUrl = dataConnectionConfig.getProperty(DataConnectionProperties.JDBC_URL);

        List<DataConnectionResource> list;

        if (isPostgres(jdbcUrl)) {
            PostgresDatabaseDiscovery databaseDiscovery = new PostgresDatabaseDiscovery();
            list = databaseDiscovery.listResources(jdbcDataConnection);
        } else if (isMySql(jdbcUrl)) {
            MySQLDatabaseDiscovery databaseDiscovery = new MySQLDatabaseDiscovery();
            list = databaseDiscovery.listResources(jdbcDataConnection);
        } else {
            DefaultDatabaseDiscovery databaseDiscovery = new DefaultDatabaseDiscovery();
            list = databaseDiscovery.listResources(jdbcDataConnection);
        }
        return list;
    }

    private boolean isPostgres(String jdbcUrl) {
        return jdbcUrl.startsWith("jdbc:postgresql");
    }

    private boolean isMySql(String jdbcUrl) {
        return jdbcUrl.startsWith("jdbc:mysql");
    }
}
