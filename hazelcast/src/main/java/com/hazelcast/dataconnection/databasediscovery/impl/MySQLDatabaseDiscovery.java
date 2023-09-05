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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static com.hazelcast.dataconnection.impl.JdbcDataConnection.OBJECT_TYPE_TABLE;

public class MySQLDatabaseDiscovery {

    public List<DataConnectionResource> listResources(JdbcDataConnection jdbcDataConnection) throws SQLException {
        try (Connection connection = jdbcDataConnection.getConnection();
             ResultSet tables = connection.getMetaData().getTables(
                     null,
                     null,
                     null,
                     new String[]{"TABLE", "VIEW"})) {
            List<DataConnectionResource> result = new ArrayList<>();
            while (tables.next()) {
                // Format DataConnectionResource name as catalog + schema+ + table_name
                String[] name = Stream.of(
                                tables.getString("TABLE_CAT"),
                                tables.getString("TABLE_SCHEM"),
                                tables.getString("TABLE_NAME")
                        )
                        .filter(Objects::nonNull)
                        .toArray(String[]::new);

                if (name[0].startsWith("sys")) {
                    continue;
                }
                result.add(new DataConnectionResource(OBJECT_TYPE_TABLE, name));
            }
            return result;
        }
    }
}
